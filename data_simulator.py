import pandas as pd
import numpy as np
import json
import os
import time
import logging
import pickle
from kafka import KafkaProducer
from datetime import datetime
import boto3
from config import KAFKA_CONFIG, S3_CONFIG, WAREHOUSE_CONFIG
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WarehouseSimulator:
    def __init__(self, csv_path, state_file='simulator_state.pkl'):
        self.csv_path = csv_path
        self.state_file = state_file
        
        # Load dataframe
        self.df = pd.read_csv(csv_path)
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        
        # Initialize state
        self.load_state()
        
        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            acks='all'
        )
        
        # S3 Client
        self.s3_client = boto3.client('s3')

    def load_state(self):
        """Load the last processed state from a file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'rb') as f:
                    state = pickle.load(f)
                    self.processed_pointer = state.get('processed_pointer', 0)
                    self.last_uploaded_pointer = state.get('last_uploaded_pointer', 0)
                    logger.info(f"Loaded state. Resuming from row {self.processed_pointer}")
                    logger.info(f"Last successful S3 upload at row {self.last_uploaded_pointer}")
            else:
                self.processed_pointer = 0
                self.last_uploaded_pointer = 0
                logger.info("No previous state found. Starting from the beginning.")
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            self.processed_pointer = 0
            self.last_uploaded_pointer = 0

    def save_state(self):
        """Save the current processing state to a file"""
        try:
            state = {
                'processed_pointer': self.processed_pointer,
                'last_uploaded_pointer': self.last_uploaded_pointer,
                'timestamp': datetime.now()
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(state, f)
            logger.info(f"Saved state. Processed up to row {self.processed_pointer}")
            logger.info(f"Last successful S3 upload at row {self.last_uploaded_pointer}")
        except Exception as e:
            logger.error(f"Error saving state: {e}")

    def upload_to_s3(self, batch_df):
        """Upload batch data to S3 with retry mechanism"""
        try:
            # Create a unique filename for this batch
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batch_{self.processed_pointer}_{timestamp}.csv"
            
            # Convert DataFrame to CSV string
            csv_buffer = batch_df.to_csv(index=False)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=S3_CONFIG['bucket_name'],
                Key=f"{S3_CONFIG['folders']['raw']}{filename}",
                Body=csv_buffer
            )
            
            # Update last uploaded pointer only after successful upload
            self.last_uploaded_pointer = self.processed_pointer
            self.save_state()
            
            logger.info(f"Successfully uploaded batch to S3: {filename}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False

    def check_storage_conditions(self, product_category):
        """Simulate storage condition monitoring"""
        base_conditions = WAREHOUSE_CONFIG['storage_conditions'][product_category]
        
        # Simulate current conditions with some random variation
        current_temp = np.random.normal(
            (base_conditions['temp_min'] + base_conditions['temp_max']) / 2,
            1.0
        )
        current_humidity = np.random.normal(base_conditions['humidity_max'] * 0.8, 5.0)
        
        status = 'NORMAL'
        if (current_temp < base_conditions['temp_min'] or 
            current_temp > base_conditions['temp_max'] or 
            current_humidity > base_conditions['humidity_max']):
            status = 'ALERT'
        
        return {
            'temperature': round(current_temp, 2),
            'humidity': round(current_humidity, 2),
            'status': status
        }

    def check_stock_status(self, product_category, current_stock, expiry_days):
        """Determine stock status based on levels and expiry"""
        status = []
        thresholds = WAREHOUSE_CONFIG['alert_thresholds']
        max_stock = WAREHOUSE_CONFIG['max_stock_levels'][product_category]
        
        # Check stock levels
        if current_stock <= max_stock * thresholds['critical_low']:
            status.append('CRITICAL_LOW')
        elif current_stock <= max_stock * thresholds['low_stock']:
            status.append('LOW')
        elif current_stock >= max_stock * thresholds['overstock']:
            status.append('OVERSTOCK')
        
        # Check expiry for perishables
        if expiry_days is not None:
            expiry_threshold = WAREHOUSE_CONFIG['alert_thresholds']['expiring_soon'][product_category]
            if expiry_days <= expiry_threshold:
                status.append('EXPIRING_SOON')
        
        return status if status else ['NORMAL']

    def calculate_replenishment_quantity(self, product_category, current_stock, daily_sales):
        """Calculate replenishment quantity based on current stock and sales"""
        max_stock = WAREHOUSE_CONFIG['max_stock_levels'][product_category]
        safety_stock = daily_sales * WAREHOUSE_CONFIG['safety_stock_days'][product_category]
        
        # Calculate optimal order quantity
        quantity = max_stock - current_stock
        
        # Add safety stock consideration
        if current_stock < safety_stock:
            quantity += (safety_stock - current_stock)
        
        return int(max(0, quantity))

    def simulate_batch_processing(self):
        """Process a batch of data and ensure S3 upload"""
        try:
            # Take exactly 50 rows for each hourly batch
            batch_df = self.df.iloc[self.processed_pointer:self.processed_pointer + 900].copy()
            
            if batch_df.empty:
                logger.info("No more data to process.")
                return True
            
            # Calculate daily sales for each product category
            daily_sales = self.df.groupby('Product_Category')['Sales_Volume'].mean().to_dict()
            
            # Process each row in the batch
            for _, row in batch_df.iterrows():
                product_category = row['Product_Category']
                
                # Simulate stock level reduction
                current_stock = int(max(0, row['Stock_Level'] - np.random.normal(
                    daily_sales[product_category], 
                    daily_sales[product_category] * 0.1
                )))
                
                # Determine expiry days for refrigerated items
                expiry_days = None
                if WAREHOUSE_CONFIG['storage_requirements'][product_category] == 'REFRIGERATED':
                    expiry_days = np.random.randint(1, WAREHOUSE_CONFIG['shelf_life'][product_category])
                
                # Check storage conditions
                storage_conditions = self.check_storage_conditions(product_category)
                
                # Prepare and send messages
                self.send_stock_messages(row, product_category, current_stock, 
                                      storage_conditions, expiry_days, daily_sales)
            
            # Try to upload to S3
            if not self.upload_to_s3(batch_df):
                logger.error("Failed to upload batch to S3. Will retry in next iteration.")
                return False
            
            # Update processed pointer and save state
            self.processed_pointer += len(batch_df)
            self.save_state()
            
            # Check if simulation is complete
            if self.processed_pointer >= len(self.df):
                logger.info("Simulation completed. All data processed.")
                if os.path.exists(self.state_file):
                    os.remove(self.state_file)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            return False

    def send_stock_messages(self, row, product_category, current_stock, 
                          storage_conditions, expiry_days, daily_sales):
        """Send all relevant messages to Kafka topics"""
        try:
            # Prepare stock message
            stock_message = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "store_location": row['Store_Location'],
                "product_category": product_category,
                "current_stock": current_stock,
                "storage_type": WAREHOUSE_CONFIG['storage_requirements'][product_category],
                "storage_conditions": storage_conditions,
                "status": self.check_stock_status(product_category, current_stock, expiry_days),
                "expiry_days": expiry_days,
                "reorder_point": WAREHOUSE_CONFIG['reorder_points'][product_category],
                "max_stock": WAREHOUSE_CONFIG['max_stock_levels'][product_category]
            }
            
            # Send stock levels to Kafka
            self.producer.send(KAFKA_CONFIG['topics']['stock_levels'], stock_message)
            
            # Generate alerts and replenishment messages if needed
            self.handle_alerts_and_replenishment(row, product_category, current_stock,
                                               stock_message, storage_conditions,
                                               expiry_days, daily_sales)
            
        except Exception as e:
            logger.error(f"Error sending messages to Kafka: {e}")

    def handle_alerts_and_replenishment(self, row, product_category, current_stock,
                                      stock_message, storage_conditions, expiry_days,
                                      daily_sales):
        """Handle alert and replenishment message generation"""
        if ('CRITICAL_LOW' in stock_message['status'] or 
            'LOW' in stock_message['status'] or 
            'EXPIRING_SOON' in stock_message['status'] or
            storage_conditions['status'] == 'ALERT'):
            
            # Prepare alert message
            alert_message = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "store_location": row['Store_Location'],
                "product_category": product_category,
                "alert_types": stock_message['status'],
                "current_stock": current_stock,
                "storage_conditions": storage_conditions,
                "priority": WAREHOUSE_CONFIG['replenishment_priority'][product_category],
                "expiry_days": expiry_days
            }
            
            # Send alert to Kafka
            self.producer.send(KAFKA_CONFIG['topics']['alerts'], alert_message)
            
            # Handle replenishment for low stock
            if 'CRITICAL_LOW' in stock_message['status'] or 'LOW' in stock_message['status']:
                replenishment_message = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "store_location": row['Store_Location'],
                    "product_category": product_category,
                    "quantity_to_replenish": self.calculate_replenishment_quantity(
                        product_category, current_stock, daily_sales[product_category])
                }
                self.producer.send(KAFKA_CONFIG['topics']['replenishment'], replenishment_message)

    def start_simulation(self):
        """Start the simulation with proper error handling and retry logic"""
        retry_count = 0
        max_retries = 3
        
        while self.processed_pointer < len(self.df):
            try:
                logger.info(f"Starting batch processing for rows {self.processed_pointer} to {self.processed_pointer + 49}")
                
                # Process the batch
                is_completed = self.simulate_batch_processing()
                
                # Reset retry count on successful processing
                retry_count = 0
                
                # If all data is processed, break the loop
                if is_completed:
                    break
                
                # Wait for one hour before next batch
                time.sleep(86400)  # 86400 seconds = 24 hours
                
            except Exception as e:
                logger.error(f"Error in simulation: {e}")
                retry_count += 1
                
                if retry_count >= max_retries:
                    logger.error("Max retries reached. Stopping simulation.")
                    break
                
                # Wait before retry
                time.sleep(60)  # Wait 1 minute before retry

if __name__ == "__main__":
    simulator = WarehouseSimulator('extended_fmcg_demand_forecasting.csv')
    simulator.start_simulation()