import os
import pandas as pd
import numpy as np
import streamlit as st
import mlflow
import mlflow.pytorch
import warnings
import boto3
import json
import urllib.parse
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sklearn.preprocessing import RobustScaler
from sklearn.model_selection import train_test_split
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import optuna
from pathlib import Path

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimeSeriesDataset(Dataset):
    """Custom Dataset for time series data"""
    def __init__(self, features, targets):
        self.features = torch.FloatTensor(features)
        self.targets = torch.FloatTensor(targets)
    
    def __len__(self):
        return len(self.features)
    
    def __getitem__(self, idx):
        return self.features[idx], self.targets[idx]

class SimpleForecastModel(nn.Module):
    """Improved neural network for forecasting with batch normalization and better architecture"""
    def __init__(self, input_size, hidden_size, output_size, dropout=0.2):
        super(SimpleForecastModel, self).__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.BatchNorm1d(hidden_size),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, hidden_size // 2),
            nn.BatchNorm1d(hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, output_size)
        )
        
        # Initialize weights properly
        for m in self.modules():
            if isinstance(m, nn.Linear):
                nn.init.kaiming_normal_(m.weight)
                nn.init.constant_(m.bias, 0)
    
    def forward(self, x):
        return self.network(x)

class InventoryForecasting:
    def __init__(self, mlflow_experiment_name="inventory_forecasting"):
        self.credentials = self._get_redshift_credentials()
        self.connection_string = self._create_connection_string()
        
        mlflow.set_tracking_uri("file:./mlruns")

        try:
            mlflow.create_experiment(mlflow_experiment_name)
        except:
            pass
        
        mlflow.set_experiment(mlflow_experiment_name)
        
        # Model parameters
        self.prediction_days = 5   # Predict next 5 days
        self.batch_size = 32
        
        # Data storage
        self.data = None
        self.models = {}
        self.scalers = {}
        self.last_training_date = None
        
        # Create models directory
        self.models_dir = Path("trained_models")
        self.models_dir.mkdir(exist_ok=True)

    def _get_redshift_credentials(self):
        """Retrieve Redshift credentials from AWS Secrets Manager"""
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name='us-east-1'
            )
            
            secrets = client.list_secrets()
            redshift_secret = next(
                (s for s in secrets['SecretList'] if 'default-namespace' in s['Name'].lower()),
                None
            )
            
            if not redshift_secret:
                raise Exception("Could not find Redshift Serverless secret")
            
            secret_response = client.get_secret_value(SecretId=redshift_secret['ARN'])
            secret = json.loads(secret_response['SecretString'])
            
            return {
                'username': 'admin',
                'password': secret.get('password'),
                'host': 'default-workgroup.039612861529.us-east-1.redshift-serverless.amazonaws.com',
                'port': 5439,
                'dbname': secret.get('dbname', 'dev')
            }
        except Exception as e:
            logger.error(f"Credential retrieval failed: {str(e)}")
            raise

    def _create_connection_string(self):
        """Create Redshift connection string"""
        encoded_password = urllib.parse.quote_plus(self.credentials['password'])
        return (f"postgresql://{self.credentials['username']}:{encoded_password}@"
                f"{self.credentials['host']}:{self.credentials['port']}/{self.credentials['dbname']}")

    def preprocess_data(self, df, product_category=None, store_location=None):
        """Preprocess data with simplified feature engineering"""
        logger.info(f"Starting preprocessing with shape: {df.shape}")
        logger.info(f"Product category: {product_category}, Store location: {store_location}")
        
        # Filter if specified
        if product_category and store_location:
            before_filter = len(df)
            df = df[
                (df['product_category'] == product_category) & 
                (df['store_location'] == store_location)
            ].copy()
            after_filter = len(df)
            logger.info(f"Filtered data from {before_filter} to {after_filter} rows")
            
            if df.empty:
                logger.error(f"No data found for category '{product_category}' at location '{store_location}'")
                available_categories = df['product_category'].unique()
                available_locations = df['store_location'].unique()
                logger.info(f"Available categories: {available_categories}")
                logger.info(f"Available locations: {available_locations}")
                raise ValueError(f"No data available for {product_category} at {store_location}")
        
        # Basic feature engineering
        df['day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
        df['month'] = pd.to_datetime(df['date']).dt.month
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Handle missing values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())
        
        return df

    def prepare_features(self, data, target_col):
        """Prepare features for model training"""
        feature_cols = [
            'sales_volume', 'price', 'promotion', 'stock_level',
            'supplier_cost', 'replenishment_lead_time', 'profit_margin',
            'day_of_week', 'month', 'is_weekend'
        ]
        
        # Verify all required columns exist
        missing_cols = [col for col in feature_cols if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Scale features
        scaler = RobustScaler()
        X = scaler.fit_transform(data[feature_cols])
        y = data[target_col].values
        
        # Store feature names in scaler
        scaler.feature_names_in_ = feature_cols
        
        return X, y, scaler

    def train_model(self, product_category, store_location, target_col):
        """Train model with hyperparameter optimization and MLflow logging"""
        # Get and preprocess data
        df = self.preprocess_data(self.data, product_category, store_location)
        X, y, scaler = self.prepare_features(df, target_col)
        
        # Split data
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, shuffle=False)
        
        # Create datasets
        train_dataset = TimeSeriesDataset(X_train, y_train.reshape(-1, 1))
        val_dataset = TimeSeriesDataset(X_val, y_val.reshape(-1, 1))
        train_loader = DataLoader(train_dataset, batch_size=self.batch_size, shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=self.batch_size)
        
        # Define hyperparameters
        hyperparameters = {
            "hidden_size": 64,
            "learning_rate": 0.001,
            "batch_size": self.batch_size,
            "num_epochs": 50,
            "dropout": 0.2
        }
        
        # Train model
        with mlflow.start_run() as run:
            # Log tags for organization
            mlflow.set_tags({
                "product_category": product_category,
                "store_location": store_location,
                "target_column": target_col
            })
            
            # Log hyperparameters
            mlflow.log_params(hyperparameters)
            
            model = SimpleForecastModel(
                input_size=X.shape[1],
                hidden_size=hyperparameters["hidden_size"],
                output_size=1,
                dropout=hyperparameters["dropout"]
            )
            
            criterion = nn.MSELoss()
            optimizer = optim.Adam(model.parameters(), lr=hyperparameters["learning_rate"])
            
            best_val_loss = float('inf')
            for epoch in range(hyperparameters["num_epochs"]):
                # Train and validate
                train_loss = self._train_epoch(model, train_loader, criterion, optimizer)
                val_loss = self._validate_epoch(model, val_loader, criterion)
                
                # Log metrics for this epoch
                mlflow.log_metrics({
                    "train_loss": train_loss,
                    "val_loss": val_loss,
                    "epoch": epoch
                }, step=epoch)
                
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    model_path = self.models_dir / f"{product_category}_{store_location}_{target_col}_model.pth"
                    torch.save(model.state_dict(), model_path)
                    
                    # Log the best model to MLflow
                    mlflow.log_artifact(str(model_path))
            
            # Log final best validation loss
            mlflow.log_metric("best_val_loss", best_val_loss)
            
            self.scalers[(product_category, store_location, target_col)] = scaler
            self.last_training_date = datetime.now()
            
            return model, scaler

    def _train_epoch(self, model, train_loader, criterion, optimizer):
        """Train for one epoch"""
        model.train()
        total_loss = 0
        for features, targets in train_loader:
            optimizer.zero_grad()
            outputs = model(features)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
        return total_loss / len(train_loader)

    def _validate_epoch(self, model, val_loader, criterion):
        """Validate for one epoch"""
        model.eval()
        total_loss = 0
        with torch.no_grad():
            for features, targets in val_loader:
                outputs = model(features)
                loss = criterion(outputs, targets)
                total_loss += loss.item()
        return total_loss / len(val_loader)

    def predict(self, product_category, store_location, target_col):
        """Improved prediction method with better feature handling and stability controls"""
        try:
            model_path = self.models_dir / f"{product_category}_{store_location}_{target_col}_model.pth"
            scaler_key = (product_category, store_location, target_col)
            
            if (not model_path.exists() or 
                scaler_key not in self.scalers or 
                (self.last_training_date and datetime.now() - self.last_training_date > timedelta(days=1))):
                model, _ = self.train_model(product_category, store_location, target_col)
            else:
                scaler = self.scalers[scaler_key]
                model = self._load_model(model_path, scaler.feature_names_in_)
            
            # Use last 30 days for more stable predictions
            df = self.preprocess_data(self.data, product_category, store_location)
            latest_data = df.tail(30)
            
            # Calculate moving averages for stability
            latest_data['sales_ma_7'] = latest_data[target_col].rolling(window=7).mean()
            latest_data['sales_ma_30'] = latest_data[target_col].rolling(window=30).mean()
            latest_data = latest_data.fillna(method='bfill')
            
            # Scale features
            scaler = self.scalers[scaler_key]
            X = latest_data[scaler.feature_names_in_].values
            X_scaled = scaler.transform(X)
            
            # Use last 7 days average for prediction stability
            features_tensor = torch.FloatTensor(X_scaled[-7:])
            
            # Make predictions with stability controls
            model.eval()
            predictions = []
            with torch.no_grad():
                current_features = features_tensor[-1].unsqueeze(0)
                last_pred = latest_data[target_col].mean()  # Use mean as baseline
                
                for _ in range(self.prediction_days):
                    # Get model prediction
                    pred = model(current_features).item()
                    
                    # Apply stability controls
                    max_change = 0.5  # Maximum 50% change between predictions
                    min_val = latest_data[target_col].min() * 0.5  # Don't go below 50% of minimum
                    max_val = latest_data[target_col].max() * 1.5  # Don't go above 150% of maximum
                    
                    # Limit the change from previous prediction
                    pred = max(min(pred, last_pred * (1 + max_change)), last_pred * (1 - max_change))
                    
                    # Ensure prediction stays within reasonable bounds
                    pred = max(min(pred, max_val), min_val)
                    
                    predictions.append(pred)
                    last_pred = pred
                    
                    # Update features with bounded values
                    new_features = current_features.clone()
                    if 'sales_volume' in scaler.feature_names_in_:
                        sales_idx = scaler.feature_names_in_.index('sales_volume')
                        new_features[0, sales_idx] = pred
                    
                    # Update other time-based features
                    new_features = self._update_time_features(new_features, scaler.feature_names_in_)
                    current_features = new_features
            
            return np.array(predictions)
        
        except Exception as e:
            logger.error(f"Prediction error for ({product_category}, {store_location}, {target_col})", exc_info=True)
            raise

    def _update_time_features(self, features, feature_names):
        """Helper method to update time-based features"""
        new_features = features.clone()
        
        # Update day of week
        if 'day_of_week' in feature_names:
            dow_idx = feature_names.index('day_of_week')
            current_dow = new_features[0, dow_idx]
            new_dow = (current_dow + 1) % 7
            new_features[0, dow_idx] = new_dow
            
            # Update is_weekend
            if 'is_weekend' in feature_names:
                weekend_idx = feature_names.index('is_weekend')
                new_features[0, weekend_idx] = 1 if new_dow >= 5 else 0
        
        # Update month if needed
        if 'month' in feature_names:
            month_idx = feature_names.index('month')
            current_month = new_features[0, month_idx]
            new_features[0, month_idx] = current_month  # Keep month stable for short-term predictions
        
        return new_features

    def _load_model(self, model_path, feature_names):
        """Load trained model"""
        try:
            model = SimpleForecastModel(
                input_size=len(feature_names),
                hidden_size=64,
                output_size=1
            )
            model.load_state_dict(torch.load(model_path))
            model.eval()
            return model
        except Exception as e:
            logger.error(f"Error loading model from {model_path}: {str(e)}", exc_info=True)
            raise
    
def create_streamlit_app(forecasting_model):
    """Create Streamlit dashboard"""
    st.title('Advanced Inventory Forecasting Dashboard')
    
    # Verify we have data
    if forecasting_model.data is None or forecasting_model.data.empty:
        st.error("No data available. Please check your database connection.")
        return

    # Get unique categories and locations
    unique_categories = sorted(forecasting_model.data['product_category'].unique())
    unique_locations = sorted(forecasting_model.data['store_location'].unique())
    
    # Sidebar inputs
    st.sidebar.header('Forecast Parameters')
    
    # Date range information
    last_date = forecasting_model.data['date'].max()
    first_date = forecasting_model.data['date'].min()
    st.sidebar.write(f"Data Range: {first_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')}")
    
    # Selection boxes
    selected_category = st.sidebar.selectbox('Product Category', unique_categories)
    selected_location = st.sidebar.selectbox('Store Location', unique_locations)
    selected_target = st.sidebar.selectbox(
        'Prediction Target', 
        ['sales_volume', 'stock_level'],
        help='Select the metric you want to predict'
    )
    
    # Filter data
    filtered_data = forecasting_model.data[
        (forecasting_model.data['product_category'] == selected_category) &
        (forecasting_model.data['store_location'] == selected_location)
    ].sort_values('date')
    
    if filtered_data.empty:
        st.error(f"No data available for {selected_category} at {selected_location}")
        return
    
    # Main content area
    st.write("### Historical Data Overview")
    
    # Display recent trends
    last_30_days = filtered_data.tail(30)
    if not last_30_days.empty:
        st.line_chart(last_30_days.set_index('date')[selected_target])
    
    # Key metrics
    if len(filtered_data) > 0:
        col1, col2, col3 = st.columns(3)
        with col1:
            current_value = filtered_data[selected_target].iloc[-1]
            previous_value = filtered_data[selected_target].iloc[-2] if len(filtered_data) > 1 else current_value
            st.metric(
                "Current Value",
                f"{current_value:,.2f}",
                delta=f"{current_value - previous_value:,.2f}"
            )
        with col2:
            st.metric(
                "7-Day Average",
                f"{filtered_data[selected_target].tail(7).mean():,.2f}"
            )
        with col3:
            st.metric(
                "30-Day Average",
                f"{filtered_data[selected_target].tail(30).mean():,.2f}"
            )
    
    if st.button('Generate Forecast', type='primary'):
        try:
            with st.spinner('Training model and generating forecast...'):
                predictions = forecasting_model.predict(
                    selected_category,
                    selected_location,
                    selected_target
                )
                
                # Create forecast display
                st.write("### 5-Day Forecast")
                
                # Get the last date from the filtered data and create forecast dates
                last_data_date = filtered_data['date'].max()
                forecast_dates = pd.date_range(
                    last_data_date + timedelta(days=1),
                    periods=5,
                    freq='D'
                )
                
                st.write(f"Forecasting for dates after: {last_data_date.strftime('%Y-%m-%d')}")
                
                forecast_df = pd.DataFrame({
                    'Date': forecast_dates,
                    'Forecast': predictions
                })
                
                # Plot historical + forecast
                historical_df = filtered_data[['date', selected_target]].rename(
                    columns={selected_target: 'Historical'}
                ).tail(30)  # Show last 30 days + forecast
                
                plot_df = pd.concat([
                    historical_df.rename(columns={'date': 'Date'}),
                    forecast_df[['Date', 'Forecast']]
                ])
                
                # Create the plot
                fig_data = plot_df.melt(
                    id_vars=['Date'],
                    var_name='Type',
                    value_name='Value'
                ).pivot(index='Date', columns='Type', values='Value')
                
                st.line_chart(fig_data)
                
                # Show detailed forecast
                st.write("### Detailed Forecast Values")
                forecast_container = st.container()
                with forecast_container:
                    for date, pred in zip(forecast_dates, predictions):
                        col1, col2 = st.columns([1, 2])
                        with col1:
                            st.write(date.strftime('%Y-%m-%d'))
                        with col2:
                            st.metric("Predicted Value", f"{pred:,.2f}")
                
        except Exception as e:
            st.error(f"Error generating forecast: {str(e)}")
            logger.error(f"Prediction error: {str(e)}", exc_info=True)

def main():
    """Main application entry point"""
    try:
        # Initialize forecasting model
        forecasting_model = InventoryForecasting()
        
        logger.info("Attempting to connect to database...")
        
        # Set page config
        st.set_page_config(
            page_title="Inventory Forecasting",
            page_icon="📊",
            layout="wide"
        )
        
        try:
            engine = create_engine(forecasting_model.connection_string)
            with engine.connect() as connection:
                # Test if connection works
                result = connection.execute("SELECT 1")
                logger.info("Database connection successful")
                
                # First, let's check what dates we have in the table
                date_check_query = """
                    SELECT MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as total_rows
                    FROM public.wh_inventory_analytics;
                """
                result = connection.execute(date_check_query)
                date_info = result.fetchone()
                
                if date_info:
                    min_date, max_date, total_rows = date_info
                    logger.info(f"Date range in table: {min_date} to {max_date}")
                    logger.info(f"Total rows in table: {total_rows}")
                    
                    # Modified query to get the latest 90 days of data without date restrictions
                    query = """
                        WITH RankedData AS (
                            SELECT 
                                date,
                                product_category,
                                sales_volume,
                                price,
                                promotion,
                                store_location,
                                weekday,
                                supplier_cost,
                                stock_level,
                                revenue,
                                cost,
                                profit,
                                stock_status,
                                year_month,
                                days_to_replenish,
                                replenishment_lead_time,
                                (revenue - cost) / NULLIF(cost, 0) AS profit_margin,
                                ROW_NUMBER() OVER (ORDER BY date DESC) as row_num
                            FROM public.wh_inventory_analytics
                        )
                        SELECT 
                            date,
                            product_category,
                            sales_volume,
                            price,
                            promotion,
                            store_location,
                            weekday,
                            supplier_cost,
                            stock_level,
                            revenue,
                            cost,
                            profit,
                            stock_status,
                            year_month,
                            days_to_replenish,
                            replenishment_lead_time,
                            profit_margin
                        FROM RankedData
                        WHERE row_num <= 90
                        ORDER BY date ASC;
                    """
                    
                    forecasting_model.data = pd.read_sql(query, engine)
                    logger.info(f"Successfully loaded {len(forecasting_model.data)} rows")
                    
                    if forecasting_model.data.empty:
                        logger.warning("DataFrame is empty after loading")
                        st.error("No data was loaded from the database")
                        return
                        
                    forecasting_model.data['date'] = pd.to_datetime(forecasting_model.data['date'])
                    
                    # Display data range information
                    st.info(f"""
                        Loaded the latest {len(forecasting_model.data)} days of data
                        Date range of loaded data: {forecasting_model.data['date'].min()} to {forecasting_model.data['date'].max()}
                        Total rows in database: {total_rows}
                    """)
                else:
                    logger.error("No data found in the table")
                    st.error("The inventory analytics table appears to be empty")
                    return
                
        except Exception as db_error:
            logger.error(f"Database error: {str(db_error)}", exc_info=True)
            st.error(f"Database connection failed: {str(db_error)}")
            return
        
        # Create Streamlit app
        create_streamlit_app(forecasting_model)
        
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        logger.error(f"Application error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()