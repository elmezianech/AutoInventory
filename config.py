# config.py
import os

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topics': {
        'stock_levels': 'inventory.stock.levels',
        'transactions': 'inventory.transactions',
        'replenishment': 'inventory.replenishment',
        'alerts': 'inventory.alerts'
    },
}

# S3 Configuration
S3_CONFIG = {
    'bucket_name': 'warehouse-data-lake',
    'folders': {
        'raw': 'raw/inventory/'
        # 'processed': 'processed/inventory/',
        # 'analytics': 'analytics/',
        # 'models': 'models/'
    }
}

# Warehouse Management Configuration
WAREHOUSE_CONFIG = {
    # Reorder points - when to trigger new orders
    'reorder_points': {
        'Household': 300,      # Non-perishable, stable demand
        'Personal Care': 250,  # Non-perishable, moderate demand
        'Dairy': 400,         # Perishable, high turnover
        'Snacks': 200,        # Moderate shelf life
        'Beverages': 350      # Heavy items, need buffer
    },
    
    # Maximum stock levels
    'max_stock_levels': {
        'Household': 1200,     # Large storage capacity needed
        'Personal Care': 1000, # Moderate storage needed
        'Dairy': 800,         # Limited by shelf life
        'Snacks': 900,        # Balance of demand and shelf life
        'Beverages': 1100     # Heavy items, space-intensive
    },
    
    # Storage requirements
    'storage_requirements': {
        'Household': 'REGULAR',
        'Personal Care': 'REGULAR',
        'Dairy': 'REFRIGERATED',
        'Snacks': 'REGULAR',
        'Beverages': 'REGULAR'
    },
    
    # Shelf life in days
    'shelf_life': {
        'Household': 365,      # 1 year
        'Personal Care': 365,  # 1 year
        'Dairy': 14,          # 2 weeks
        'Snacks': 90,         # 3 months
        'Beverages': 180      # 6 months
    },
    
    # Priority levels for replenishment
    'replenishment_priority': {
        'Household': 'MEDIUM',
        'Personal Care': 'MEDIUM',
        'Dairy': 'HIGH',      # Due to perishability
        'Snacks': 'LOW',
        'Beverages': 'LOW'
    },
    
    # Alert thresholds (percentage of max stock)
    'alert_thresholds': {
        'critical_low': 0.15,   # 15% of max stock
        'low_stock': 0.25,      # 25% of max stock
        'overstock': 0.9,       # 90% of max stock
        'expiring_soon': {
            'Household': 60,     # Alert 60 days before expiry
            'Personal Care': 60, # Alert 60 days before expiry
            'Dairy': 3,         # Alert 3 days before expiry
            'Snacks': 30,       # Alert 30 days before expiry
            'Beverages': 45     # Alert 45 days before expiry
        }
    },
    
    # Safety stock days (buffer stock)
    'safety_stock_days': {
        'Household': 14,        # 2 weeks
        'Personal Care': 14,    # 2 weeks
        'Dairy': 4,            # 4 days due to perishability
        'Snacks': 10,          # 10 days
        'Beverages': 14        # 2 weeks
    },
    
    # Lead time buffer (additional days added to supplier lead time)
    'lead_time_buffer': {
        'Household': 2,
        'Personal Care': 2,
        'Dairy': 1,            # Shorter due to perishability
        'Snacks': 2,
        'Beverages': 3         # Longer due to weight/volume
    },
    
    # Storage conditions
    'storage_conditions': {
        'Household': {'temp_min': 15, 'temp_max': 25, 'humidity_max': 60},
        'Personal Care': {'temp_min': 15, 'temp_max': 25, 'humidity_max': 60},
        'Dairy': {'temp_min': 2, 'temp_max': 4, 'humidity_max': 75},
        'Snacks': {'temp_min': 15, 'temp_max': 20, 'humidity_max': 50},
        'Beverages': {'temp_min': 10, 'temp_max': 20, 'humidity_max': 55}
    }
}