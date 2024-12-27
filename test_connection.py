import boto3
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import logging
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def get_redshift_credentials():
    """Retrieve credentials from Redshift Serverless managed secret"""
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name='us-east-1'
        )
        
        # List secrets to find the Redshift-managed one
        secrets = client.list_secrets()
        redshift_secret = None
        
        # Find the secret that contains 'default-namespace' or your namespace name
        for secret in secrets['SecretList']:
            if 'default-namespace' in secret['Name'].lower():
                redshift_secret = secret
                break
        
        if not redshift_secret:
            raise Exception("Could not find Redshift Serverless secret")
            
        # Get the secret value
        secret_response = client.get_secret_value(SecretId=redshift_secret['ARN'])
        secret = json.loads(secret_response['SecretString'])
        
        # Debug information (remove in production)
        print("Found secret with fields:", list(secret.keys()))
        print("Username from secret:", secret.get('username', 'not found'))
        
        # Construct credentials dictionary
        credentials = {
            'username': 'admin',  # Using known admin username
            'password': secret.get('password'),
            'host': 'default-workgroup.039612861529.us-east-1.redshift-serverless.amazonaws.com',
            'port': 5439,
            'dbname': secret.get('dbname', 'dev')  # Replace 'dev' with your database name
        }
        
        return credentials
        
    except Exception as e:
        logger.error(f"Failed to retrieve credentials: {str(e)}")
        raise

def create_redshift_connection():
    """Create a connection to Redshift using credentials"""
    try:
        credentials = get_redshift_credentials()
        
        # Log connection details (remove in production)
        logger.debug(f"Connecting as: {credentials['username']}")
        logger.debug(f"To host: {credentials['host']}:{credentials['port']}")
        logger.debug(f"Database: {credentials['dbname']}")
        
        # Create connection string
        encoded_password = urllib.parse.quote_plus(credentials['password'])
        conn_str = (f"postgresql://{credentials['username']}:{encoded_password}@"
                   f"{credentials['host']}:{credentials['port']}/{credentials['dbname']}")
        
        # Create engine with SSL mode required
        engine = create_engine(
            conn_str,
            connect_args={
                'sslmode': 'require'
            }
        )
        
        return engine
    
    except Exception as e:
        logger.error(f"Failed to create connection: {str(e)}")
        raise

def test_connection():
    """Test the Redshift connection"""
    try:
        engine = create_redshift_connection()
        
        with engine.connect() as connection:
            result = pd.read_sql("SELECT CURRENT_DATE AS today", connection)
            logger.info("Connection successful!")
            print(result)
            
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        # Print full error details
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    # Print AWS environment check
    import os
    print("\nAWS Credentials Check:")
    print("AWS_ACCESS_KEY_ID set:", 'AWS_ACCESS_KEY_ID' in os.environ)
    print("AWS_SECRET_ACCESS_KEY set:", 'AWS_SECRET_ACCESS_KEY' in os.environ)
    print("AWS_DEFAULT_REGION set:", 'AWS_DEFAULT_REGION' in os.environ)
    print("\nTesting connection...")
    test_connection()