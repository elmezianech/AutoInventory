import boto3
import json

def get_redshift_password():
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='us-east-1'
    )
    
    # List secrets to find the Redshift-managed one
    secrets = client.list_secrets()
    
    # Find the secret that contains 'default-namespace' or your namespace name
    for secret in secrets['SecretList']:
        if 'default-namespace' in secret['Name'].lower():
            # Get the secret value
            secret_response = client.get_secret_value(SecretId=secret['ARN'])
            secret_dict = json.loads(secret_response['SecretString'])
            print("Password:", secret_dict.get('password'))
            return

print("Retrieving password from Secrets Manager...")
get_redshift_password()