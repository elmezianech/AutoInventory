import boto3
import json

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    
    # Extract file key from the S3 event
    key = event['Records'][0]['s3']['object']['key']
    
    # We know our target path is 'raw/inventory/'
    if key.startswith('raw/inventory/'):
        try:
            # Prepare Glue job arguments
            arguments = {
                '--file_name': key.split('/')[-1],
                '--enable-continuous-cloudwatch-log': 'true'
            }

            response = glue_client.start_job_run(
                JobName='job',
                Arguments=arguments,
                Timeout=300
            )
            
            print(f"Glue job triggered successfully. JobRunId: {response['JobRunId']}")
        except Exception as e:
            print(f"Error triggering Glue job: {str(e)}")
            raise e
    else:
        print(f"File {key} is not in the inventory folder. Ignoring the event.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully.')
    }
