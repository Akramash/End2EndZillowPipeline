import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Extract the source bucket name and object key from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Define the target bucket
    target_bucket = 'cleaned-data-csv-bucket-ak'
    
    target_file_name = object_key[:-5]
    print(target_file_name)
   
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    print(response)
    data = response['Body'].read().decode('utf-8')
    print(data)
    data = json.loads(data)
    print(data)
    
    # Convert JSON data to DataFrame
    df = pd.DataFrame(data['results'])
    
    # Select specific columns
    selected_columns = ['bathrooms', 'bedrooms', 'city', 'homeStatus', 
                        'homeType', 'livingArea', 'price', 'rentZestimate', 'zipcode']
    df = df[selected_columns]
    print(df)
    
    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)
    
    # Upload CSV to S3
    s3_client.put_object(Bucket=target_bucket, Key=f"{target_file_name}.csv", Body=csv_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('CSV conversion and S3 upload completed successfully')
    }