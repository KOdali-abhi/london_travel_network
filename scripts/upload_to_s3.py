import boto3
import pandas as pd
import os
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

def configure_s3_client():
    """Configure and return S3 client"""
    return boto3.client('s3')

def convert_to_parquet(df, output_path):
    """Convert DataFrame to Parquet format"""
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path, compression='snappy')

def upload_to_s3(s3_client, file_path, bucket_name, s3_key):
    """Upload file to S3"""
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading {file_path}: {str(e)}")

def process_and_upload_data(input_file, bucket_name):
    """Process CSV data and upload to S3 in Parquet format"""
    # Read CSV file
    df = pd.read_csv(input_file)
    
    # Convert timestamp string to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Add partition columns
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    
    # Create S3 client
    s3_client = configure_s3_client()
    
    # Process and upload data by partition
    for (year, month, day), partition_df in df.groupby(['year', 'month', 'day']):
        # Create partition path
        partition_path = f"year={year}/month={month}/day={day}"
        local_path = f"temp_data_{year}_{month}_{day}.parquet"
        
        # Convert to Parquet
        convert_to_parquet(partition_df, local_path)
        
        # Upload to S3
        s3_key = f"tfl-journeys/{partition_path}/data.parquet"
        upload_to_s3(s3_client, local_path, bucket_name, s3_key)
        
        # Clean up temporary file
        os.remove(local_path)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload TFL journey data to S3')
    parser.add_argument('input_file', help='Path to input CSV file')
    parser.add_argument('bucket_name', help='S3 bucket name')
    
    args = parser.parse_args()
    
    process_and_upload_data(args.input_file, args.bucket_name) 