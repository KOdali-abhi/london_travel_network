import json
import os
import boto3
import requests
from datetime import datetime
import logging
from typing import Dict, List, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
kinesis = boto3.client('kinesis')
s3 = boto3.client('s3')

# TfL API configuration
TFL_BASE_URL = "https://api.tfl.gov.uk"
TFL_APP_ID = os.environ['TFL_APP_ID']
TFL_API_KEY = os.environ['TFL_API_KEY']
RAW_BUCKET = os.environ['RAW_BUCKET']
KINESIS_STREAM = os.environ['KINESIS_STREAM']

def get_tfl_data(endpoint: str) -> Dict[str, Any]:
    """Fetch data from TfL API"""
    url = f"{TFL_BASE_URL}/{endpoint}"
    params = {
        "app_id": TFL_APP_ID,
        "app_key": TFL_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching TfL data: {str(e)}")
        raise

def process_line_status() -> List[Dict[str, Any]]:
    """Process line status data"""
    data = get_tfl_data("Line/Mode/tube,overground,dlr/Status")
    processed_records = []
    
    for line in data:
        record = {
            "line_id": line.get("id"),
            "line_name": line.get("name"),
            "status": line.get("lineStatuses", [{}])[0].get("statusSeverityDescription"),
            "reason": line.get("lineStatuses", [{}])[0].get("reason"),
            "timestamp": datetime.utcnow().isoformat()
        }
        processed_records.append(record)
    
    return processed_records

def save_to_s3(data: List[Dict[str, Any]], data_type: str):
    """Save raw data to S3"""
    timestamp = datetime.utcnow()
    key = f"{data_type}/year={timestamp.year}/month={timestamp.month}/day={timestamp.day}/data_{timestamp.timestamp()}.json"
    
    try:
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.info(f"Data saved to S3: s3://{RAW_BUCKET}/{key}")
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        raise

def send_to_kinesis(records: List[Dict[str, Any]]):
    """Send records to Kinesis stream"""
    try:
        for record in records:
            kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(record),
                PartitionKey=record.get("line_id", "default")
            )
        logger.info(f"Sent {len(records)} records to Kinesis")
    except Exception as e:
        logger.error(f"Error sending to Kinesis: {str(e)}")
        raise

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler function"""
    try:
        # Process line status data
        line_status_records = process_line_status()
        
        # Save raw data to S3
        save_to_s3(line_status_records, "line_status")
        
        # Send to Kinesis for real-time processing
        send_to_kinesis(line_status_records)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data processing completed successfully",
                "records_processed": len(line_status_records)
            })
        }
    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error processing data",
                "error": str(e)
            })
        } 