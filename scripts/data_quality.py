from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
from datetime import datetime
import boto3
import json

class DataQualityChecker:
    def __init__(self, spark):
        self.spark = spark
        self.sns = boto3.client('sns')
        self.metrics = {}
        
    def check_completeness(self, df, required_columns):
        """Check for completeness of required columns"""
        for column in required_columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            total_count = df.count()
            completeness = (total_count - null_count) / total_count * 100
            self.metrics[f"{column}_completeness"] = completeness
            
    def check_uniqueness(self, df, unique_columns):
        """Check uniqueness constraints"""
        total_count = df.count()
        distinct_count = df.select(unique_columns).distinct().count()
        uniqueness = distinct_count / total_count * 100
        self.metrics["uniqueness_score"] = uniqueness
        
    def check_timeliness(self, df, timestamp_column):
        """Check data timeliness"""
        max_timestamp = df.agg({timestamp_column: "max"}).collect()[0][0]
        current_time = datetime.now()
        time_diff = (current_time - max_timestamp).total_seconds() / 60
        self.metrics["data_freshness_minutes"] = time_diff
        
    def check_consistency(self, df, consistency_rules):
        """Check business rule consistency"""
        for rule_name, rule_condition in consistency_rules.items():
            valid_records = df.filter(rule_condition).count()
            total_records = df.count()
            consistency = valid_records / total_records * 100
            self.metrics[f"{rule_name}_consistency"] = consistency
            
    def check_value_distribution(self, df, column, expected_values):
        """Check value distributions"""
        value_counts = df.groupBy(column).count().collect()
        distribution = {row[column]: row["count"] for row in value_counts}
        self.metrics[f"{column}_distribution"] = distribution
        
    def send_alerts(self, topic_arn, threshold_rules):
        """Send alerts if metrics violate thresholds"""
        alerts = []
        for metric, value in self.metrics.items():
            if metric in threshold_rules:
                min_val, max_val = threshold_rules[metric]
                if value < min_val or value > max_val:
                    alerts.append(f"Alert: {metric} value {value} outside threshold [{min_val}, {max_val}]")
        
        if alerts:
            message = "\n".join(alerts)
            self.sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps({
                    "alerts": alerts,
                    "metrics": self.metrics
                })
            )

def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TfL Data Quality Checks") \
        .getOrCreate()

    # Read data
    journey_data = spark.read.parquet("s3://your-bucket/processed/journeys/")
    
    # Initialize checker
    checker = DataQualityChecker(spark)
    
    # Define quality check parameters
    required_columns = ["journey_id", "timestamp", "station_start", "station_end"]
    unique_columns = ["journey_id"]
    consistency_rules = {
        "valid_journey_time": col("journey_duration") > 0,
        "valid_passenger_count": col("passenger_count") > 0
    }
    threshold_rules = {
        "journey_id_completeness": (99.0, 100.0),
        "uniqueness_score": (98.0, 100.0),
        "data_freshness_minutes": (0, 15.0)
    }
    
    # Run checks
    checker.check_completeness(journey_data, required_columns)
    checker.check_uniqueness(journey_data, unique_columns)
    checker.check_timeliness(journey_data, "timestamp")
    checker.check_consistency(journey_data, consistency_rules)
    checker.check_value_distribution(journey_data, "transport_mode", 
                                   ["Underground", "Bus", "DLR", "Overground"])
    
    # Send alerts if needed
    checker.send_alerts(
        "arn:aws:sns:region:account:data-quality-alerts",
        threshold_rules
    )
    
    # Log metrics to CloudWatch
    cloudwatch = boto3.client('cloudwatch')
    for metric, value in checker.metrics.items():
        if isinstance(value, (int, float)):
            cloudwatch.put_metric_data(
                Namespace='TfL/DataQuality',
                MetricData=[{
                    'MetricName': metric,
                    'Value': value,
                    'Unit': 'Percent' if 'completeness' in metric or 'score' in metric else 'None'
                }]
            )

if __name__ == "__main__":
    main() 