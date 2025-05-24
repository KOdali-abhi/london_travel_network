import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import datetime
from scripts.data_quality import DataQualityChecker

class TestDataQuality(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestDataQuality") \
            .master("local[1]") \
            .getOrCreate()
        
        # Define schema for test data
        cls.schema = StructType([
            StructField("journey_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("station_start", StringType(), True),
            StructField("station_end", StringType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("journey_duration", IntegerType(), True)
        ])
        
        # Create test data
        cls.test_data = [
            ("J001", datetime.datetime.now(), "Station1", "Station2", 10, 15),
            ("J002", datetime.datetime.now(), "Station2", "Station3", 5, 20),
            ("J003", datetime.datetime.now(), "Station3", "Station4", 8, 25)
        ]
        
        # Create DataFrame
        cls.df = cls.spark.createDataFrame(cls.test_data, cls.schema)
        
        # Initialize checker
        cls.checker = DataQualityChecker(cls.spark)
    
    def test_completeness_check(self):
        """Test completeness check functionality"""
        required_columns = ["journey_id", "timestamp"]
        self.checker.check_completeness(self.df, required_columns)
        
        for column in required_columns:
            self.assertEqual(
                self.checker.metrics[f"{column}_completeness"],
                100.0,
                f"Expected 100% completeness for {column}"
            )
    
    def test_uniqueness_check(self):
        """Test uniqueness check functionality"""
        unique_columns = ["journey_id"]
        self.checker.check_uniqueness(self.df, unique_columns)
        
        self.assertEqual(
            self.checker.metrics["uniqueness_score"],
            100.0,
            "Expected 100% uniqueness for journey_id"
        )
    
    def test_consistency_check(self):
        """Test consistency rules check"""
        from pyspark.sql.functions import col
        
        consistency_rules = {
            "valid_journey_time": col("journey_duration") > 0,
            "valid_passenger_count": col("passenger_count") > 0
        }
        
        self.checker.check_consistency(self.df, consistency_rules)
        
        for rule in consistency_rules.keys():
            self.assertEqual(
                self.checker.metrics[f"{rule}_consistency"],
                100.0,
                f"Expected 100% consistency for {rule}"
            )
    
    def test_value_distribution(self):
        """Test value distribution check"""
        self.checker.check_value_distribution(
            self.df,
            "station_start",
            ["Station1", "Station2", "Station3"]
        )
        
        distribution = self.checker.metrics["station_start_distribution"]
        self.assertTrue(
            all(station in distribution for station in ["Station1", "Station2", "Station3"]),
            "Expected all stations in distribution"
        )
    
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main() 