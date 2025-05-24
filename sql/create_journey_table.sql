CREATE EXTERNAL TABLE IF NOT EXISTS tfl_journeys (
    journey_id STRING,
    timestamp TIMESTAMP,
    transport_mode STRING,
    station_start STRING,
    station_end STRING,
    passenger_count INT,
    journey_duration INT,
    fare_amount DECIMAL(10,2),
    is_peak BOOLEAN,
    day_of_week INT
)
PARTITIONED BY (
    year INT,
    month INT,
    day INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 's3://your-bucket-name/tfl-journeys/'
TBLPROPERTIES ('parquet.compression'='SNAPPY'); 