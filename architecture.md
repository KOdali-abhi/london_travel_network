# London Travel Network - Data Engineering Architecture

## Data Pipeline Architecture

```mermaid
graph LR
    %% Data Sources
    A1[TfL Unified API] --> B1
    A2[Historical Data] --> B1
    A3[Station Data] --> B1
    A4[Network Status] --> B1

    %% Ingestion Layer
    subgraph Ingestion
        B1[AWS Lambda\nIngestion Functions]
        B2[Amazon Kinesis\nData Streams]
        B3[AWS DMS\nChange Data Capture]
    end
    B1 --> B2
    B1 --> B3

    %% Raw Storage
    subgraph Raw Zone
        C1[S3 Raw Zone\nJSON/CSV/XML]
    end
    B2 --> C1
    B3 --> C1

    %% Processing Layer
    subgraph Processing
        D1[EMR Cluster\nSpark Processing]
        D2[AWS Glue\nData Catalog]
        D3[Data Quality\nChecks]
    end
    C1 --> D1
    D1 --> D2
    D1 --> D3

    %% Processed Storage
    subgraph Processed Zone
        E1[S3 Processed Zone\nParquet Format]
        E2[Partition Management]
    end
    D1 --> E1
    D2 --> E2
    E1 --> E2

    %% Analytics Layer
    subgraph Analytics
        F1[Amazon Athena]
        F2[QuickSight\nDashboards]
        F3[Redshift\nWarehouse]
    end
    E1 --> F1
    F1 --> F2
    E1 --> F3

    %% Monitoring
    subgraph Monitoring
        G1[CloudWatch]
        G2[SNS Alerts]
        G3[Pipeline Metrics]
    end
    D1 --> G1
    G1 --> G2
    G1 --> G3

    style Ingestion fill:#e1f5fe
    style Raw Zone fill:#fff3e0
    style Processing fill:#f3e5f5
    style Processed Zone fill:#e8f5e9
    style Analytics fill:#fce4ec
    style Monitoring fill:#fff8e1
```

## Key Components

### Data Sources
- TfL Unified API: Real-time transport data
- Historical Data: Past journey records
- Station Data: Infrastructure information
- Network Status: Service updates

### Ingestion Layer
- Lambda Functions: API polling and data collection
- Kinesis: Stream processing for real-time data
- DMS: Change data capture for historical data

### Storage Zones
- Raw Zone: Original format data preservation
- Processed Zone: Optimized Parquet storage
- Analytics Zone: Query-optimized views

### Processing
- EMR Cluster: Distributed data processing
- Glue Catalog: Schema and metadata management
- Data Quality: Automated validation checks

### Analytics
- Athena: SQL queries on S3 data
- QuickSight: Business intelligence
- Redshift: Data warehousing

### Monitoring
- CloudWatch: Resource monitoring
- SNS: Alert notifications
- Custom Metrics: Pipeline health tracking

## Data Flow

1. Data ingestion from multiple TfL sources
2. Stream processing of real-time data
3. Raw data storage in S3
4. EMR processing and transformation
5. Storage in optimized format
6. Analytics and visualization
7. Continuous monitoring and alerting 