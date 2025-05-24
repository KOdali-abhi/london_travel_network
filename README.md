# London Travel Network Data Engineering Pipeline

A scalable data engineering solution for processing and analyzing Transport for London (TfL) network data using AWS services.

## Project Overview

This project implements a robust data pipeline that processes over 10GB of TfL data, including real-time API data, historical journeys, and network status information. The pipeline supports both batch and real-time processing, with comprehensive data quality checks and monitoring.

![Architecture Diagram](docs/images/architecture.png)

## Features

- Real-time data ingestion from TfL Unified API
- Batch processing of historical data
- Data quality validation and monitoring
- Automated alerting system
- Analytics-ready data in optimized format
- Cost-effective query capabilities

## Prerequisites

- Python 3.9+
- AWS Account with appropriate permissions
- AWS CLI configured
- Node.js 14+ (for AWS CDK)
- Docker (for local testing)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/london-travel-network.git
cd london-travel-network
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install AWS CDK:
```bash
npm install -g aws-cdk
```

5. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configurations
```

## Project Structure

```
london_travel_network/
├── docs/                    # Documentation
│   ├── images/             # Architecture diagrams
│   └── api_specs/          # API specifications
├── scripts/                # Processing scripts
│   ├── data_quality.py     # Data quality checks
│   └── deploy_infrastructure.py  # AWS CDK deployment
├── lambda/                 # Lambda functions
│   └── ingestion/         # Data ingestion functions
├── tests/                 # Test files
├── sample_data/          # Sample datasets
├── .env.example         # Environment variables template
├── requirements.txt     # Python dependencies
└── README.md           # Project documentation
```

## Deployment

1. Bootstrap AWS CDK (first time only):
```bash
cdk bootstrap
```

2. Deploy the infrastructure:
```bash
cdk deploy
```

3. Verify deployment:
```bash
cdk ls
```

## Usage

1. Start data ingestion:
```bash
python scripts/start_ingestion.py
```

2. Monitor pipeline:
```bash
python scripts/monitor_pipeline.py
```

3. Run data quality checks:
```bash
python scripts/data_quality.py
```

## Data Sources

- TfL Unified API
- Historical Journey Data
- Station Topology Data
- Network Performance Data

## Performance Metrics

- Processing Time: 248 seconds for 10.4GB
- Throughput: ~1.2 million records/second
- Data Completeness: 99.92%
- Query Performance: 2.4 seconds average

## Monitoring & Alerts

- CloudWatch Metrics
- SNS Notifications
- Custom Dashboard
- Error Tracking

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Test Results

See [test_results_tfl.md](test_results_tfl.md) for detailed test results and performance metrics.

## Contact

Your Name - your.email@example.com
Project Link: https://github.com/yourusername/london-travel-network 