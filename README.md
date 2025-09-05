# MetricWatch

A semantic layer for product metrics designed to streamline and automate analyst tasks through intelligent anomaly detection and automated alerting.

## Overview

MetricWatch is a comprehensive metrics monitoring and anomaly detection system that helps data teams:

- **Monitor product metrics** across multiple dimensions (time, geography, platform)
- **Detect anomalies** using statistical analysis with configurable sensitivity
- **Automate alerts** through Slack and other integrations
- **Generate visualizations** automatically in Superset
- **Orchestrate workflows** via Airflow integration
- **Track metric dependencies** through a hierarchical configuration system

## Key Features

### 🔍 Intelligent Anomaly Detection
- Statistical anomaly detection using configurable sigma thresholds
- Multi-granularity analysis (hourly, daily, weekly, monthly)
- Sliding window analysis with customizable window sizes
- Dimensional slicing by country, platform, and custom attributes

### 📊 Automated Visualization
- Automatic chart generation in Apache Superset
- Pre-configured dashboards for metric monitoring
- Integration with ClickHouse for high-performance analytics

### 🔔 Smart Alerting
- Slack bot integration for real-time notifications
- Dependency-aware alerting (alerts on root cause metrics)
- Configurable alert thresholds and sensitivity

### 🔄 Workflow Orchestration
- Airflow DAG templates for automated metric processing
- Scheduled anomaly detection runs
- Integration with existing data pipelines

### 📈 Metric Hierarchy Management
- YAML-based metric configuration
- Dependency tracking between metrics
- Formula-based calculated metrics
- Support for lagged metrics and retention analysis

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   ClickHouse    │    │   MetricWatch    │    │    Superset     │
│   (Data Store)  │◄──►│   (Core Engine)  │◄──►│ (Visualization) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │   Integrations   │
                    │  ┌─────────────┐ │
                    │  │   Airflow   │ │
                    │  └─────────────┘ │
                    │  ┌─────────────┐ │
                    │  │    Slack    │ │
                    │  └─────────────┘ │
                    └──────────────────┘
```

## Installation

### Prerequisites

- Python 3.11+
- ClickHouse database (with ready to use marts)
– Slack Bot (optional, for chat interface)
- Apache Superset (optional, for visualizations)
- Apache Airflow (optional, for orchestration)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd metric_watch
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   # ClickHouse Configuration
   export CH_HOST=your-clickhouse-host
   export CH_USER=your-username
   export CH_PASSWORD=your-password

   # Superset Configuration (optional)
   export SS_URL=http://your-superset-instance
   export SS_USER_NAME=your-superset-username
   export SS_PASSWORD=your-superset-password
   export SS_DATABASE_ID=your-database-id

   # Airflow Configuration (optional)
   export AIRFLOW_URL=http://your-airflow-instance
   ```

4. **Set up database schema**
   
   Follow the instructions in [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) to create the required database structure.

### Docker Installation

```bash
# Build the Docker image
docker build -t metricwatch .

# Run the container
docker run -e CH_HOST=your-host -e CH_USER=your-user -e CH_PASSWORD=your-password metricwatch
```

## Configuration

### Metrics Configuration

Define your metrics in `metrics_hierarchy.yml`:

```yaml
metrics:
  - name: new_registered_users
    description: "New registered users"
    depends_on:
      - new_visitors
      - 1h_registration_rate
    granularities:
      - hour:
          window_size: 168  # 7 days
          sigma_n: 3        # 3 standard deviations
          slices:
            - platform:
                - iOS
                - Android
                - web
      - day:
          window_size: 28   # 28 days
          sigma_n: 3
          slices:
            - country:
                - USA
                - Germany
                - Poland
```

### Metric Properties

- **name**: Unique identifier for the metric
- **description**: Human-readable description
- **depends_on**: List of parent metrics for dependency tracking
- **formula**: Optional calculation formula for derived metrics
- **granularities**: Time-based analysis configurations
  - **window_size**: Number of time periods for historical comparison
  - **sigma_n**: Standard deviation threshold for anomaly detection
  - **slices**: Dimensional breakdowns for analysis

## Usage

### Command Line Interface

```bash
# Run anomaly detection for all metrics
python main.py

# Check specific metric
python main.py --metric=new_registered_users

# Check specific time range
python main.py --start=2024-01-01 --end=2024-04-01

# Check with specific granularity and parameters
python main.py --metric=new_registered_users --granularity=hour --window=168 --sigma=3
```

## Examples

### Basic Anomaly Detection

```bash
# Monitor new user registrations with 3-sigma threshold
python main.py --metric=new_registered_users --granularity=day --sigma=3

# Check all metrics for the last week
python main.py --start=2024-01-01 --end=2024-01-07
```

### Advanced Usage

```bash
# Monitor iOS platform specifically
python main.py --metric=new_registered_users --slice=iOS

# Custom window size for seasonal analysis
python main.py --metric=new_registered_users --window=84 --sigma=2
```

## Database Schema

MetricWatch expects specific database tables and structure. See [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) for detailed requirements including:

- User tracking tables (`app.users`, `app.events`)
- Subscription and sales data
- Platform and country dictionaries
- Time-series optimized schema for ClickHouse

## Development

### Project Structure

```
metric_watch/
├── metric_watch/           # Core library
│   ├── charts.py          # Superset integration
│   ├── helpers.py         # Anomaly detection logic
│   ├── constants.py       # Configuration constants
│   └── ...
├── airflow_examples/      # Airflow DAG templates
├── slack_bot/            # Slack integration
├── templates/            # SQL query templates
├── metrics_hierarchy.yml # Metrics configuration
└── main.py              # CLI entry point
```

### Adding New Metrics

1. Define the metric in `metrics_hierarchy.yml`
2. Create SQL template in `templates/` if needed
3. Test the configuration:
   ```bash
   python main.py --metric=your_new_metric
   ```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Monitoring and Alerting

### Alert Types

- **Anomaly Alerts**: When metrics exceed sigma thresholds
- **Dependency Alerts**: When root cause metrics show issues

### Alert Channels

- Slack notifications with detailed metric context
- Log-based alerts for system monitoring

## Performance Considerations

- **ClickHouse Optimization**: Uses columnar storage for fast analytics
- **Sliding Windows**: Efficient time-series analysis

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

**Copyright 2024 Hyperskill**  
**Author: Vladimir Klimov**

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Support

For issues and questions:
- Create an issue in the repository
- Check the [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) for setup help
- Review the example configurations in `airflow_examples/`
