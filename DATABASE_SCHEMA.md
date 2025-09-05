# Database Schema Requirements

This document describes the expected database schema for the MetricWatch anomaly detection tool. The tool has been designed to work with ClickHouse databases but can be adapted for other SQL databases.

## Required Databases

### 1. `app` Database
Contains core application data including user information and subscriptions.

#### Tables:

**`app.users`**
- `id` (UInt64) - Unique user identifier
- `date_registered` (DateTime) - User registration timestamp
- `date_joined` (DateTime) - User first visit timestamp
- `country_id` (UInt64) - Reference to country dictionary
- `sub_created_at_least` (DateTime) - Earliest subscription creation date

**`app.subscriptions`**
- `user_id` (UInt64) - Reference to users.id
- `created_at` (DateTime) - Subscription creation timestamp
- `type` (String) - Subscription type ('personal', 'premium', etc.)

**`app.users_dict`** (ClickHouse Dictionary)
- `id` (UInt64) - User ID
- `country_id` (UInt64) - Country identifier

**`app.events`**
- `user_id` (UInt64) - Reference to app.users.id
- `dt` (DateTime) - Activity timestamp
- `dt_registered` (DateTime) - User registration timestamp
- `dt_joined` (DateTime) - User join timestamp
- `date` (Date) - Activity date
- `action` (String) - Activity type ('view', 'completed_submission', 'failed_submission', 'rejected_submission')
- `registration_date` (Date) - User registration date

**`app.sales`**
- `user_id` (UInt64) - Reference to app.users.id
- `amount_usd_cents` (UInt64) - Sale amount in USD cents
- `product_type` (String) - Product type ('personal', 'premium', etc.)

**`app.countries_dict`** (ClickHouse Dictionary)
- `id` (UInt64) - Country ID
- `name` (String) - Country name

**`app.platforms_dict`** (ClickHouse Dictionary)
- `id` (UInt64) - User ID
- `platform` (String) - Platform name ('iOS', 'Android', 'web')

### 3. `metric_watch` Database
Used by the tool to store generated views and intermediate results.

## Data Requirements

### User Journey Tracking
The schema supports tracking user journeys through these key events:
- **Registration**: When users sign up (`date_registered`)
- **First Visit**: When users first interact (`date_joined`)
- **Activity**: User interactions with content (`user_activity`)
- **Subscriptions**: When users subscribe to services
- **Sales**: Revenue tracking

### Time-based Analysis
All tables should include appropriate timestamp fields to support:
- Hourly granularity analysis
- Daily, weekly, and monthly aggregations
- Lag-based metrics (10-day, 14-day lags)
- Retention analysis (2nd week retention)

### Dimensional Analysis
The schema supports slicing metrics by:
- **Country**: Geographic segmentation
- **Platform**: Device/platform segmentation (iOS, Android, web)
- **Product Type**: Subscription tier analysis

## Setup Instructions

1. Create the required databases: `app`, `metric_watch`
2. Create tables with the schema described above
3. Set up ClickHouse dictionaries for efficient lookups
4. Ensure proper indexing for time-based queries
5. Configure environment variables for database connections

## Environment Variables

```bash
CLICKHOUSE_HOST_URL=http://your-clickhouse-host:8123
CH_USER=your_username
CH_PASSWORD=your_password
```

## Notes

- This schema is designed for ClickHouse but can be adapted for other SQL databases
- Dictionary tables are ClickHouse-specific and may need alternative implementations in other databases
- Ensure proper data retention policies for large analytics tables
- Consider partitioning strategies for time-series data
