# GA4-Magento Monitoring System

A monitoring system that compares Magento transaction data (source of truth) with GA4 tracking data, providing weekly coverage analysis via Slack notifications.

## Overview

This system:
- Fetches transaction, revenue, and other key performance indicators from BigQuery
- Compares Magento and GA4 data to calculate coverage rates
- Uses Claude AI to analyze trends and generate insights
- Delivers concise, formatted weekly reports to Slack

## Features

- Automated weekly coverage analysis at 8:00 UTC
- Smart status indicators for critical, warning, and good states
- Comprehensive KPI monitoring including:
  - Transaction coverage
  - Revenue coverage
  - Product view tracking
  - Add-to-cart events
  - Checkout initiations
  - User engagement metrics
- Trend analysis with historic context
- Cloud-based serverless architecture on Google Cloud Platform

## Technical Stack

- **Backend**: Python 3.9
- **Data Source**: BigQuery
- **AI Analysis**: Claude AI via Anthropic API
- **Notifications**: Slack API
- **Hosting**: Google Cloud Run (scheduled jobs)
- **Scheduling**: Cloud Scheduler
- **Containerization**: Docker

## Environment Variables

The system requires the following environment variables:
- `BQ_PATH_KEY`: Path to the BigQuery service account key file
- `BQ_PROJECT_ID`: BigQuery project ID
- `BQ_DATASET_ID`: BigQuery dataset ID
- `BQ_TABLE_ID`: BigQuery table ID
- `ANTHROPIC_API_KEY`: Claude AI API key
- `SLACK_TOKEN`: Slack API token
- `SLACK_CHANNEL_ID`: Slack channel ID for notifications

## Deployment

The system is deployed as a Cloud Run job in Google Cloud Platform, triggered weekly by Cloud Scheduler.

## Sample Output

```
✅ Weekly KPI Coverage Report - Mar 3-9, 2025
Overall data tracking coverage has improved across most metrics. Revenue tracking shows stability with slight improvement.

Transaction Data:
- *Transactions*: 5,328 (Magento) vs 4,383 (GA4)
- *Transaction Coverage*: 82.3% (↑ 3.5%)
- *Revenue*: $298.2K (Magento) vs $242.5K (GA4)
- *Revenue Coverage*: 81.3% (↑ 1.2%)

User Journey Metrics:
- *Product Views*: 95.2% coverage (↑ 2.1%)
- *Add to Cart*: 87.4% coverage (↑ 4.3%)
- *Checkout Initiations*: 84.9% coverage (↑ 3.7%)

Top Undertracked Categories:
- Accessories: 76.2% revenue coverage
- Sale Items: 79.1% revenue coverage
```

## Maintenance

- Update `requirements.txt` when adding new dependencies
- Rebuild the Docker image after code changes
- Test manually before updating scheduled jobs