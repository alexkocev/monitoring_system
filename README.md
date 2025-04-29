# GA4 vs Magento Monitoring System

A monitoring system that compares Magento transaction data (source of truth) with GA4 tracking data, providing daily coverage analysis via Slack notifications.

## Overview

This system:
- Fetches transaction and revenue data from BigQuery
- Compares Magento and GA4 data to calculate coverage rates
- Uses Claude AI to analyze trends and generate insights
- Delivers concise, formatted daily reports to Slack

## Features

- Automated daily coverage analysis at 8:00 UTC
- Smart status indicators for critical, warning, and good states
- Transaction and revenue coverage metrics
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

The system is deployed as a Cloud Run job in Google Cloud Platform, triggered daily by Cloud Scheduler.

## Sample Output

```
✅ Coverage Above Threshold - Feb 28, 2025
Transaction coverage has improved above 80% after a week of below-threshold performance. Revenue coverage remains stable.
- *Transactions*: 716 (Magento) vs 589 (GA4)
- *Transaction Coverage*: 82.3% (↑ 3.5%)
- *Revenue*: $40.8K (Magento) vs $32.7K (GA4)
- *Revenue Coverage*: 80.1% (↑ 1.8%)
```

## Maintenance

- Update `requirements.txt` when adding new dependencies
- Rebuild the Docker image after code changes
- Test manually before updating scheduled jobs