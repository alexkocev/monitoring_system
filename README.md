# Weekly Reporting System

A comprehensive automated reporting solution that delivers weekly ecommerce analysis combining Magento data, Google Analytics 4, and market trends directly to Slack and Google Docs.

## Overview

This system provides a complete weekly performance snapshot that includes:
- Market news and trends analysis affecting ecommerce performance
- Key business KPIs with week-over-week comparison
- Top products performance analysis
- Magento to GA4 data coverage validation
- Visualizations for all metrics with custom scorecards

All analyses are powered by AI to generate actionable insights from raw data.

## Key Features

- **Market Intelligence**: Automated scraping and analysis of industry news, market trends, and events relevant to ecommerce performance
- **Multi-Source KPI Analysis**: Combines Magento (transaction source of truth) with GA4 data for comprehensive KPI tracking:
  - Revenue and transaction metrics
  - Session data and user engagement
  - Conversion rates and funnel analytics
  - Average order value and units per order
- **Product Performance**: Identifies top-performing products with revenue contribution analysis
- **Data Quality Monitoring**: Validates GA4 implementation by comparing tracking data with Magento transactions
- **AI-Powered Insights**: Uses Claude AI to analyze patterns and generate business-relevant insights
- **Multi-Channel Delivery**: Sends formatted reports to both Slack (for immediate team visibility) and Google Docs (for detailed review and sharing)
- **Rich Visualizations**: Custom scorecards and trend visualizations for all metrics

## Delivery Formats

### Slack Report
- Concise, formatted summaries with emoji indicators for status
- Embedded visualizations for key metrics
- Bulleted insights and recommendations
- Link to complete Google Doc report

### Google Docs Report
- Complete analysis with all visualizations
- Properly formatted with headings, bullet points, and highlights
- Suitable for client sharing and executive review
- Structured sections for each area of analysis

## Technical Stack

- **Programming**: Python 3.11
- **Data Sources**: 
  - BigQuery (Magento and GA4 data)
  - Tavily API (Market intelligence)
  - yfinance (Currency conversion)
- **Analysis & ML**: 
  - pandas/numpy (Data processing)
  - Anthropic Claude API (AI insights generation)
- **Visualization**: 
  - matplotlib/seaborn (Custom visualizations)
  - plotly (Interactive charts)
- **Delivery**: 
  - Slack API
  - Google Drive API
  - Google Docs API
- **Infrastructure**:
  - Google Cloud Run (serverless execution)
  - Cloud Scheduler (automated timing)
  - Docker (containerization)
  - Google Cloud Storage (asset storage)

## Configuration

The system requires the following environment variables:

### Data Sources
- `BQ_PATH_KEY`: Path to BigQuery service account key
- `BQ_PROJECT_ID`: BigQuery project ID
- `BQ_DATASET_ID`: BigQuery dataset ID containing the monitoring table
- `BQ_TABLE_ID`: BigQuery table ID for Magento-GA4 combined data
- `BQ_GA4_TABLE_PATH`: Path to raw GA4 events table

### API Keys
- `ANTHROPIC_API_KEY`: Claude AI API key
- `SLACK_TOKEN`: Slack API token
- `TAVILY_KEY`: Tavily API key for market intelligence

### Delivery Configuration
- `SLACK_CHANNEL_ID`: Slack channel ID for report delivery
- `FOLDER_ID`: Google Drive folder ID for storing reports
- `GCP_SERVICE_ACCOUNT_OAUTH`: Path to Google service account credentials

## Deployment

The system is deployed as a containerized Cloud Run job in Google Cloud Platform:

1. Image is built and stored in Google Artifact Registry
2. Cloud Run job is configured with all required environment variables
3. Cloud Scheduler triggers the job weekly (typically Tuesday mornings)
4. Execution logs are available in Google Cloud Logging

## Setup Process

Detailed setup instructions are available in the deployment guide, including:

1. Creating required BigQuery datasets and tables
2. Setting up service accounts with appropriate permissions
3. Configuring Google Drive templates and permissions
4. Deploying the Cloud Run job and scheduler
5. Testing and monitoring the deployment

## Example Output

The system generates multiple visualizations including:
- KPI scorecards with week-over-week comparisons
- Timeline charts for all metrics
- Product contribution analysis 
- GA4-Magento coverage tracking

## Maintenance

To update the system:
1. Make code changes in the repository
2. Rebuild the Docker image
3. Push to Google Artifact Registry
4. Update the Cloud Run job to use the new image

## Requirements

See `requirements.txt` for a complete list of Python dependencies.