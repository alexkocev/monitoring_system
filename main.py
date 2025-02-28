# -----------
# - IMPORTS -
# -----------

from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
from anthropic import Anthropic
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import os
from dotenv import load_dotenv
load_dotenv()
# BigQuery Credentials and Table Information
BQ_PATH_KEY = os.getenv("BQ_PATH_KEY")              # "/path/to/your/service-account-key.json" generated from BQ. Should be in the same directory as this script
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")          # "project-id"
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")        # "dataset-id"
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")        # "table-id"

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")   

SLACK_TOKEN = os.getenv("SLACK_TOKEN")       
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")      

# Set the Google Cloud credentials environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_PATH_KEY

# Initialize a BigQuery client
client = bigquery.Client(project=BQ_PROJECT_ID)
# ------------------------------------------------------------
# -- Fetch and analyze GA4-Magento data from BigQuery -----
# ------------------------------------------------------------

def fetch_existing_data_from_bq():
    try:
        # Check if the table has a schema by getting table metadata
        table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        table = client.get_table(table_ref)

        # If table has no schema, return an empty DataFrame
        if not table.schema:
            print(f"Table {BQ_TABLE_ID} exists but has no schema. Returning empty DataFrame.")
            return pd.DataFrame()

        # If table exists and has a schema, query the data
        query = f"""
            SELECT
            order_date,
            COUNT(DISTINCT magento_transaction_id) AS magento_transactions,
            COUNT(DISTINCT ga4_transaction_id) AS ga4_transactions,
            SAFE_DIVIDE(
                COUNT(DISTINCT ga4_transaction_id), 
                COUNT(DISTINCT magento_transaction_id)
            ) * 100 AS transaction_coverage_rate,
            SUM(magento_revenue) AS magento_revenue,
            SUM(ga4_revenue) AS ga4_revenue,
            SAFE_DIVIDE(
                SUM(ga4_revenue), 
                SUM(magento_revenue)
            ) * 100 AS revenue_coverage_rate
            FROM {BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}
            GROUP BY order_date
            ORDER BY order_date DESC

            """
        query_job = client.query(query)
        df_existing = query_job.to_dataframe()
        return df_existing
    
    except Exception as e:
        print(f"Error fetching data from table {BQ_TABLE_ID}: {str(e)}")
        raise
    
df = fetch_existing_data_from_bq()
# ------------------------------------------------------------
# --    Remove most recent rows as GA4 is not accurate   -----
# ------------------------------------------------------------

def remove_last_24hours_rows(df):
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Get the current time and subtract 24 hours
    current_time = datetime.now()
    time_24_hours_ago = current_time - timedelta(hours=24)

    # Get the day before that time (i.e., yesterday)
    day_before_24_hours = time_24_hours_ago.date()

    # Filter the DataFrame to keep only rows up to the day before the given day
    df_filtered = df[df['order_date'].dt.date < day_before_24_hours]

    return df_filtered

df = remove_last_24hours_rows(df)
# ------------------------------------------------------------
# --       Format df into markdown for Claude analysis   -----
# ------------------------------------------------------------

def generate_table_for_analysis(df):
    # Split data into transactions and revenue sections
    tx_table = df[['order_date', 'magento_transactions', 'ga4_transactions', 'transaction_coverage_rate']]
    rev_table = df[['order_date', 'magento_revenue', 'ga4_revenue', 'revenue_coverage_rate']]
    
    # Convert both tables to markdown
    tx_table_md = tx_table.to_markdown(index=False)
    rev_table_md = rev_table.to_markdown(index=False)
    
    return tx_table_md, rev_table_md
df
# ------------------------------------------------------------
# --   Generate the analysis prompt for Claude based on the tables   -----
# ------------------------------------------------------------

def analyze_with_claude(df):
    # Prepare the tables in markdown format
    tx_table_md, rev_table_md = generate_table_for_analysis(df)
    
    # Build the prompt
    prompt = f"""
    I need you to analyze our GA4-Magento coverage rates and provide a standardized daily report. The data shows a comparison between Magento (our source of truth) and GA4 tracking.

    ## Context
    - The data covers {df.order_date.min().strftime('%b %d')} to {df.order_date.max().strftime('%b %d, %Y')}
    - We consider coverage rates below 80% as concerning and below 50% as critical issues
    - Normal coverage rate for our business is typically between 80-95%
    - The most recent data is from **{df.order_date.max().strftime('%b %d, %Y')}** (note: we wait 24 hours to ensure GA4 data is complete)

    ## Transactions Coverage Data
    This table shows the number of transactions recorded by Magento vs GA4, as well as the transaction coverage rate.
    {tx_table_md}

    ## Revenue Coverage Data
    This table shows the revenue generated according to Magento vs GA4, as well as the revenue coverage rate.
    {rev_table_md}

    ## Output Format Requirements
    Your analysis must strictly follow this format:

    ```
    [EMOJI] *[only few words for the highlight]* - [latest_date]
    Transaction coverage has improved above 80% after a week of below-threshold performance, but revenue coverage remains at the minimum acceptable level. Continued monitoring needed.
    - *Transactions*: 716 (Magento) vs 589 (GA4) - *Coverage*: 82.3% (↑ 3.5%)
    - *Revenue*: $40.8K (Magento) vs $32.7K (GA4) - *Coverage*: 80.1% (↑ 1.8%)
    ```

    ## Status Indicators
    Select the appropriate emoji indicator based on these criteria:
    - **Critical (🚨)**: If there's a significant drop compared to previous data or if coverage is below 50%
    - **Warning (⚠️)**: If coverage is below 80% or if there's a slight drop over several days
    - **Good (✅)**: If coverage is stable and above 70%

    ## Direction Indicators
    For changes:
    - Use (↑) for increases in coverage
    - Use (↓) for decreases in coverage
    - Use (→) for no change in coverage (less than 0.5% change)

    ## Important Notes
    1. Use exactly the format shown above, including bullet points, bolding, and emoji placement
    2. Keep the "Overall Trend" on the second line to 1-2 concise sentences focusing on the most important insight
    3. Round percentages to 1 decimal place
    4. Format numbers with thousands separators (e.g., 1,503)
    5. Format revenue in thousands (K) with 1 decimal place

    Your entire response should be exactly in this format without any additional text or explanations.

    """
    
    # Initialize Anthropic client (using their direct API instead of langchain)
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    
    # Send the prompt to Claude
    response = client.messages.create(
        model="claude-3-7-sonnet-20250219",
        max_tokens=1000,
        temperature=0,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    # Extract analysis text
    analysis = response.content[0].text
    print("Completed Claude analysis")
    
    return analysis

# Example usage:
# Get Claude's analysis of the data tables
analysis = analyze_with_claude(df)

# Output Claude's response
print(analysis)
# ------------------------------------------------------------
# --   Send to Slack   -----
# ------------------------------------------------------------

# Add right before sending to Slack
print(f"About to send message to channel: {SLACK_CHANNEL_ID}")
print(f"Analysis content: {analysis[:100]}...") # Just print the first 100 chars

# Modify the send_message_to_channel function to print more info
def send_message_to_channel(message_text):
    slack_client = WebClient(token=SLACK_TOKEN)
    
    print(f"Slack token starts with: {SLACK_TOKEN[:10]}...")
    
    try:
        # Post message to the channel
        response = slack_client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=message_text
        )
        print(f"Message sent successfully: {response['ts']}")
        return response
    except SlackApiError as e:
        print(f"Error sending message: {e}")
        # Print more details about the error
        if hasattr(e, 'response') and 'error' in e.response:
            print(f"Error details: {e.response['error']}")
        return None

send_message_to_channel(analysis)












