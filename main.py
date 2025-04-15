#%%
# -----------
# - IMPORTS -
# -----------

# Standard library imports
import os
import json
import time
import random
from datetime import datetime, timedelta

# Data processing and analysis
import numpy as np
import pandas as pd
from scipy import interpolate

# Google Cloud and API services
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Visualization
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
from matplotlib.ticker import PercentFormatter, MultipleLocator
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# External APIs and services
from anthropic import Anthropic
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tavily import TavilyClient
import yfinance as yf
import requests

# Environment variables
from dotenv import load_dotenv
load_dotenv()

# BigQuery Credentials and Table Information
BQ_PATH_KEY = os.getenv("BQ_PATH_KEY")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")

BQ_GA4_TABLE_PATH = os.getenv("BQ_GA4_TABLE_PATH")
GCP_SERVICE_ACCOUNT_OAUTH = os.getenv("GCP_SERVICE_ACCOUNT_OAUTH")


ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")   
SLACK_TOKEN = os.getenv("SLACK_TOKEN")       
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")      
FOLDER_ID = os.getenv("FOLDER_ID")


# Set the Google Cloud credentials environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_PATH_KEY

# Initialize a BigQuery client
client = bigquery.Client(project=BQ_PROJECT_ID)
# ------------------------------------------------------------
# -- OVERVIEW: date range -----
# ------------------------------------------------------------

def get_analysis_periods(run_date=None):
    """
    Calculate the date periods for analysis: previous week and the week before that.
    
    Parameters:
    run_date (datetime, optional): The date the script is run.
                                  If None, uses current date.
    
    Returns:
    dict: Dictionary containing:
        - 'analysis_period': Dict with 'start' and 'end' dates for the previous week
        - 'previous_period': Dict with 'start' and 'end' dates for the week before the previous week
    """
    
    # If no run date specified, use today
    if run_date is None:
        run_date = datetime.now().date()
    else:
        # Ensure run_date is a date object
        if isinstance(run_date, str):
            run_date = datetime.strptime(run_date, '%Y-%m-%d').date()
        elif isinstance(run_date, datetime):
            run_date = run_date.date()
    
    # Find the previous Monday
    days_since_monday = (run_date.weekday())
    previous_monday = run_date - timedelta(days=days_since_monday + 7)
    previous_sunday = previous_monday + timedelta(days=6)
    
    # Find the Monday before that
    earlier_monday = previous_monday - timedelta(days=7)
    earlier_sunday = earlier_monday + timedelta(days=6)
    
    # Return the periods as a dictionary
    return {
        'analysis_period': {
            'start': previous_monday.strftime('%Y-%m-%d'),
            'end': previous_sunday.strftime('%Y-%m-%d')
        },
        'previous_period': {
            'start': earlier_monday.strftime('%Y-%m-%d'),
            'end': earlier_sunday.strftime('%Y-%m-%d')
        }
    }

get_analysis_periods()
# ------------------------------------------------------------
# -- NEWS: Fetch data using tavily  -----
# ------------------------------------------------------------

def perform_search(client, query, topic="news", time_range="week", max_results=3):
    """Perform a search using the Tavily API."""
    try:
        response = client.search(
            query=query,
            topic=topic,
            time_range=time_range,
            search_depth="advanced",
            max_results=max_results,
            include_images=True,
            include_image_descriptions=True
        )
        return response
    except Exception as e:
        print(f"Error in search: {e}")
        # Return empty result structure instead of failing
        return {"results": []}

def get_holidays(country_code, start_date, end_date):
    """
    Get holidays for a specific country within a date range.
    
    Args:
        country_code (str): Country code (e.g., 'GB' for United Kingdom)
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        list: List of holidays in the specified date range
    """
    # Set a timeout for API requests to prevent hanging
    TIMEOUT = 5  # seconds
    
    # Create a mock holiday list in case the API fails
    mock_holidays = [
        {
            "date": start_date,
            "name": "Holiday information could not be retrieved",
            "localName": "API request failed or timed out",
            "countryCode": country_code
        }
    ]
    
    try:
        # Format dates for API
        start_year = start_date.split('-')[0]
        end_year = end_date.split('-')[0]
        
        # Use the Nager.Date API with timeout
        url = f"https://date.nager.at/api/v3/PublicHolidays/{start_year}/{country_code}"
        
        print(f"Requesting holidays from: {url}")
        response = requests.get(url, timeout=TIMEOUT)
        
        if response.status_code == 200:
            all_holidays = response.json()
            
            # Filter holidays that fall within our date range
            filtered_holidays = []
            for holiday in all_holidays:
                holiday_date = holiday.get('date')
                if start_date <= holiday_date <= end_date:
                    filtered_holidays.append({
                        'date': holiday_date,
                        'name': holiday.get('name'),
                        'localName': holiday.get('localName'),
                        'countryCode': country_code
                    })
            
            # If start and end dates span different years, get holidays for the end year too
            if start_year != end_year:
                url = f"https://date.nager.at/api/v3/PublicHolidays/{end_year}/{country_code}"
                print(f"Requesting additional holidays from: {url}")
                
                response = requests.get(url, timeout=TIMEOUT)
                if response.status_code == 200:
                    end_year_holidays = response.json()
                    for holiday in end_year_holidays:
                        holiday_date = holiday.get('date')
                        if start_date <= holiday_date <= end_date:
                            filtered_holidays.append({
                                'date': holiday_date,
                                'name': holiday.get('name'),
                                'localName': holiday.get('localName'),
                                'countryCode': country_code
                            })
            
            if filtered_holidays:
                return filtered_holidays
            else:
                print("No holidays found in the specified date range")
                return []
        else:
            print(f"Failed to get holidays: HTTP {response.status_code}")
            return []
            
    except requests.exceptions.Timeout:
        print(f"Holiday API request timed out after {TIMEOUT} seconds")
        return []
    except requests.exceptions.ConnectionError:
        print("Holiday API connection error")
        return []
    except Exception as e:
        print(f"Error getting holidays: {e}")
        return []


def fetch_news_from_tavily(periods, country_code="GB"):
    """
    Gather news about Qwertee and relevant ecommerce trends.
    
    Args:
        start_date (str, optional): Start date in 'YYYY-MM-DD' format
        end_date (str, optional): End date in 'YYYY-MM-DD' format
        country_code (str, optional): Country code for holidays (default: "GB" for UK)
    
    Returns:
        dict: Dictionary with all collected results
    """
    # Get the date periods using the get_analysis_periods function
    date_periods = periods
    
    # Extract the date ranges for previous week
    start_date = date_periods['analysis_period']['start']
    end_date = date_periods['analysis_period']['end']
    
    client = TavilyClient(api_key=os.getenv("TAVILY_KEY")  )
    date_range = f"from {start_date} to {end_date}"
    all_results = {}
    
    print(f"Getting holidays for {country_code} {date_range}")
    
    # Get holidays during this period
    holidays = get_holidays(country_code, start_date, end_date)
    
    print(f"Found {len(holidays)} holidays")
    
    # Define search queries for different aspects
    queries = {
        "qwertee_specific": f"Qwertee t-shirt store news updates England {date_range}",
        "ecommerce_trends": f"Ecommerce trends England {date_range}",
        "tshirt_industry": f"T-shirt industry news England {date_range}",
        "weather": f"Weather in England {date_range}",
        "marketing_trends": f"Innovative ecommerce marketing strategies in England {date_range}"
    }
    
    # Uncomment these additional queries once the basic functionality is working
    # queries.update({
    #     "qwertee_social": f"Qwertee social media engagement trends {date_range}",
    #     "pop_culture": f"Latest pop culture trends impacting t-shirt sales UK {date_range}",
    #     "online_retail": f"Online retail performance UK {date_range}",
    #     "consumer_behavior": f"Changes in UK consumer shopping behavior {date_range}",
    #     "competitor_news": f"News about Redbubble TeePublic TeeFury {date_range}",
    #     "supply_chain": f"T-shirt supply chain issues UK {date_range}",
    #     "marketing_trends": f"Innovative ecommerce marketing strategies {date_range}"
    # })
    
    # Execute all searches and collect results
    for category, query in queries.items():
        print(f"Searching for: {query}")
        response = perform_search(client, query)
        all_results[category] = response.get("results", [])
        
        # Add a small status print for each query
        print(f"Found {len(all_results[category])} results for {category}")
        
        # Add a small delay between API calls to avoid rate limiting
        time.sleep(2)
    
    # Save all results to a single JSON file
    combined_results = {
        "collection_date": datetime.now().strftime('%Y-%m-%d'),
        "data_period": date_range,
        "subject": "Qwertee Ecommerce Analysis",
        "country_code": country_code,
        "holidays": holidays,
        "results": all_results
    }
    
    return combined_results

def format_news_for_ai(results):
    """Format the results to be fed into an AI for analysis."""
    formatted_text = f"# Qwertee Market Analysis Report Data\n\n"
    formatted_text += f"Collection Date: {results['collection_date']}\n"
    formatted_text += f"Data Period: {results['data_period']}\n"
    formatted_text += f"Country: {results['country_code']}\n\n"
    
    # Add holidays section
    formatted_text += f"## Holidays and Special Days\n\n"
    if results.get('holidays'):
        for holiday in results['holidays']:
            formatted_text += f"- {holiday['date']}: {holiday['name']} ({holiday['localName']})\n"
    else:
        formatted_text += "No holidays during this period.\n"
    
    formatted_text += "\n"
    
    for category, category_results in results['results'].items():
        formatted_text += f"## {category.replace('_', ' ').title()}\n\n"
        
        if not category_results:
            formatted_text += "No results found for this category.\n\n"
            continue
            
        for item in category_results:
            formatted_text += f"### {item.get('title', 'No Title')}\n"
            formatted_text += f"Source: {item.get('url', 'No URL')}\n"
            formatted_text += f"Published: {item.get('published_date', 'Date unknown')}\n\n"
            formatted_text += f"{item.get('content', 'No content available')}\n\n"
            formatted_text += f"---\n\n"
    
    return formatted_text


# ------------------------------------------------------------
# -- NEWS: Generate the analysis prompt for Claude  -----
# ------------------------------------------------------------
def build_news_summary(formatted_data, store_description):
    """
    Generate a concise one-paragraph summary of major news and market updates for Qwertee.
    
    Parameters:
    formatted_data (str): The formatted analysis text from the qwertee_analysis_for_ai.txt file
    
    Returns:
    str: Anthropic's concise analysis of the Qwertee data
    """
    # Get API key from environment variable
    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
    
    if not ANTHROPIC_API_KEY:
        raise ValueError("No Anthropic API key found. Set ANTHROPIC_API_KEY environment variable.")

    # Build the prompt for Anthropic
    prompt = f"""
    I need you to analyze the following market analysis data and provide a very concise one-paragraph summary of only the major news and market updates that likely impacted the store during this period.

    ## Context
    - We're doing a news summary for this ecommerce store: {store_description}
    - I need a short, focused overview of only the most significant market news and updates
    - The summary should be business-focused and highlight only major events
    
    ## Raw Analysis Data
    ```
    {formatted_data}
    ```

    ## Output Format Requirements
    Your summary must follow this format exactly:

    ```
    [One concise paragraph (4-5 sentences maximum) summarizing only the most important market news, industry updates, and events that likely impacted Qwertee during this period. Focus on factual information rather than analysis.

    - Bullet point 1: Key market event and how it might affect t-shirt retail
    - Bullet point 2: Another important development
    - Bullet point 3: Additional significant news or trend
    - Bullet point 4: Any relevant pop culture, gaming, or geek culture news
    ]
    ```

    ## Important Notes
    1. Use bullet points for clarity, each focused on a different aspect
    2. Make every bullet point directly relevant to a t-shirt ecommerce business
    3. For each bullet point, briefly indicate potential impact on the business
    4. Use factual and direct language
    5. Prioritize information about: retail trends, ecommerce developments, supply chain issues, pop culture events, competitor activities
    6. For bold use 1x* such as *bold*
    7. For bullet points use "• "
    """
    
    # Call the Anthropic API
    try:
        # Initialize Anthropic client
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        
        # Send the prompt to Claude
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",  # Sonnet is sufficient for a concise summary
            max_tokens=300,  # Reduced token count for brevity
            temperature=0,  # Keep it deterministic
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract analysis text
        analysis = response.content[0].text
        analysis = analysis.strip('` \n')
        print("Analysis completed successfully")
        
        return analysis
        
    except Exception as e:
        print(f"Error calling Anthropic API: {str(e)}")
        return f"Error calling AI API: {str(e)}"


"""Process the Qwertee analysis data and generate a concise market update."""

# update = build_news_summary(formatted_data)


# ------------------------------------------------------------
# -- KPIs MAGENTO: Fetch and analyze Magento transaction data from BigQuery -----
# ------------------------------------------------------------

def fetch_magento_kpi_data():
    try:
        # Check if the table has a schema by getting table metadata
        table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        table = client.get_table(table_ref)

        # If table has no schema, return an empty DataFrame
        if not table.schema:
            print(f"Table {BQ_TABLE_ID} exists but has no schema. Returning empty DataFrame.")
            return pd.DataFrame()

        # If table exists and has a schema, query the data
        # This query will process 5.13 MB when run.
        query = f"""
            -- BigQuery SQL to analyze daily e-commerce KPIs with currency conversion to EUR
            -- For the past 90 days, broken down by day

            SELECT
            DATE(order_date) AS order_day,
            currency,
            COUNT(DISTINCT magento_transaction_id) AS transaction_count,
            ROUND(SUM(magento_quantity), 0) AS total_units,
            ROUND(SUM(sub_total), 2) AS total_revenue,
            ROUND(SUM(discount), 2) AS total_discount,
            FROM {BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}
            WHERE
            DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            AND DATE(order_date) < CURRENT_DATE()
            -- Optionally filter by order status if needed
            -- AND order_status IN ('New', 'Completed')
            GROUP BY
            order_day, 
            currency
            ORDER BY
            order_day DESC,
            currency 
            """
        query_job = client.query(query)
        df_existing = query_job.to_dataframe()
        return df_existing
    
    except Exception as e:
        print(f"Error fetching data from table {BQ_TABLE_ID}: {str(e)}")
        raise
    
# df = fetch_existing_data_from_bq()

# ------------------------------------------------------------
# -- KPIs MAGENTO: Convert currency -----
# ------------------------------------------------------------

def convert_currencies_and_group_vectorized(df):
    """
    Convert all currency values to EUR and group by date using vectorized operations
    to avoid FutureWarning about Series.float() deprecation
    
    Parameters:
    df (pandas.DataFrame): DataFrame with currency, total_revenue, and total_discount columns
    
    Returns:
    pandas.DataFrame: Grouped DataFrame with all monetary values in EUR
    """
    # Make a copy of the dataframe to avoid modifying the original
    df_copy = df.copy()
    
    # Convert to appropriate types
    df_copy['total_revenue'] = df_copy['total_revenue'].astype(float)
    df_copy['total_discount'] = df_copy['total_discount'].astype(float)
    
    # Create new EUR columns initialized with the same values
    df_copy['total_revenue_eur'] = df_copy['total_revenue']
    df_copy['total_discount_eur'] = df_copy['total_discount']
    
    # Get unique currencies (except EUR)
    currencies = [c for c in df_copy['currency'].unique() if c != 'EUR']
    
    # Define fallback rates
    fallback_rates = {
        'USD': 0.92, 'GBP': 1.15, 'CHF': 0.96, 'DKK': 0.13,
        'NOK': 0.09, 'SEK': 0.09, 'PLN': 0.23
    }
    
    # Get date range
    min_date = pd.to_datetime(df_copy['order_day']).min()
    max_date = pd.to_datetime(df_copy['order_day']).max()
    
    # Add buffer days
    start_date = (min_date - timedelta(days=5)).strftime('%Y-%m-%d')
    end_date = (max_date + timedelta(days=5)).strftime('%Y-%m-%d')
    
    # Store all rates
    rates_by_currency_date = {}
    
    # Get rates for each currency
    for currency in currencies:
        # Skip EUR
        if currency == 'EUR':
            continue
            
        # Create ticker symbol (e.g., 'USDEUR=X')
        ticker = f"{currency}EUR=X"
        
        try:
            # Fetch data
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            
            if data.empty:
                # Use fallback if no data
                print(f"No data for {currency}, using fallback rate: {fallback_rates.get(currency, 1.0)}")
                
                # Create rates for all dates
                date_range = pd.date_range(start=start_date, end=end_date)
                for date in date_range:
                    date_str = date.strftime('%Y-%m-%d')
                    if date_str not in rates_by_currency_date:
                        rates_by_currency_date[date_str] = {}
                    rates_by_currency_date[date_str][currency] = fallback_rates.get(currency, 1.0)
            else:
                # Use the rates for each date
                for date, row in data.iterrows():
                    date_str = date.strftime('%Y-%m-%d')
                    if date_str not in rates_by_currency_date:
                        rates_by_currency_date[date_str] = {}
                    rates_by_currency_date[date_str][currency] = row['Close']
                
                print(f"Fetched {len(data)} days of exchange rates for {currency}")
        except Exception as e:
            print(f"Error fetching {currency} rate: {e}. Using fallback rate.")
            
            # Use fallback rate for all dates
            date_range = pd.date_range(start=start_date, end=end_date)
            for date in date_range:
                date_str = date.strftime('%Y-%m-%d')
                if date_str not in rates_by_currency_date:
                    rates_by_currency_date[date_str] = {}
                rates_by_currency_date[date_str][currency] = fallback_rates.get(currency, 1.0)
    
    # Convert the dataframe to include date strings for lookup
    df_copy['date_str'] = pd.to_datetime(df_copy['order_day']).dt.strftime('%Y-%m-%d')
    
    # Function to get exchange rate for a date and currency
    def get_rate(date_str, currency):
        # If currency is EUR, rate is 1.0
        if currency == 'EUR':
            return 1.0
        
        # If date exists in our rates
        if date_str in rates_by_currency_date and currency in rates_by_currency_date[date_str]:
            return rates_by_currency_date[date_str][currency]
        
        # Otherwise find closest previous date
        available_dates = sorted([d for d in rates_by_currency_date.keys() if d <= date_str])
        if available_dates:
            closest_date = available_dates[-1]
            if currency in rates_by_currency_date[closest_date]:
                return rates_by_currency_date[closest_date][currency]
        
        # Fallback to fixed rate if nothing else works
        return fallback_rates.get(currency, 1.0)
    
    # Apply conversion for each row - vectorized approach by currency and date
    for currency in df_copy['currency'].unique():
        # Skip EUR
        if currency == 'EUR':
            continue
        
        # Get rows for this currency
        currency_mask = df_copy['currency'] == currency
        
        # Apply conversion date by date for this currency
        for date_str in df_copy.loc[currency_mask, 'date_str'].unique():
            # Get rate for this date and currency
            rate = get_rate(date_str, currency)
            
            # Create a mask for this currency and date
            mask = (df_copy['currency'] == currency) & (df_copy['date_str'] == date_str)
            
            # Apply conversion
            df_copy.loc[mask, 'total_revenue_eur'] = df_copy.loc[mask, 'total_revenue'] * rate
            df_copy.loc[mask, 'total_discount_eur'] = df_copy.loc[mask, 'total_discount'] * rate
    
    # Group by date
    df_grouped = df_copy.groupby('order_day').agg({
        'transaction_count': 'sum',
        'total_units': 'sum',
        'total_revenue_eur': 'sum',
        'total_discount_eur': 'sum'
    }).reset_index()
    
    # Round to 2 decimal places
    df_grouped['total_revenue_eur'] = df_grouped['total_revenue_eur'].round(2)
    df_grouped['total_discount_eur'] = df_grouped['total_discount_eur'].round(2)
    
    # Sort by date, most recent first
    df_grouped = df_grouped.sort_values('order_day', ascending=False)
    
    return df_grouped

# Example usage:
# df_eur = convert_currencies_and_group_vectorized(df)
# ------------------------------------------------------------
# -- KPIs GA4: Fetch Data from GA4     -----
# ------------------------------------------------------------

def fetch_ga4_kpi_data():
    # This query will process 11.95 GB when run.

    query = f"""
    SELECT
    -- Date dimension
    PARSE_DATE('%Y%m%d', event_date) AS date,
    
    -- Revenue metrics (from purchase events)
    SUM(event_value_in_usd) AS total_revenue_usd,
    
    -- Transaction count - using a subquery to find purchase events with transaction IDs
    COUNT(DISTINCT CASE 
        WHEN event_name = 'purchase' THEN 
        (SELECT value.string_value 
        FROM UNNEST(event_params) 
        WHERE key = 'transaction_id')
        ELSE NULL 
    END) AS transactions,
    
    -- User metrics
    COUNT(DISTINCT user_pseudo_id) AS users,
    
    -- Session metrics - counting unique session IDs
    COUNT(DISTINCT 
        (SELECT value.int_value 
        FROM UNNEST(event_params) 
        WHERE key = 'ga_session_id')
    ) AS sessions

    FROM
    `{BQ_GA4_TABLE_PATH}`

    WHERE
    -- Date range - last 90 days
    _TABLE_SUFFIX BETWEEN 
        FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
        AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())

    GROUP BY
    date
    
    ORDER BY
    date DESC
    """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    
    df['date'] = pd.to_datetime(df['date'])
    
    return df
    

# ------------------------------------------------------------
# -- KPIs: Merge Magento and GA4 data     -----
# ------------------------------------------------------------

def merge_data_sources(df_magento_eur, df_ga4):
    """
    Merge BQ Magento data (after currency conversion) with GA4 data into a single DataFrame.
    
    Parameters:
    df_bq_eur (pandas.DataFrame): DataFrame with Magento data already converted to EUR
                                 Must contain: order_day, transaction_count, total_units, 
                                 total_revenue_eur
    df_ga4 (pandas.DataFrame): DataFrame with GA4 data
                              Must contain: date, sessions, transactions, users, ecr
    
    Returns:
    pandas.DataFrame: A merged DataFrame with all metrics
    """
    
    # Ensure date columns are in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df_magento_eur['order_day']):
        df_magento_eur['order_day'] = pd.to_datetime(df_magento_eur['order_day'])
    
    if not pd.api.types.is_datetime64_any_dtype(df_ga4['date']):
        df_ga4['date'] = pd.to_datetime(df_ga4['date'])
    
    # Rename columns for consistency
    df_bq_renamed = df_magento_eur.rename(columns={'order_day': 'date'})
    
    # If ecr does not exist in df_ga4, calculate it
    if 'ecr' not in df_ga4.columns:
        df_ga4['ecr'] = df_ga4.apply(
            lambda row: (row['transactions'] / row['sessions'] * 100) if row['sessions'] > 0 else 0, 
            axis=1
        )
    
    # Merge the two DataFrames on date
    merged_df = pd.merge(
        df_bq_renamed, 
        df_ga4[['date', 'sessions', 'ecr', 'users']], 
        on='date', 
        how='outer'
    )
    
    # Sort by date in descending order (most recent first)
    merged_df = merged_df.sort_values('date', ascending=False)
    
    # Fill NaN values 
    merged_df = merged_df.fillna(0)
    
    return merged_df
# ------------------------------------------------------------
# --           KPIs: calculate KPIs                      -----
# ------------------------------------------------------------
def analyze_weekly_kpis(df_merged, run_date=None):
    """
    Calculate KPIs for the previous week, the week before that, and a timeline
    of the last 60 days for each KPI. All numeric values are converted to native
    Python types and rounded to at most 1 decimal place to reduce token usage.
    
    Parameters:
    df_merged (pandas.DataFrame): Merged DataFrame with daily data containing:
                                 date, transaction_count, total_units, total_revenue_eur,
                                 sessions, ecr
    run_date (datetime, optional): The date the script is run. 
                                  If None, uses current date.
    
    Returns:
    dict: Dictionary containing:
        - 'date_ranges': Date ranges for previous and earlier weeks
        - For each KPI (transaction_count, total_revenue_eur, aov_eur, units_per_order, sessions, ecr):
            - 'previous_week': Value for the previous week (rounded)
            - 'earlier_week': Value for the week before the previous week (rounded)
            - 'percent_change': Percentage change between the weeks (rounded)
            - 'timeline': 60-day timeline of daily values (rounded)
        - 'text': Formatted text summary of the KPI analysis
    """
    
    # Get the date periods using the get_analysis_periods function
    date_periods = get_analysis_periods(run_date)
    
    # Extract the date ranges for previous week and earlier week
    previous_monday = datetime.strptime(date_periods['analysis_period']['start'], '%Y-%m-%d').date()
    previous_sunday = datetime.strptime(date_periods['analysis_period']['end'], '%Y-%m-%d').date()
    earlier_monday = datetime.strptime(date_periods['previous_period']['start'], '%Y-%m-%d').date()
    earlier_sunday = datetime.strptime(date_periods['previous_period']['end'], '%Y-%m-%d').date()
    
    # If no run date specified, use today
    if run_date is None:
        run_date = datetime.now().date()
    else:
        # Ensure run_date is a date object
        if isinstance(run_date, str):
            run_date = datetime.strptime(run_date, '%Y-%m-%d').date()
        elif isinstance(run_date, datetime):
            run_date = run_date.date()
    
    # Convert date to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df_merged['date']):
        df_merged['date'] = pd.to_datetime(df_merged['date'])
    
    # Filter data for previous week
    previous_week_mask = (
        (df_merged['date'] >= pd.Timestamp(previous_monday)) & 
        (df_merged['date'] <= pd.Timestamp(previous_sunday))
    )
    previous_week_data = df_merged[previous_week_mask]
    
    # Filter data for the week before that
    earlier_week_mask = (
        (df_merged['date'] >= pd.Timestamp(earlier_monday)) & 
        (df_merged['date'] <= pd.Timestamp(earlier_sunday))
    )
    earlier_week_data = df_merged[earlier_week_mask]
    
    # Get data for the last 60 days for timeline
    sixty_days_ago = run_date - timedelta(days=60)
    timeline_mask = (df_merged['date'] >= pd.Timestamp(sixty_days_ago))
    timeline_data = df_merged[timeline_mask].sort_values('date')
    
    # Initialize the result dictionary with the date ranges from get_analysis_periods
    result_dict = {
        'date_ranges': date_periods
    }
    
    # Check if we have data for the previous week
    if previous_week_data.empty:
        print(f"Warning: No data found for the previous week ({previous_monday} to {previous_sunday})")
        previous_week_kpis = {
            'transaction_count': 0,
            'total_revenue_eur': 0,
            'aov_eur': 0,
            'units_per_order': 0,
            'sessions': 0,
            'ecr': 0,
        }
    else:
        # Calculate KPIs for previous week
        total_transactions = previous_week_data['transaction_count'].sum()
        total_revenue = previous_week_data['total_revenue_eur'].sum()
        total_units = previous_week_data['total_units'].sum()
        total_sessions = previous_week_data['sessions'].sum()
        
        # Calculate derived KPIs
        aov = total_revenue / total_transactions if total_transactions > 0 else 0
        units_per_order = total_units / total_transactions if total_transactions > 0 else 0
        
        # Calculate eCR as transaction_count / sessions or use the average of eCR values
        # Option 1: Calculate from totals
        ecr_from_totals = (total_transactions / total_sessions * 100) if total_sessions > 0 else 0
        
        # Option 2: Use average of daily eCR values weighted by sessions
        if 'ecr' in previous_week_data.columns and not previous_week_data['ecr'].isnull().all():
            total_weighted_ecr = (previous_week_data['ecr'] * previous_week_data['sessions']).sum()
            ecr_weighted = total_weighted_ecr / total_sessions if total_sessions > 0 else 0
        else:
            ecr_weighted = ecr_from_totals
        
        # Use the pre-calculated eCR if available, otherwise use calculated version
        ecr = ecr_weighted
        
        # Convert to native Python types and round
        previous_week_kpis = {
            'transaction_count': int(total_transactions),
            'total_revenue_eur': round(float(total_revenue), 1),
            'aov_eur': round(float(aov), 1),
            'units_per_order': round(float(units_per_order), 1),
            'sessions': int(total_sessions),
            'ecr': round(float(ecr), 1),
        }
    
    # Check if we have data for the earlier week
    if earlier_week_data.empty:
        print(f"Warning: No data found for the earlier week ({earlier_monday} to {earlier_sunday})")
        earlier_week_kpis = {
            'transaction_count': 0,
            'total_revenue_eur': 0,
            'aov_eur': 0,
            'units_per_order': 0,
            'sessions': 0,
            'ecr': 0,
        }
    else:
        # Calculate KPIs for earlier week
        total_transactions_earlier = earlier_week_data['transaction_count'].sum()
        total_revenue_earlier = earlier_week_data['total_revenue_eur'].sum()
        total_units_earlier = earlier_week_data['total_units'].sum()
        total_sessions_earlier = earlier_week_data['sessions'].sum()
        
        # Calculate derived KPIs
        aov_earlier = total_revenue_earlier / total_transactions_earlier if total_transactions_earlier > 0 else 0
        units_per_order_earlier = total_units_earlier / total_transactions_earlier if total_transactions_earlier > 0 else 0
        
        # Calculate eCR as transaction_count / sessions or use the average of eCR values
        # Option 1: Calculate from totals
        ecr_earlier_from_totals = (total_transactions_earlier / total_sessions_earlier * 100) if total_sessions_earlier > 0 else 0
        
        # Option 2: Use average of daily eCR values weighted by sessions
        if 'ecr' in earlier_week_data.columns and not earlier_week_data['ecr'].isnull().all():
            total_weighted_ecr_earlier = (earlier_week_data['ecr'] * earlier_week_data['sessions']).sum()
            ecr_earlier_weighted = total_weighted_ecr_earlier / total_sessions_earlier if total_sessions_earlier > 0 else 0
        else:
            ecr_earlier_weighted = ecr_earlier_from_totals
        
        # Use the pre-calculated eCR if available, otherwise use calculated version
        ecr_earlier = ecr_earlier_weighted
        
        # Convert to native Python types and round
        earlier_week_kpis = {
            'transaction_count': int(total_transactions_earlier),
            'total_revenue_eur': round(float(total_revenue_earlier), 1),
            'aov_eur': round(float(aov_earlier), 1),
            'units_per_order': round(float(units_per_order_earlier), 1),
            'sessions': int(total_sessions_earlier),
            'ecr': round(float(ecr_earlier), 1),
        }
    
    # Calculate percentage changes
    percent_changes = {}
    for key in previous_week_kpis.keys():
        if earlier_week_kpis[key] == 0:
            # Avoid division by zero
            percent_changes[key] = float('inf') if previous_week_kpis[key] > 0 else 0
        else:
            percent_changes[key] = round(((previous_week_kpis[key] - earlier_week_kpis[key]) / earlier_week_kpis[key]) * 100, 1)
    
    # Generate timeline data for each KPI
    timelines = {}
    
    # Direct KPIs from data
    for kpi in ['transaction_count', 'total_units', 'total_revenue_eur', 'sessions']:
        if kpi in timeline_data.columns:
            # Create a daily series
            timeline_series = timeline_data.set_index('date')[kpi]
            
            # Convert to list of [date, value] pairs for easy plotting
            # Round and convert to native Python types
            if kpi == 'transaction_count' or kpi == 'total_units' or kpi == 'sessions':
                timeline_list = [[date.strftime('%Y-%m-%d'), int(value)] 
                                for date, value in timeline_series.items()]
            else:
                timeline_list = [[date.strftime('%Y-%m-%d'), round(float(value), 1)] 
                                for date, value in timeline_series.items()]
            
            timelines[kpi] = timeline_list
    
    # Handle eCR timeline - use the pre-calculated values if available
    if 'ecr' in timeline_data.columns and not timeline_data['ecr'].isnull().all():
        ecr_timeline = [[date.strftime('%Y-%m-%d'), round(float(value), 1)] 
                     for date, value in timeline_data.set_index('date')['ecr'].items()]
    else:
        # Calculate eCR from transactions and sessions if ecr column not available
        ecr_timeline = []
        for _, row in timeline_data.iterrows():
            date = row['date'].strftime('%Y-%m-%d')
            if row['sessions'] > 0:
                ecr_value = (row['transaction_count'] / row['sessions']) * 100
            else:
                ecr_value = 0
            ecr_timeline.append([date, round(float(ecr_value), 1)])
    
    timelines['ecr'] = ecr_timeline
    
    # Derived KPIs that need calculation
    # Create daily AOV timeline
    aov_timeline = []
    for _, row in timeline_data.iterrows():
        date = row['date'].strftime('%Y-%m-%d')
        transactions = row['transaction_count']
        revenue = row['total_revenue_eur']
        
        # Calculate daily AOV, handling division by zero
        if transactions > 0:
            aov = revenue / transactions
        else:
            aov = 0
            
        aov_timeline.append([date, round(float(aov), 1)])
    
    timelines['aov_eur'] = aov_timeline
    
    # Create daily units per order timeline
    upo_timeline = []
    for _, row in timeline_data.iterrows():
        date = row['date'].strftime('%Y-%m-%d')
        transactions = row['transaction_count']
        units = row['total_units']
        
        # Calculate daily units per order, handling division by zero
        if transactions > 0:
            upo = units / transactions
        else:
            upo = 0
            
        upo_timeline.append([date, round(float(upo), 1)])
    
    timelines['units_per_order'] = upo_timeline
    
    # Build the final KPI data structure
    kpi_data = {}
    for kpi in ['transaction_count', 'total_revenue_eur', 'aov_eur', 'units_per_order', 'sessions', 'ecr']:
        if kpi in previous_week_kpis and kpi in earlier_week_kpis and kpi in percent_changes and kpi in timelines:
            kpi_data[kpi] = {
                'previous_week': previous_week_kpis[kpi],
                'earlier_week': earlier_week_kpis[kpi],
                'percent_change': percent_changes[kpi],
                'timeline': timelines[kpi]
            }
    
    # Format the results as text for display
    formatted_results_text = f"\n{'=' * 50}\n"
    formatted_results_text += f"WEEKLY KPI ANALYSIS: {previous_monday} to {previous_sunday} vs. {earlier_monday} to {earlier_sunday}\n"
    formatted_results_text += f"{'=' * 50}\n"
    formatted_results_text += f"{'KPI':<25} {'Previous Week':<15} {'Earlier Week':<15} {'% Change':<10}\n"
    formatted_results_text += f"{'-' * 65}\n"
    
    for kpi, label in [
        ('total_revenue_eur', 'Total Revenue (EUR)'),
        ('transaction_count', 'Transaction Count'), 
        ('aov_eur', 'AOV (EUR)'),
        ('units_per_order', 'Units per Order'),
        ('sessions', 'Sessions'),
        ('ecr', 'eCommerce CR (%)'),
    ]:
        if kpi in kpi_data:
            prev_val = kpi_data[kpi]['previous_week']
            earlier_val = kpi_data[kpi]['earlier_week']
            pct_change = kpi_data[kpi]['percent_change']
            
            # Format values based on type
            if kpi in ['transaction_count', 'sessions']:
                prev_formatted = f"{int(prev_val):,}"
                earlier_formatted = f"{int(earlier_val):,}"
            elif kpi == 'ecr':
                prev_formatted = f"{prev_val:.1f}%"
                earlier_formatted = f"{earlier_val:.1f}%"
            else:
                prev_formatted = f"{prev_val:,.2f}"
                earlier_formatted = f"{earlier_val:,.2f}"
            
            # Format percent change with appropriate sign
            if pct_change == float('inf'):
                pct_change_formatted = "∞"
            else:
                pct_change_formatted = f"{'+' if pct_change > 0 else ''}{pct_change:.2f}%"
            
            formatted_results_text += f"{label:<25} {prev_formatted:<15} {earlier_formatted:<15} {pct_change_formatted:<10}\n"
    
    # Return the complete result
    return {
        'date_ranges': result_dict['date_ranges'],
        'kpis': kpi_data,
        'text': formatted_results_text
    }

# ------------------------------------------------------------
# -- KPIs: analyse KPIs with Claude -----
# ------------------------------------------------------------
def analyze_kpis_with_claude(store_description, news_report, kpi_data, value_to_analyze=None):
    """
    Generate analyses for KPIs using Claude.
    
    Parameters:
    store_description (str): Description of the store/business
    news_report (str): Recent market news relevant to analysis
    kpi_data (dict): The data dictionary from analyze_weekly_kpis function
    value_to_analyze (str, optional): Specific KPI to analyze. If None, analyzes all KPIs.
    
    Returns:
    dict: Dictionary with KPI names as keys and Claude's analyses as values
    """
    # Extract KPI data from the results
    kpis_data = kpi_data['kpis']
    date_ranges = kpi_data['date_ranges']
    
    # Define KPIs to analyze (now including sessions and ecr)
    all_kpis = ['transaction_count', 'total_revenue_eur', 'aov_eur', 'units_per_order', 'sessions', 'ecr']
    
    # If a specific value is provided, only analyze that one
    kpis_to_analyze = [value_to_analyze] if value_to_analyze else all_kpis
    
    # Create a mapping of display names for readability
    kpi_display_names = {
        'transaction_count': 'Transaction Count',
        'total_revenue_eur': 'Revenue (EUR)',
        'aov_eur': 'Average Order Value (EUR)',
        'units_per_order': 'Units per Order',
        'sessions': 'Sessions',
        'ecr': 'eCommerce Conversion Rate'
    }
    
    # Prepare summary of all KPIs for context
    all_kpis_summary = "\n## Other KPIs Performance Summary\n"
    for kpi in all_kpis:
        if kpi in kpis_data:
            prev_val = kpis_data[kpi]['previous_week']
            earlier_val = kpis_data[kpi]['earlier_week']
            pct_change = kpis_data[kpi]['percent_change']
            
            # Format values based on KPI type
            if kpi in ['transaction_count', 'sessions', 'users']:
                prev_formatted = f"{int(prev_val):,}"
                earlier_formatted = f"{int(earlier_val):,}"
            elif kpi == 'ecr':
                prev_formatted = f"{prev_val:.1f}%"
                earlier_formatted = f"{earlier_val:.1f}%"
            elif kpi in ['total_revenue_eur', 'aov_eur']:
                prev_formatted = f"€{prev_val:,.2f}"
                earlier_formatted = f"€{earlier_val:,.2f}"
            else:
                prev_formatted = f"{prev_val:,.2f}"
                earlier_formatted = f"{earlier_val:,.2f}"
            
            # Format percent change
            if pct_change == float('inf'):
                pct_change_formatted = "NEW"
            else:
                pct_change_formatted = f"{'+' if pct_change > 0 else ''}{pct_change:.1f}%"
            
            all_kpis_summary += f"- {kpi_display_names.get(kpi, kpi)}: {prev_formatted} (vs {earlier_formatted}, {pct_change_formatted})\n"
    
    # Store analyses
    analyses = {}
    
    # Loop through each KPI to analyze
    for kpi_name in kpis_to_analyze:
        if kpi_name not in kpis_data:
            print(f"Warning: {kpi_name} not found in KPI data")
            continue
            
        # Extract data for the current KPI
        kpi_specific_data = kpis_data[kpi_name]
        previous_week_value = kpi_specific_data['previous_week']
        earlier_week_value = kpi_specific_data['earlier_week']
        percent_change = kpi_specific_data['percent_change']
        
        # Format KPI name for display
        kpi_display_name = kpi_display_names.get(kpi_name, kpi_name)
        
        # Create timeline data string for reference
        timeline_data = ""
        if 'timeline' in kpi_specific_data and kpi_specific_data['timeline']:
            # Take the last 14 days to keep it concise
            recent_timeline = kpi_specific_data['timeline'][-14:]
            timeline_data = "\n\n## Recent Daily Values\n"
            for date, value in recent_timeline:
                if isinstance(value, (int, float)):
                    if kpi_name == 'total_revenue_eur' or kpi_name == 'aov_eur':
                        formatted_value = f"€{value:,.2f}"
                    elif kpi_name == 'ecr':
                        formatted_value = f"{value:,.1f}%"
                    else:
                        formatted_value = f"{value:,}"
                else:
                    formatted_value = str(value)
                
                timeline_data += f"{date}: {formatted_value}\n"
        
        # Build the prompt for Claude
        prompt = f"""
        I need you to analyze one specific KPI from our e-commerce data and provide a focused explanation of what it indicates. You will receive store context, market news, and KPI data, along with how other KPIs are performing for context.

        ## Store Context
        {store_description}

        ## Recent Market News
        {news_report}

        ## Analysis Period
        - Current period: {date_ranges['analysis_period']['start']} to {date_ranges['analysis_period']['end']}
        - Previous period: {date_ranges['previous_period']['start']} to {date_ranges['previous_period']['end']}

        ## KPI to Analyze: {kpi_display_name}
        - Current period value: {previous_week_value}{' sessions' if kpi_name == 'sessions' else '%' if kpi_name == 'ecr' else ''}
        - Previous period value: {earlier_week_value}{' sessions' if kpi_name == 'sessions' else '%' if kpi_name == 'ecr' else ''}
        - Percent change: {percent_change}%{timeline_data}
        {all_kpis_summary}

        ## Instructions
        Provide 1-2 paragraphs (4-6 sentences total) that explain:
        1. What the trend indicates about our business performance
        2. Possible explanations for the trend, considering:
        • The store context (t-shirt business with 24-48 hour availability)
        • Recent market news
        • Other KPIs' performance
        3. Important patterns in the timeline data
        4. A brief suggestion on what to monitor next

        Important formatting guidelines:
        - Use bullet points for listing key insights (2-3 bullets)
        - Bold any critical insights or numbers
        - Keep language clear and direct
        - Focus on actionable insights
        - Include numeric data to support your points
        - For bold use 1x* such as *bold*
        - For bullet points use "• "
        """
        
        try:
            # Initialize Anthropic client
            client = Anthropic(api_key=ANTHROPIC_API_KEY)
            
            # Send the prompt to Claude
            response = client.messages.create(
                model="claude-3-5-sonnet-20241022",  # Sonnet is sufficient for this analysis
                max_tokens=500,
                temperature=0,  # Keep it deterministic
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract analysis text
            analysis = response.content[0].text
            print(f"Analysis for {kpi_name} completed successfully")
            
            # Store the analysis
            analyses[kpi_name] = analysis.strip()
            
        except Exception as e:
            print(f"Error analyzing {kpi_name}: {str(e)}")
            analyses[kpi_name] = f"Error: Could not analyze {kpi_display_name}"
    
    return analyses
# ------------------------------------------------------------
# -- KPIs: viz scorecard and timeline for all KPIs -----
# ------------------------------------------------------------
def create_kpi_scorecard_with_timeline(kpi_results, output_dir='images'):
    """
    Create visualizations for each KPI showing a timeline chart on the left
    and a scorecard with the value and change on the right.
    
    Parameters:
    kpi_results (dict): The results dictionary from the analyze_weekly_kpis function
                        Contains 'kpis' with data for each KPI (including sessions and ecr)
    output_dir (str): Directory to save the plot images
    
    Returns:
    dict: Dictionary with paths to all generated plot files
    """

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract data
    kpis_data = kpi_results['kpis']
    date_ranges = kpi_results['date_ranges']
    
    # Get the start and end date of the analysis period
    analysis_start_date = datetime.strptime(date_ranges['analysis_period']['start'], '%Y-%m-%d')
    analysis_end_date = datetime.strptime(date_ranges['analysis_period']['end'], '%Y-%m-%d')
    
    # Format the date ranges for display
    current_date_range = f"{date_ranges['analysis_period']['start']} to {date_ranges['analysis_period']['end']}"
    previous_date_range = f"{date_ranges['previous_period']['start']} to {date_ranges['previous_period']['end']}"
    
    # KPI information for visualization - now including sessions and ecr
    kpi_info = {
        'transaction_count': {
            'label': 'Transactions',
            'format': '{:,}',
            'color': '#1f77b4',
            'y_format': '{:,.0f}'
        },
        'total_revenue_eur': {
            'label': 'Revenue (EUR)',
            'format': '€{:,.1f}',  # We'll modify this dynamically for each KPI below
            'color': '#2ca02c',
            'y_format': '€{:,.0f}'
        },
        'aov_eur': {
            'label': 'AOV (EUR)',
            'format': '€{:,.1f}',
            'color': '#ff7f0e',
            'y_format': '€{:,.1f}'
        },
        'units_per_order': {
            'label': 'Units per Order',
            'format': '{:,.1f}',
            'color': '#9467bd',
            'y_format': '{:,.1f}'
        },
        'sessions': {
            'label': 'Sessions',
            'format': '{:,}',
            'color': '#9c27b0',  # Purple for sessions
            'y_format': '{:,.0f}'
        },
        'ecr': {
            'label': 'eCommerce CR (%)',
            'format': '{:.1f}%',
            'color': '#ff9800',  # Orange for eCR
            'y_format': '{:.1f}%'
        },
        'users': {
            'label': 'Users',
            'format': '{:,}',
            'color': '#17becf',  # Teal for users
            'y_format': '{:,.0f}'
        }
    }
    
    # Generate plots for each KPI
    plot_paths = {}
    
    for kpi_name, kpi_info_item in kpi_info.items():
        # Skip if this KPI isn't in the results
        if kpi_name not in kpis_data:
            print(f"Skipping visualization for {kpi_info_item['label']} - not in results")
            continue
            
        kpi_data = kpis_data[kpi_name]
        
        # Create figure with 2-column grid - timeline on left, scorecard on right
        fig = plt.figure(figsize=(14, 5))
        gs = gridspec.GridSpec(1, 2, width_ratios=[2, 1])  # Reversed ratio from before
        
        # Create timeline on the left column
        ax_timeline = plt.subplot(gs[0])
        create_kpi_timeline(
            ax_timeline, 
            kpi_data['timeline'], 
            kpi_info_item['label'], 
            kpi_info_item['color'],
            kpi_info_item['y_format'],
            analysis_start_date,
            analysis_end_date
        )
        
        # Get the KPI value
        value = kpi_data['previous_week']
        
        # Determine the format to use - special handling for revenue values
        if kpi_name == 'total_revenue_eur' and value >= 1000:
            # For revenue over 1000, use 'k' format
            value_format = '€{:.1f}k'
            value_to_display = value / 1000  # Convert to thousands
        else:
            # Use the standard format from kpi_info
            value_format = kpi_info_item['format']
            value_to_display = value
        
        # Create scorecard in right column
        ax_card = plt.subplot(gs[1])
        create_kpi_scorecard(
            ax_card, 
            kpi_info_item['label'], 
            current_date_range,
            value_to_display, 
            kpi_data['percent_change'],
            value_format,
            previous_date_range
        )
        
        # Adjust layout
        plt.tight_layout()
        
        # Save the plot
        output_path = os.path.join(output_dir, f"{kpi_name}_with_timeline.png")
        plt.savefig(output_path, dpi=120, bbox_inches='tight')
        plt.close()
        
        plot_paths[kpi_name] = output_path
        print(f"Created visualization for {kpi_info_item['label']} at {output_path}")
    
    return plot_paths

def create_kpi_scorecard(ax, label, date_range, value, percent_change, value_format='{:,.2f}', comparison_date_range=None):
    """
    Create a KPI scorecard with value and percent change on a given axis.
    Includes date ranges under the title and below the percentage change.
    
    Parameters:
    ax: Matplotlib axis
    label: KPI label
    date_range: Date range string for the current period
    value: KPI value
    percent_change: Percentage change
    value_format: Format string for the value
    comparison_date_range: Date range string for the comparison period
    """
    # Clear the axis
    ax.clear()
    
    # Hide the axes
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    # Remove the ticks
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Set background color to white
    ax.set_facecolor('#ffffff')
    
    # Format the value
    if isinstance(value_format, str):
        # Check if we need to format as 'k' for thousands
        if 'k' in value_format and value >= 1000:
            value_str = value_format.format(value / 1000)
        else:
            value_str = value_format.format(value)
    else:
        value_str = f"{value:,}"
        
    # Format percent change and determine color
    if percent_change == float('inf'):
        pct_change_str = "NEW"
        color = '#3498db'  # Blue for new
    elif percent_change > 0:
        pct_change_str = f"+{percent_change:.1f}%"
        color = '#27ae60'  # Green for positive
    elif percent_change < 0:
        pct_change_str = f"{percent_change:.1f}%"
        color = '#e74c3c'  # Red for negative
    else:
        pct_change_str = "0%"
        color = '#95a5a6'  # Gray for no change
    
    # Add the title/label at the top
    ax.text(0.5, 0.95, label, fontsize=20, ha='center', va='center', fontweight='bold')
    
    # Add the date range subtitle below the title
    ax.text(0.5, 0.85, date_range, fontsize=12, ha='center', va='center', fontstyle='italic')
    
    # Add the main value in the center
    ax.text(0.5, 0.5, value_str, fontsize=32, ha='center', va='center', fontweight='bold')
    
    # Add the percent change with background
    ax.text(0.5, 0.15, pct_change_str, fontsize=18, ha='center', va='center', 
            fontweight='bold', color='white',
            bbox=dict(boxstyle="round,pad=0.5", facecolor=color, alpha=0.9))
    
    # Add the comparison date range subtitle below the percent change
    if comparison_date_range:
        ax.text(0.5, 0.05, f"vs {comparison_date_range}", fontsize=12, ha='center', va='center', 
               fontstyle='italic', color='#555555')

def create_kpi_timeline(ax, timeline_data, label, color='#1f77b4', y_format='{:,.0f}', 
                         analysis_start_date=None, analysis_end_date=None):
    """
    Create a timeline chart for a KPI on a given axis with the analysis period highlighted.
    Only data up to the analysis end date will be shown with the analysis period in darker color.
    
    Parameters:
    ax: Matplotlib axis
    timeline_data: List of [date, value] pairs
    label: KPI label for the y-axis
    color: Line color
    y_format: Format string for y-axis labels
    analysis_start_date: The start date of the analysis period
    analysis_end_date: The end date of the analysis period
    """
    
    # Extract dates and values
    dates = [datetime.strptime(item[0], '%Y-%m-%d') for item in timeline_data]
    values = [item[1] for item in timeline_data]
    
    # Filter data to only include dates up to the analysis end date
    if analysis_end_date:
        filtered_mask = np.array([date <= analysis_end_date for date in dates])
        filtered_dates = np.array(dates)[filtered_mask]
        filtered_values = np.array(values)[filtered_mask]
    else:
        filtered_dates = np.array(dates)
        filtered_values = np.array(values)
    
    # Plot a single continuous line for the filtered timeline
    ax.plot(filtered_dates, filtered_values, color=color, linewidth=2.5)
    
    # Find the analysis period date range
    if filtered_dates.size > 0 and analysis_start_date and analysis_end_date:
        # Create masks for analysis period and earlier periods
        analysis_period_mask = np.array([
            (date >= analysis_start_date) & (date <= analysis_end_date) 
            for date in filtered_dates
        ])
        earlier_mask = np.array([(date < analysis_start_date) for date in filtered_dates])
        
        # Split the data into two parts: analysis period and earlier
        analysis_period_dates = filtered_dates[analysis_period_mask]
        analysis_period_values = filtered_values[analysis_period_mask]
        earlier_dates = filtered_dates[earlier_mask]
        earlier_values = filtered_values[earlier_mask]
        
        # Add fill for earlier period with lighter opacity
        if len(earlier_dates) > 0:
            ax.fill_between(earlier_dates, earlier_values, color=color, alpha=0.1)
        
        # Add fill for analysis period with darker opacity
        if len(analysis_period_dates) > 0:
            ax.fill_between(analysis_period_dates, analysis_period_values, color=color, alpha=0.3)
            
            # Add a subtle vertical line at the start of the analysis period
            if len(earlier_dates) > 0:  # Only if we have earlier data
                ax.axvline(x=analysis_start_date, color='#999999', linestyle='--', alpha=0.3)
    else:
        # Fallback if no dates or analysis dates
        ax.fill_between(filtered_dates, filtered_values, color=color, alpha=0.1)
    
    # Format the x-axis to show dates horizontally (no rotation)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=14))  # Bi-weekly ticks
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)  # Horizontal labels
    
    # Format y-axis with custom formatter
    if y_format:
        from matplotlib.ticker import FuncFormatter
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: y_format.format(x)))
    
    # Set grid, only for y-axis
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    
    # Remove top and right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Set y-axis label
    ax.set_ylabel(label)
    
    # Remove x-axis label
    ax.set_xlabel('')

# ------------------------------------------------------------
# -- PRODUCT: Fetch Product Data from GA4     -----
# ------------------------------------------------------------

def fetch_product_data():

    # If table exists and has a schema, query the data
    # This query will process 726.62 MB when run.

    query = f"""
    SELECT
    -- Date dimension
    PARSE_DATE('%Y%m%d', event_date) AS date,
    
    -- Product dimension
    items.item_name AS product_name,
    items.item_id AS product_id,
    items.item_category AS product_category,
    
    -- Metrics
    SUM(items.item_revenue) AS revenue,
    SUM(items.quantity) AS quantity,
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions
    
    FROM
    `{BQ_GA4_TABLE_PATH}`,
    UNNEST(items) AS items
    WHERE
    -- Filter for purchase events only
    event_name = 'purchase'
    -- Date range
    AND _TABLE_SUFFIX BETWEEN 
    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    -- Ensure we have valid product data
    AND items.item_name IS NOT NULL
    
    GROUP BY
    date,
    product_name,
    product_id,
    product_category
    
    ORDER BY
    date DESC,
    revenue DESC
    """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    
    df['date'] = pd.to_datetime(df['date'])
    
    return df
    

# ------------------------------------------------------------
# -- PRODUCT: Generate Product Data     -----
# ------------------------------------------------------------
def analyze_top_products(df_product, run_date=None, top_n=5):
    """
    Analyze the top N products by revenue for the analysis period and calculate
    daily percentage contribution to total revenue for the timeline.
    
    Parameters:
    df_product (pandas.DataFrame): DataFrame with product data containing:
                                 date, product_name, revenue, quantity, transactions
    run_date (datetime, optional): The date the script is run. If None, uses current date.
    top_n (int, optional): Number of top products to analyze
    
    Returns:
    dict: Dictionary containing:
        - 'date_ranges': Date ranges for previous and earlier weeks
        - 'kpis': Dictionary with product_name keys, each containing:
            - 'previous_week': Value for the previous week
            - 'earlier_week': Value for the week before the previous week
            - 'percent_change': Percentage change between the weeks
            - 'revenue_percent': Percentage of total revenue for the previous week
            - 'earlier_revenue_percent': Percentage of total revenue for the earlier week
            - 'timeline': 60-day timeline of daily percentage values (% of daily total revenue)
    """
    
    # Make sure top_n is a number
    if not isinstance(top_n, int) or top_n <= 0:
        top_n = 5
        print(f"Warning: Invalid top_n value. Using default value of 5.")
    
    # Handle run_date
    if run_date is not None:
        # Check if run_date is valid
        if isinstance(run_date, (int, float, bool)):
            print(f"Warning: Invalid run_date value ({run_date}). Using current date instead.")
            run_date = None
        else:
            # Ensure run_date is a date object
            try:
                if isinstance(run_date, str):
                    run_date = datetime.strptime(run_date, '%Y-%m-%d').date()
                elif isinstance(run_date, datetime):
                    run_date = run_date.date()
            except Exception as e:
                print(f"Warning: Could not parse run_date '{run_date}': {str(e)}. Using current date instead.")
                run_date = None
    
    # If no valid run_date specified, use today
    if run_date is None:
        run_date = datetime.now().date()
        
    # Get the date periods using the get_analysis_periods function
    date_periods = get_analysis_periods(run_date)
    
    # Extract date ranges
    analysis_start = datetime.strptime(date_periods['analysis_period']['start'], '%Y-%m-%d')
    analysis_end = datetime.strptime(date_periods['analysis_period']['end'], '%Y-%m-%d')
    previous_start = datetime.strptime(date_periods['previous_period']['start'], '%Y-%m-%d')
    previous_end = datetime.strptime(date_periods['previous_period']['end'], '%Y-%m-%d')
    
    # Clean up the input DataFrame - make a copy to avoid warnings
    df_clean = df_product.copy()
    
    # Convert date to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df_clean['date']):
        df_clean['date'] = pd.to_datetime(df_clean['date'])
    
    # Add a date_only column for easier grouping
    df_clean['date_only'] = df_clean['date'].dt.date
    
    # Replace NaN values in revenue with 0
    df_clean['revenue'] = df_clean['revenue'].fillna(0)
    
    # Filter data for the analysis period (previous week)
    analysis_mask = (
        (df_clean['date'] >= pd.Timestamp(analysis_start)) & 
        (df_clean['date'] <= pd.Timestamp(analysis_end))
    )
    analysis_data = df_clean[analysis_mask]
    
    # Filter data for the previous period (earlier week)
    previous_mask = (
        (df_clean['date'] >= pd.Timestamp(previous_start)) & 
        (df_clean['date'] <= pd.Timestamp(previous_end))
    )
    previous_data = df_clean[previous_mask]
    
    # Get data for the last 60 days for timeline
    sixty_days_ago = run_date - timedelta(days=60)
    timeline_mask = (df_clean['date'] >= pd.Timestamp(sixty_days_ago))
    timeline_data = df_clean[timeline_mask]
    
    # Group by product and calculate total revenue for the analysis period
    analysis_product_totals = analysis_data.groupby('product_name')['revenue'].sum().reset_index()
    
    # Calculate the total revenue for analysis period - convert to standard float
    analysis_total_revenue = float(analysis_product_totals['revenue'].sum())
    
    # Sort by revenue and get top N products
    top_products = analysis_product_totals.sort_values('revenue', ascending=False).head(top_n)
    top_product_names = top_products['product_name'].tolist()
    
    # Group by product for the previous period
    previous_product_totals = previous_data.groupby('product_name')['revenue'].sum().reset_index()
    
    # Calculate the total revenue for previous period - convert to standard float
    previous_total_revenue = float(previous_product_totals['revenue'].sum())
    
    # Initialize the result structure
    result_dict = {
        'date_ranges': date_periods,
        'kpis': {},
        'text': '',
        'top_product_names': top_product_names,
        'total_revenue': {
            'previous_week': round(float(analysis_total_revenue), 1),
            'earlier_week': round(float(previous_total_revenue), 1)
        }
    }
    
    # Calculate daily totals for the timeline period
    # Group by date_only and sum revenue - convert to standard dict with float values
    daily_totals_series = timeline_data.groupby('date_only')['revenue'].sum()
    daily_totals = {date: float(total) for date, total in daily_totals_series.items()}
    
    # Process each top product
    for product_name in top_product_names:
        # Get data for analysis period (previous week) - convert to standard float
        analysis_row = analysis_product_totals[analysis_product_totals['product_name'] == product_name]
        analysis_revenue = float(analysis_row['revenue'].values[0]) if not analysis_row.empty else 0.0
        
        # Get data for previous period (earlier week) - convert to standard float
        previous_row = previous_product_totals[previous_product_totals['product_name'] == product_name]
        previous_revenue = float(previous_row['revenue'].values[0]) if not previous_row.empty else 0.0
        
        # Calculate percent change with standard float and round to 1 decimal
        if previous_revenue > 0:
            percent_change = round(float((analysis_revenue - previous_revenue) / previous_revenue * 100), 1)
        else:
            percent_change = float('inf') if analysis_revenue > 0 else 0.0
        
        # Calculate revenue percentage contribution with standard float and round to 1 decimal
        revenue_percent = 0.0
        if analysis_total_revenue > 0:
            revenue_percent = round(float(analysis_revenue / analysis_total_revenue * 100), 1)
            
        # Calculate earlier revenue percentage contribution with standard float and round to 1 decimal
        earlier_revenue_percent = 0.0
        if previous_total_revenue > 0:
            earlier_revenue_percent = round(float(previous_revenue / previous_total_revenue * 100), 1)
        
        # Calculate daily percentage contribution for timeline
        # Get product revenue by day and convert to standard dict with float values
        product_timeline = timeline_data[timeline_data['product_name'] == product_name]
        product_daily_series = product_timeline.groupby('date_only')['revenue'].sum()
        product_daily = {date: float(total) for date, total in product_daily_series.items()}
        
        # Calculate percentage of daily total for each date
        timeline_list = []
        
        # Go through all dates in the timeline period
        for date in sorted(daily_totals.keys()):
            date_str = date.strftime('%Y-%m-%d')
            
            # Get total revenue for this date
            date_total = daily_totals[date]
            
            # Get product revenue for this date (0 if no data)
            product_rev = product_daily.get(date, 0.0)
            
            # Calculate percentage with standard float and round to 1 decimal
            if date_total > 0:
                pct_of_daily = round(float(product_rev / date_total * 100), 1)
            else:
                pct_of_daily = 0.0
                
            # Add to timeline - ensure both values are standard Python types
            timeline_list.append([str(date_str), float(pct_of_daily)])
        
        # Store in the KPIs structure with properly rounded standard float values
        result_dict['kpis'][product_name] = {
            'previous_week_revenue_eur': round(float(analysis_revenue), 1),
            'earlier_week_revenue_eur': round(float(previous_revenue), 1),
            'percent_change': float(percent_change),
            'revenue_eur_share_pct': float(revenue_percent),
            'earlier_revenue_eur_share_pctt': float(earlier_revenue_percent),
            'daily_share_pct': timeline_list
        }
    
    # Generate text summary
    formatted_results_text = f"\n{'=' * 70}\n"
    formatted_results_text += f"TOP PRODUCTS ANALYSIS: {analysis_start.strftime('%Y-%m-%d')} to {analysis_end.strftime('%Y-%m-%d')} vs. {previous_start.strftime('%Y-%m-%d')} to {previous_end.strftime('%Y-%m-%d')}\n"
    formatted_results_text += f"{'=' * 70}\n"
    formatted_results_text += f"{'Product':<25} {'Current Week':<15} {'% of Total':<10} {'Previous Week':<15} {'% of Total':<10} {'% Change':<10}\n"
    formatted_results_text += f"{'-' * 87}\n"
    
    for product_name in top_product_names:
        product_data = result_dict['kpis'][product_name]
        
        # Truncate long product names for display
        display_name = (product_name[:22] + '...') if len(product_name) > 25 else product_name
        
        # Format the values
        current_formatted = f"€{product_data['previous_week_revenue_eur']:,.1f}"
        current_pct_formatted = f"{product_data['percent_change']:,.1f}%"
        previous_formatted = f"€{product_data['earlier_week_revenue_eur']:,.1f}"
        previous_pct_formatted = f"{product_data['earlier_revenue_eur_share_pctt']:,.1f}%"
        
        # Format percent change with appropriate sign
        pct_change = product_data['percent_change']
        if pct_change == float('inf'):
            pct_change_formatted = "NEW"
        else:
            pct_change_formatted = f"{'+' if pct_change > 0 else ''}{pct_change:.1f}%"
        
        formatted_results_text += f"{display_name:<25} {current_formatted:<15} {current_pct_formatted:<10} {previous_formatted:<15} {previous_pct_formatted:<10} {pct_change_formatted:<10}\n"
    
    # Add totals row
    formatted_results_text += f"{'-' * 87}\n"
    formatted_results_text += f"{'TOTAL':<25} {'€{:,.1f}'.format(result_dict['total_revenue']['previous_week']):<15} {'100.0%':<10} {'€{:,.1f}'.format(result_dict['total_revenue']['earlier_week']):<15} {'100.0%':<10} "
    
    # Calculate total percent change as standard float with 1 decimal place
    if result_dict['total_revenue']['earlier_week'] > 0:
        total_pct_change = round(float((result_dict['total_revenue']['previous_week'] - result_dict['total_revenue']['earlier_week']) / 
                           result_dict['total_revenue']['earlier_week'] * 100), 1)
        total_pct_formatted = f"{'+' if total_pct_change > 0 else ''}{total_pct_change:.1f}%"
    else:
        total_pct_formatted = "N/A"
    
    formatted_results_text += f"{total_pct_formatted:<10}\n"
    
    # Add note about top products percentage
    top_products_total = sum(result_dict['kpis'][product]['previous_week_revenue_eur'] for product in top_product_names)
    top_products_percent = round(float(top_products_total / result_dict['total_revenue']['previous_week'] * 100), 1) if result_dict['total_revenue']['previous_week'] > 0 else 0.0
    
    formatted_results_text += f"\nNote: Top {len(top_product_names)} products represent {top_products_percent:.1f}% of total revenue in current period.\n"
    
    # Add the text summary to the result
    result_dict['text'] = formatted_results_text
    
    return result_dict

# ------------------------------------------------------------
# -- PRODUCT: Generate Product visualisations     -----
# ------------------------------------------------------------


def create_product_comparison_chart(analysis_results, output_dir='images'):
    """
    Create a horizontal bar chart comparing product revenue percentage contribution
    between periods, with the current week bar positioned ABOVE the previous week bar
    for each product.
    
    Parameters:
    analysis_results (dict): Dictionary with analysis results from analyze_top_products
    output_dir (str): Directory to save the plot images
    
    Returns:
    str: Path to the generated plot
    """

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract data
    product_names = analysis_results['top_product_names']
    date_ranges = analysis_results['date_ranges']
    analysis_period = date_ranges['analysis_period']
    previous_period = date_ranges['previous_period']
    
    # Prepare data for plotting
    current_percentages = []
    previous_percentages = []
    
    # Get data for each product
    for product_name in product_names:
        product_data = analysis_results['kpis'][product_name]
        current_percentages.append(product_data['revenue_eur_share_pct'])
        previous_percentages.append(product_data['earlier_revenue_eur_share_pctt'])
    
    # Create sorted data for plotting (descending by current percentages)
    # Zip all data together for sorting
    sorted_data = sorted(zip(product_names, current_percentages, previous_percentages), 
                         key=lambda x: x[1], reverse=True)
    
    # Unpack the sorted data
    sorted_product_names = [item[0] for item in sorted_data]
    sorted_current_percentages = [item[1] for item in sorted_data]
    sorted_previous_percentages = [item[2] for item in sorted_data]
    
    # Create figure and axis with reduced margins
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.subplots_adjust(left=0.1, right=0.95, top=0.9, bottom=0.1)
    
    # Set up bar positions with minimal space between products to maximize bar area
    number_of_products = len(sorted_product_names)
    bar_width = 0.40  # Wider bars
    y_spacing = 0.5   # Reduced spacing between different product groups
    
    # Create positions for each product group
    group_positions = np.arange(number_of_products) * (2 * bar_width + y_spacing)
    
    # Create positions for the two bars per product
    # CRITICAL: Current week (dark blue) bar is positioned ABOVE the previous week bar
    current_week_positions = group_positions - 0.2  # Top bar (current week)
    previous_week_positions = group_positions + 0.2  # Bottom bar (previous week)
    
    # Set the colors with current week more prominent
    current_color = '#3498db'  # Bright blue for current week
    previous_color = '#99c2e9'  # Lighter blue for previous week
    
    # Create the bars
    current_bars = ax.barh(current_week_positions, sorted_current_percentages, bar_width,
                           label=f"Current Week ({analysis_period['start']} to {analysis_period['end']})",
                           color=current_color)
    
    previous_bars = ax.barh(previous_week_positions, sorted_previous_percentages, bar_width,
                            label=f"Previous Week ({previous_period['start']} to {previous_period['end']})",
                            color=previous_color)
    
    # Add percentage labels next to the bars
    # Define offset for labels - smaller offset to keep labels closer to bars
    max_value = max(max(sorted_current_percentages) if sorted_current_percentages else 0,
                    max(sorted_previous_percentages) if sorted_previous_percentages else 0)
    offset = max_value * 0.01  # Reduced offset
    
    for i, (cur_pct, prev_pct) in enumerate(zip(sorted_current_percentages, sorted_previous_percentages)):
        # Current period percentage
        ax.text(cur_pct + offset, current_week_positions[i], f"{cur_pct:.1f}%",
                va='center', ha='left', color='#333333', fontsize=9)
        
        # Previous period percentage
        if prev_pct > 0:
            ax.text(prev_pct + offset, previous_week_positions[i], f"{prev_pct:.1f}%",
                    va='center', ha='left', color='#555555', fontsize=9)
    
    # Set y-ticks in the middle of each product group
    ax.set_yticks(group_positions)
    
    # Truncate long product names
    shortened_names = [name[:25] + '...' if len(name) > 28 else name for name in sorted_product_names]
    
    # Set the y-tick labels
    ax.set_yticklabels(shortened_names, fontsize=10)
    
    # Invert the y-axis to get the highest product at the top
    ax.invert_yaxis()
    
    # Calculate appropriate x-axis limit based on the data
    max_percentage = max(max(sorted_current_percentages) if sorted_current_percentages else 0,
                         max(sorted_previous_percentages) if sorted_previous_percentages else 0)
    # Add just a small padding (10% of max value)
    padding = max_percentage * 0.1
    max_x = max_percentage + padding
    
    # Ensure max_x is not absurdly large (cap at 100% if data is reasonable)
    if max_x > 100 and max_percentage < 90:
        max_x = 100
    
    # Set x-axis limits - start from 0 for proper comparison
    ax.set_xlim(0, max_x)
    
    # Set x-axis tick spacing based on data range
    if max_x <= 10:
        ax.xaxis.set_major_locator(MultipleLocator(1.0))  # 1% increments
    elif max_x <= 20:
        ax.xaxis.set_major_locator(MultipleLocator(2.0))  # 2% increments
    elif max_x <= 50:
        ax.xaxis.set_major_locator(MultipleLocator(5.0))  # 5% increments
    else:
        ax.xaxis.set_major_locator(MultipleLocator(10.0))  # 10% increments
    
    # Format x-axis as percentages
    ax.xaxis.set_major_formatter(PercentFormatter(100, decimals=1))
    
    # Set axis labels
    ax.set_xlabel('Share of Total Revenue (%)', fontsize=10)
    
    # Adjust legend position and style
    ax.legend(loc='lower right', frameon=False, fontsize=9)
    
    # Remove unnecessary spines for cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    # Add subtle grid lines
    ax.grid(axis='x', linestyle='--', alpha=0.2)
    
    # Set background color to pure white
    ax.set_facecolor('#ffffff')
    fig.patch.set_facecolor('#ffffff')
    
    # Add a title to the chart
    # plt.title('Top Products by Revenue Share', fontsize=14, pad=20)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the figure
    output_path = os.path.join(output_dir, 'top_products_comparison.png')
    plt.savefig(output_path, dpi=120, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close()
    
    print(f"Product comparison chart saved to {output_path}")
    return output_path
# ------------------------------------------------------------
# -- PRODUCT: Analyse product data using CLaude    -----
# ------------------------------------------------------------

def analyze_top_products_with_claude(store_description, news_report, df_product):
    """
    Generate analysis for top products performance using Claude with timeline data for trend analysis.
    
    Parameters:
    store_description (str): Description of the store/business
    news_report (str): Recent market news relevant to analysis
    df_product (dict): Dictionary with results from analyze_top_products
    
    Returns:
    str: Claude's comprehensive analysis of the top products including timeline trends
    """
    # Extract key data for the analysis
    date_ranges = df_product['date_ranges']
    top_product_names = df_product['top_product_names']
    total_revenue = df_product['total_revenue']
    kpis = df_product['kpis']
    
    # Calculate total revenue percent change
    if total_revenue['earlier_week'] > 0:
        total_revenue_change = round(((total_revenue['previous_week'] - total_revenue['earlier_week']) / 
                                    total_revenue['earlier_week'] * 100), 1)
    else:
        total_revenue_change = float('inf')
    
    # Create a summary of the top products performance
    product_summaries = []
    for product_name in top_product_names:
        product_data = kpis[product_name]
        
        # Format the data for display
        current_revenue = product_data['previous_week_revenue_eur']
        previous_revenue = product_data['earlier_week_revenue_eur']
        change_pct = product_data['percent_change']
        current_share = product_data['revenue_eur_share_pct']
        previous_share = product_data['earlier_revenue_eur_share_pctt']
        
        # Create a summary string
        if change_pct == float('inf'):
            change_str = "new product"
        else:
            change_str = f"{'+' if change_pct > 0 else ''}{change_pct:.1f}%"
            
        product_summary = (
            f"{product_name}: €{current_revenue:,.0f} ({change_str}), "
            f"representing {current_share:.1f}% of total revenue (was {previous_share:.1f}% previously)"
        )
        product_summaries.append(product_summary)
    
    # Calculate overall stats for top products
    top_products_current_revenue = sum(kpis[product]['previous_week_revenue_eur'] for product in top_product_names)
    top_products_previous_revenue = sum(kpis[product]['earlier_week_revenue_eur'] for product in top_product_names)
    
    top_products_current_share = round(top_products_current_revenue / total_revenue['previous_week'] * 100, 1) if total_revenue['previous_week'] > 0 else 0
    top_products_previous_share = round(top_products_previous_revenue / total_revenue['earlier_week'] * 100, 1) if total_revenue['earlier_week'] > 0 else 0
    
    # Format the date ranges
    current_period = f"{date_ranges['analysis_period']['start']} to {date_ranges['analysis_period']['end']}"
    previous_period = f"{date_ranges['previous_period']['start']} to {date_ranges['previous_period']['end']}"
    
    # Create timeline summaries for each product (last 14 days for readability)
    timeline_summaries = []
    for product_name in top_product_names:
        product_data = kpis[product_name]
        
        # Get the daily share data (take just last 14 days to keep it manageable)
        daily_share = product_data['daily_share_pct'][-14:]
        
        # Format the timeline data for this product
        timeline_summary = f"{product_name} daily revenue share (%):\n"
        for date, share in daily_share:
            timeline_summary += f"  {date}: {share:.1f}%\n"
        
        timeline_summaries.append(timeline_summary)
    
    # Build the prompt for Claude
    prompt = f"""
    I need you to analyze the top products performance for our limited-edition t-shirt store and provide a comprehensive summary. You will receive store context, market news, product performance data, and timeline data for trend analysis.

    ## Store Context
    {store_description}

    ## Recent Market News
    {news_report}

    ## Analysis Period
    - Current period: {current_period}
    - Previous period: {previous_period}

    ## Top Products Data
    - Total Revenue: €{total_revenue['previous_week']:,.0f} (vs €{total_revenue['earlier_week']:,.0f}, {'+' if total_revenue_change > 0 else ''}{total_revenue_change}%)
    - Top {len(top_product_names)} products represent {top_products_current_share}% of total revenue (vs {top_products_previous_share}% in previous period)
    - These are our limited-edition designs available for only 24-48 hours

    Top Products:
    {chr(10).join(f"- {summary}" for summary in product_summaries)}

    ## Product Timeline Data (Last 14 Days)
    The following shows daily revenue share percentages for each product over the last 14 days:

    {chr(10).join(timeline_summaries)}

    ## Instructions
    Provide 1-2 paragraphs (total 6-8 sentences) that cover:
    1. Overall performance of top products and their contribution to revenue
    2. Notable standout products with specific numbers (growth %)
    3. Any patterns by product category/theme (gaming, movies, minimalist, etc.)
    4. Timeline trends (growing popularity, volatility, etc.)
    5. Suggestions for product strategy

    Key formatting requirements:
    - Use bullet points to highlight top performers
    - Bold key product names and significant percentage changes
    - Include specific revenue numbers for top products
    - Group insights by theme (gaming, movies, minimalist designs)
    - Directly relate market news to product performance when relevant
    - For bold use 1x* such as *bold*
    - For bullet points use "• "
    """
    
    try:
        # Initialize Anthropic client
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        
        # Send the prompt to Claude
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",  # Using Sonnet for this analysis
            max_tokens=800,
            temperature=0,  # Keep it deterministic
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract analysis text
        analysis = response.content[0].text
        print(f"Top products analysis completed successfully")
        
        return analysis.strip()
        
    except Exception as e:
        print(f"Error analyzing top products: {str(e)}")
        return f"Error: Could not analyze top products performance. {str(e)}"
    
# analysis= analyze_top_products_with_claude("tshirt store in the uk", "no news", df_product)

# ------------------------------------------------------------
# -- COVERAGE: Fetch and analyze GA4-Magento data from BigQuery -----
# ------------------------------------------------------------

def fetch_magento_ga4_data():
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
    
# ------------------------------------------------------------
# -- COVERAGE: clean df coverage with round values, only until last week -----
# ------------------------------------------------------------

def clean_coverage_data(df_coverage, run_date=None):
    """
    Clean the coverage data by:
    1. Filtering out data beyond the analysis period
    2. Rounding the coverage rates to 2 decimal places
    3. Removing any rows with NaN values in key metrics
    
    Parameters:
    df_coverage (pandas.DataFrame): Raw coverage data from BigQuery
    run_date (datetime, optional): The date the script is run. If None, uses current date.
    
    Returns:
    pandas.DataFrame: Cleaned coverage data
    """
    
    # Get the date periods using the get_analysis_periods function
    date_periods = get_analysis_periods(run_date)
    
    # Extract the end date of the analysis period
    analysis_end_date = datetime.strptime(date_periods['analysis_period']['end'], '%Y-%m-%d').date()
    
    # Ensure order_date is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df_coverage['order_date']):
        df_coverage['order_date'] = pd.to_datetime(df_coverage['order_date'])
    
    # Filter to keep only data up to the analysis period end date
    filtered_df = df_coverage[df_coverage['order_date'].dt.date <= analysis_end_date].copy()
    
    # Round coverage rates to 2 decimal places
    if 'transaction_coverage_rate' in filtered_df.columns:
        filtered_df['transaction_coverage_rate'] = filtered_df['transaction_coverage_rate'].round(1)
    
    if 'revenue_coverage_rate' in filtered_df.columns:
        filtered_df['revenue_coverage_rate'] = filtered_df['revenue_coverage_rate'].round(1)
    
    if 'magento_revenue' in filtered_df.columns:
        filtered_df['magento_revenue'] = filtered_df['magento_revenue'].round(1)

    if 'ga4_revenue' in filtered_df.columns:
        filtered_df['ga4_revenue'] = filtered_df['ga4_revenue'].round(1)
        
    # Remove rows with NaN values in critical columns
    critical_columns = [
        'magento_transactions', 'ga4_transactions', 'transaction_coverage_rate',
        'magento_revenue', 'ga4_revenue', 'revenue_coverage_rate'
    ]
    
    # Only check columns that actually exist in the dataframe
    columns_to_check = [col for col in critical_columns if col in filtered_df.columns]
    
    if columns_to_check:
        filtered_df = filtered_df.dropna(subset=columns_to_check)
    
    return filtered_df
# ------------------------------------------------------------
# -- COVERAGE: calculate weekly coverage -----
# ------------------------------------------------------------
def calculate_weekly_coverage(df, run_date=None):
    """
    Calculate weekly coverage metrics for GA4-Magento integration using the shared
    get_analysis_periods function to maintain consistency across all analysis functions.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with coverage data
                          Must contain: order_date, magento_transactions, ga4_transactions,
                          magento_revenue, ga4_revenue
    run_date (datetime, optional): The date the script is run.
                                  If None, uses current date.
    
    Returns:
    pandas.DataFrame: DataFrame with weekly coverage metrics
    """
    # Get the date periods using the get_analysis_periods function
    date_periods = get_analysis_periods(run_date)
    
    # Extract the date ranges for previous week and earlier week
    previous_monday = datetime.strptime(date_periods['analysis_period']['start'], '%Y-%m-%d').date()
    previous_sunday = datetime.strptime(date_periods['analysis_period']['end'], '%Y-%m-%d').date()
    earlier_monday = datetime.strptime(date_periods['previous_period']['start'], '%Y-%m-%d').date()
    earlier_sunday = datetime.strptime(date_periods['previous_period']['end'], '%Y-%m-%d').date()
    
    # Convert order_date to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df['order_date']):
        df['order_date'] = pd.to_datetime(df['order_date'])
    
    # Filter data for previous week
    previous_week_mask = (
        (df['order_date'] >= pd.Timestamp(previous_monday)) & 
        (df['order_date'] <= pd.Timestamp(previous_sunday))
    )
    previous_week_data = df[previous_week_mask]
    
    # Filter data for the week before that
    earlier_week_mask = (
        (df['order_date'] >= pd.Timestamp(earlier_monday)) & 
        (df['order_date'] <= pd.Timestamp(earlier_sunday))
    )
    earlier_week_data = df[earlier_week_mask]
    
    # Create aggregate results for both weeks
    weekly_results = []
    
    # Process previous week data
    if not previous_week_data.empty:
        # Aggregate the data
        previous_week_agg = {
            'week_label': f"{previous_monday.strftime('%b %d')} - {previous_sunday.strftime('%b %d')}",
            'week_start': previous_monday,
            'week_end': previous_sunday,
            'magento_transactions': previous_week_data['magento_transactions'].sum(),
            'ga4_transactions': previous_week_data['ga4_transactions'].sum(),
            'magento_revenue': previous_week_data['magento_revenue'].sum(),
            'ga4_revenue': previous_week_data['ga4_revenue'].sum(),
        }
        
        # Calculate coverage rates
        previous_week_agg['transaction_coverage_rate'] = (
            (previous_week_agg['ga4_transactions'] / previous_week_agg['magento_transactions']) * 100
            if previous_week_agg['magento_transactions'] > 0 else 0
        )
        previous_week_agg['revenue_coverage_rate'] = (
            (float(previous_week_agg['ga4_revenue']) / float(previous_week_agg['magento_revenue'])) * 100
            if previous_week_agg['magento_revenue'] > 0 else 0
        )
        
        weekly_results.append(previous_week_agg)
    else:
        print(f"Warning: No data found for the previous week ({previous_monday} to {previous_sunday})")
    
    # Process earlier week data
    if not earlier_week_data.empty:
        # Aggregate the data
        earlier_week_agg = {
            'week_label': f"{earlier_monday.strftime('%b %d')} - {earlier_sunday.strftime('%b %d')}",
            'week_start': earlier_monday,
            'week_end': earlier_sunday,
            'magento_transactions': earlier_week_data['magento_transactions'].sum(),
            'ga4_transactions': earlier_week_data['ga4_transactions'].sum(),
            'magento_revenue': earlier_week_data['magento_revenue'].sum(),
            'ga4_revenue': earlier_week_data['ga4_revenue'].sum(),
        }
        
        # Calculate coverage rates
        earlier_week_agg['transaction_coverage_rate'] = (
            (earlier_week_agg['ga4_transactions'] / earlier_week_agg['magento_transactions']) * 100
            if earlier_week_agg['magento_transactions'] > 0 else 0
        )
        earlier_week_agg['revenue_coverage_rate'] = (
            (float(earlier_week_agg['ga4_revenue']) / float(earlier_week_agg['magento_revenue'])) * 100
            if earlier_week_agg['magento_revenue'] > 0 else 0
        )
        
        weekly_results.append(earlier_week_agg)
    else:
        print(f"Warning: No data found for the earlier week ({earlier_monday} to {earlier_sunday})")
    
    # Convert to DataFrame
    df_weekly = pd.DataFrame(weekly_results)
    
    return df_weekly




# ------------------------------------------------------------
# -- COVERAGE: Generate the analysis prompt for Claude based on the tables   -----
# ------------------------------------------------------------
def analyze_weekly_coverage_with_claude(df_weekly, df_coverage=None, run_date=None):
    """
    Analyze weekly coverage rates between Magento and GA4 using Claude AI,
    with additional trend data from daily coverage for more context.
    
    Parameters:
    df_weekly (pandas.DataFrame): Weekly aggregated coverage data
    df_coverage (pandas.DataFrame, optional): Daily coverage data for trend analysis
    run_date (datetime, optional): The date the script is run. 
                                  If None, uses current date.
    
    Returns:
    str: Claude's analysis of the coverage data
    """
    # Check if we have at least two weeks of data for comparison
    if len(df_weekly) < 2:
        print("Warning: Need at least two weeks of data for proper analysis")
        return "Insufficient data for weekly comparison analysis"
    
    # Sort by week_start to ensure proper ordering (most recent first)
    df_weekly = df_weekly.sort_values('week_start', ascending=False).reset_index(drop=True)
    
    # Get the date periods using the get_analysis_periods function for context
    date_periods = get_analysis_periods(run_date)
    current_period = f"{date_periods['analysis_period']['start']} to {date_periods['analysis_period']['end']}"
    previous_period = f"{date_periods['previous_period']['start']} to {date_periods['previous_period']['end']}"
    
    # Prepare the tables in markdown format
    analysis_df = df_weekly.copy()
    
    # Format the coverage rates for display
    analysis_df['transaction_coverage_rate'] = analysis_df['transaction_coverage_rate'].round(1)
    analysis_df['revenue_coverage_rate'] = analysis_df['revenue_coverage_rate'].round(1)
    
    # Select columns for display
    tx_table = analysis_df[['week_label', 'magento_transactions', 'ga4_transactions', 'transaction_coverage_rate']]
    rev_table = analysis_df[['week_label', 'magento_revenue', 'ga4_revenue', 'revenue_coverage_rate']]
    
    # Convert both tables to markdown
    tx_table_md = tx_table.to_markdown(index=False)
    rev_table_md = rev_table.to_markdown(index=False)
    
    # Calculate week-over-week changes
    current_week = df_weekly.iloc[0]
    previous_week = df_weekly.iloc[1]
    
    tx_coverage_change = current_week['transaction_coverage_rate'] - previous_week['transaction_coverage_rate']
    rev_coverage_change = current_week['revenue_coverage_rate'] - previous_week['revenue_coverage_rate']
    
    # Add daily trend data if available
    trend_analysis = ""
    if df_coverage is not None and not df_coverage.empty:
        # Ensure order_date is in datetime format
        if not pd.api.types.is_datetime64_any_dtype(df_coverage['order_date']):
            df_coverage['order_date'] = pd.to_datetime(df_coverage['order_date'])
        
        # Sort by date (most recent first) and get the last 14 days for trend analysis
        df_trend = df_coverage.sort_values('order_date', ascending=False).head(14).copy()
        
        # Round coverage rates for display
        if 'transaction_coverage_rate' in df_trend.columns:
            df_trend['transaction_coverage_rate'] = df_trend['transaction_coverage_rate'].round(1)
        if 'revenue_coverage_rate' in df_trend.columns:
            df_trend['revenue_coverage_rate'] = df_trend['revenue_coverage_rate'].round(1)
        
        # Format the date for better readability
        df_trend['date'] = df_trend['order_date'].dt.strftime('%Y-%m-%d')
        
        # Select columns for the trend table
        trend_columns = ['date', 'transaction_coverage_rate', 'revenue_coverage_rate']
        trend_columns = [col for col in trend_columns if col in df_trend.columns]
        
        if trend_columns:
            trend_table = df_trend[trend_columns].sort_values('date', ascending=False)
            
            # Rename columns for clarity
            trend_table = trend_table.rename(columns={
                'date': 'Date',
                'transaction_coverage_rate': 'TX Coverage %',
                'revenue_coverage_rate': 'Revenue Coverage %'
            })
            
            # Convert to markdown
            trend_table_md = trend_table.to_markdown(index=False)
            
            # Add to the analysis
            trend_analysis = f"""
## Daily Coverage Trend (Last 14 Days)
This table shows the daily coverage rates to help identify patterns and drops.
{trend_table_md}
"""
    
    # Build the prompt
    prompt = f"""
    I need you to analyze our weekly GA4-Magento coverage rates and provide a standardized weekly report. The data shows a comparison between Magento (our source of truth) and GA4 tracking.

    ## Context
    - We're analyzing for the period: {current_period}
    - We're comparing the week of {current_week['week_label']} with the week of {previous_week['week_label']}
    - We consider coverage rates below 80% as concerning and below 50% as critical issues
    - Normal coverage rate for our business is typically between 80-95%

    ## Transactions Coverage Data (Weekly)
    This table shows the number of transactions recorded by Magento vs GA4, as well as the transaction coverage rate.
    {tx_table_md}

    ## Revenue Coverage Data (Weekly)
    This table shows the revenue generated according to Magento vs GA4, as well as the revenue coverage rate.
    {rev_table_md}
    {trend_analysis}

    ## Weekly Changes
    - Transaction Coverage Change: {tx_coverage_change:.1f}%
    - Revenue Coverage Change: {rev_coverage_change:.1f}%

    ## Output Format Requirements
    Your analysis must strictly follow this format:

    ```
    [EMOJI] *[only few words for the weekly highlight]*
    Weekly transaction coverage improved/declined compared to the previous week, with [insight about the numbers]. Revenue coverage is [assessment of revenue coverage].
    - *Transactions*: [magento_count] (Magento) vs [ga4_count] (GA4) - *Coverage*: [coverage]% ([direction] [change]%)
    - *Revenue*: €[magento_revenue]K (Magento) vs €[ga4_revenue]K (GA4) - *Coverage*: [coverage]% ([direction] [change]%)
    ```

    ## Status Indicators
    Select the appropriate emoji indicator based on these criteria:
    - **Critical (🚨)**: If there's a significant drop compared to previous data or if coverage is below 50%
    - **Warning (⚠️)**: If coverage is below 80% or if there's a slight drop over several weeks
    - **Good (✅)**: If coverage is stable and above 80%

    ## Direction Indicators
    For changes:
    - Use (↑) for increases in coverage
    - Use (↓) for decreases in coverage
    - Use (→) for no change in coverage (less than 5% change)

    ## Important Notes
    1. Use exactly the format shown above, including bullet points, bolding, and emoji placement
    2. Keep the second line to 1-2 concise sentences focusing on the most important insight
    3. Round percentages to 1 decimal place
    4. Format numbers with thousands separators (e.g., 1,503)
    5. Format revenue in thousands (K) with 1 decimal place
    6. Use € (Euro) symbol for currency, not $
    7. If the daily trend data shows any significant drops or patterns, briefly mention them in your analysis.
    8. For bold use 1x* such as *bold*
    9. For bullet points use "• "
        
    Your entire response should be exactly in this format without any additional text or explanations.
    """
    
    # Initialize Anthropic client
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
    print("Completed Claude weekly coverage analysis")
    
    return analysis

# ------------------------------------------------------------
# -- COVERAGE: Generate timeline visualization   -----
# ------------------------------------------------------------

def create_coverage_visualization(df_coverage, df_weekly=None, run_date=None, output_dir='images'):
    """
    Create visualization for transaction coverage showing a timeline chart on the left
    and a scorecard with the value and change on the right (like KPI visualizations).
    
    Parameters:
    df_coverage (pandas.DataFrame): DataFrame with daily coverage data
    df_weekly (pandas.DataFrame, optional): Weekly aggregated coverage data for scorecard
    run_date (datetime, optional): The date the script is run. If None, uses current date.
    output_dir (str): Directory to save the plot images
    
    Returns:
    str: Path to the generated plot file
    """

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Get the date periods using the get_analysis_periods function
    date_periods = get_analysis_periods(run_date)
    
    # Get the start and end date of the analysis period
    analysis_start_date = datetime.strptime(date_periods['analysis_period']['start'], '%Y-%m-%d')
    analysis_end_date = datetime.strptime(date_periods['analysis_period']['end'], '%Y-%m-%d')
    
    # Get the previous period dates for comparison
    previous_start_date = datetime.strptime(date_periods['previous_period']['start'], '%Y-%m-%d')
    previous_end_date = datetime.strptime(date_periods['previous_period']['end'], '%Y-%m-%d')
    
    # Format the date ranges for display
    current_date_range = f"{date_periods['analysis_period']['start']} to {date_periods['analysis_period']['end']}"
    previous_date_range = f"{date_periods['previous_period']['start']} to {date_periods['previous_period']['end']}"
    
    # Ensure order_date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df_coverage['order_date']):
        df_coverage['order_date'] = pd.to_datetime(df_coverage['order_date'])
    
    # Sort by date
    df_coverage = df_coverage.sort_values('order_date')
        
    # Create figure with 2-column grid - timeline on left, scorecard on right
    fig = plt.figure(figsize=(14, 5))
    gs = gridspec.GridSpec(1, 2, width_ratios=[2, 1])
    
    # Create timeline on the left column
    ax_timeline = plt.subplot(gs[0])
    create_transaction_timeline(
        ax_timeline, 
        df_coverage,
        'Transaction Coverage',
        analysis_start_date,
        analysis_end_date
    )
    
    # Create scorecard in right column if weekly data is available
    ax_card = plt.subplot(gs[1])
    
    if df_weekly is not None and len(df_weekly) >= 2:
        # Sort by week_start to ensure proper ordering (most recent first)
        df_weekly_sorted = df_weekly.sort_values('week_start', ascending=False).reset_index(drop=True)
        
        # Get the current and previous week values
        current_value = df_weekly_sorted.iloc[0]['transaction_coverage_rate']
        previous_value = df_weekly_sorted.iloc[1]['transaction_coverage_rate']
        
        # Calculate percent change
        if previous_value > 0:
            percent_change = ((current_value - previous_value) / previous_value) * 100
        else:
            percent_change = float('inf') if current_value > 0 else 0
        
        # Create scorecard
        create_transaction_scorecard(
            ax_card, 
            'Transaction Coverage', 
            current_date_range,
            current_value, 
            percent_change,
            previous_date_range  # Add previous date range for comparison
        )
    else:
        # If no weekly data, display a message
        ax_card.text(0.5, 0.5, "Weekly data not available", 
                    ha='center', va='center', fontsize=14, 
                    transform=ax_card.transAxes)
        ax_card.axis('off')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the plot
    output_path = os.path.join(output_dir, f"transaction_coverage_visualization.png")
    plt.savefig(output_path, dpi=120, bbox_inches='tight')
    plt.close()
    
    return output_path

def create_transaction_timeline(ax, df, label, analysis_start_date=None, analysis_end_date=None):
    """
    Create a timeline chart showing Magento vs GA4 transactions with light/dark blue coloring 
    for the analysis period.
    
    Parameters:
    ax: Matplotlib axis
    df: DataFrame with coverage data
    label: Chart label
    analysis_start_date: The start date of the analysis period
    analysis_end_date: The end date of the analysis period
    """

    # Filter data to only include dates up to the analysis end date
    if analysis_end_date:
        filtered_df = df[df['order_date'] <= analysis_end_date].copy()
    else:
        filtered_df = df.copy()
    
    # Sort by date
    filtered_df = filtered_df.sort_values('order_date')
    
    # Extract dates and values
    dates = filtered_df['order_date'].tolist()
    magento_values = filtered_df['magento_transactions'].tolist()
    ga4_values = filtered_df['ga4_transactions'].tolist()
    
    # Set colors
    magento_color = '#1f77b4'  # Dark blue for Magento
    ga4_color = '#9ecae1'      # Light blue for GA4
    
    # Plot Magento and GA4 transactions as lines
    ax.plot(dates, magento_values, '-', color=magento_color, linewidth=2, label='Magento')
    ax.plot(dates, ga4_values, '-', color=ga4_color, linewidth=2, label='GA4')
    
    # Add fill between the analysis period dates
    if dates and analysis_start_date and analysis_end_date:
        # Find indices for the analysis period
        analysis_indices = []
        for i, date in enumerate(dates):
            if date >= analysis_start_date and date <= analysis_end_date:
                analysis_indices.append(i)
        
        # If we have dates in the analysis period, fill them
        if analysis_indices:
            analysis_start_idx = min(analysis_indices)
            analysis_end_idx = max(analysis_indices)
            
            # Get the dates and values for the analysis period
            analysis_dates = dates[analysis_start_idx:analysis_end_idx+1]
            analysis_magento = magento_values[analysis_start_idx:analysis_end_idx+1]
            analysis_ga4 = ga4_values[analysis_start_idx:analysis_end_idx+1]
            
            # Fill between the Magento and GA4 lines in the analysis period with darker color
            ax.fill_between(analysis_dates, analysis_magento, analysis_ga4, 
                           color='#9ecae1', alpha=0.5)
            
            # Add vertical lines at the start and end of the analysis period
            ax.axvline(x=analysis_start_date, color='#999999', linestyle='--', alpha=0.5)
            ax.axvline(x=analysis_end_date, color='#999999', linestyle='--', alpha=0.5)
    
    # Format x-axis to show fewer dates for better visibility
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=14))  # Bi-weekly ticks instead of weekly
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)  # Horizontal labels
    
    # Format y-axis with thousands separator
    from matplotlib.ticker import FuncFormatter
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f'{x:,.0f}'))
    
    # Set grid lines
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    ax.grid(axis='x', visible=False)
    
    # Remove top and right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Set labels
    ax.set_ylabel('Transactions', fontsize=12)
    ax.set_xlabel('', fontsize=12)
    
    # Add legend
    ax.legend(loc='upper left', frameon=True)

def create_transaction_scorecard(ax, label, date_range, value, percent_change, comparison_date_range=None):
    """
    Create a transaction coverage scorecard with value and percent change on a given axis.
    Includes a date range subtitle under the title and comparison date range under the percentage change.
    
    Parameters:
    ax: Matplotlib axis
    label: Coverage metric label
    date_range: Date range string for subtitle
    value: Current coverage value
    percent_change: Percentage change from previous period
    comparison_date_range: Date range string for comparison period
    """
    # Clear the axis
    ax.clear()
    
    # Hide the axes
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    # Remove the ticks
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Set background color to white
    ax.set_facecolor('#ffffff')
    
    # Format the value
    value_str = f"{value:.1f}%"
    
    # Determine status color based on coverage value
    if value < 50:
        status_color = '#e74c3c'  # Red for critical
        status_text = "CRITICAL"
    elif value < 80:
        status_color = '#f39c12'  # Amber for warning
        status_text = "WARNING"
    else:
        status_color = '#2ecc71'  # Green for good
        status_text = "GOOD"
    
    # Format percent change and determine color
    if percent_change == float('inf'):
        pct_change_str = "NEW"
        change_color = '#3498db'  # Blue for new
    elif percent_change > 0:
        pct_change_str = f"+{percent_change:.1f}%"
        change_color = '#27ae60'  # Green for positive
    elif percent_change < 0:
        pct_change_str = f"{percent_change:.1f}%"
        change_color = '#e74c3c'  # Red for negative
    else:
        pct_change_str = "0%"
        change_color = '#95a5a6'  # Gray for no change
    
    # Add the title/label at the top
    ax.text(0.5, 0.95, label, fontsize=20, ha='center', va='center', fontweight='bold')
    
    # Add the date range subtitle
    ax.text(0.5, 0.85, date_range, fontsize=12, ha='center', va='center', fontstyle='italic')
    
    # Add the status indicator below the subtitle
    ax.text(0.5, 0.72, status_text, fontsize=14, ha='center', va='center', 
            color='white', fontweight='bold',
            bbox=dict(boxstyle="round,pad=0.3", facecolor=status_color, alpha=0.9))
    
    # Add the main value in the center
    ax.text(0.5, 0.5, value_str, fontsize=32, ha='center', va='center', fontweight='bold')
    
    # Add the percent change
    ax.text(0.5, 0.15, pct_change_str, fontsize=18, ha='center', va='center', 
            fontweight='bold', color='white',
            bbox=dict(boxstyle="round,pad=0.5", facecolor=change_color, alpha=0.9))
    
    # Add the comparison date range subtitle below the percent change
    if comparison_date_range:
        ax.text(0.5, 0.05, f"vs {comparison_date_range}", fontsize=12, ha='center', va='center', 
               fontstyle='italic', color='#555555')



# ---------------------------
# --- GENERATE ALL OUTPUTS --
# ---------------------------
periods = get_analysis_periods()
store_description = "Qwertee is an online t-shirt retailer specializing in limited-edition graphic tees featuring pop culture, gaming, and geek-inspired designs. The store operates on a time-sensitive model, offering new designs daily that are available for just 24-48 hours at discounted prices before potentially being retired forever. Qwertee sources its artwork from independent artists worldwide, who receive royalties for each shirt sold featuring their design."

# Intro
# emoji = random.choice(['🌏', '✏️', '📈', '📊', '🎯'])
emoji = '📋'

date_range = f"{periods['analysis_period']['start']} to {periods['analysis_period']['end']}"
previous_range = f"{periods['previous_period']['start']} to {periods['previous_period']['end']}"

intro = f"""
------------------------------------------------
{emoji} *WEEKLY REPORT* - *{date_range}* {emoji}

VS previous week {previous_range}
"""

# Create News report
raw_news = fetch_news_from_tavily(periods, "UK")
formatted_raw_news = format_news_for_ai(raw_news)
news_report = build_news_summary(formatted_raw_news, store_description)

# Create KPI analyses - analyze each KPI separately
df_magento = fetch_magento_kpi_data()
df_magento_eur = convert_currencies_and_group_vectorized(df_magento)

df_ga4 = fetch_ga4_kpi_data()
df_merged = merge_data_sources(df_magento_eur, df_ga4)
kpi_data = analyze_weekly_kpis(df_merged, run_date=None)

kpi_analyses = analyze_kpis_with_claude(store_description, news_report, kpi_data, value_to_analyze=None)

os.makedirs("images", exist_ok=True)
kpi_plot_paths = create_kpi_scorecard_with_timeline(kpi_data, output_dir='images')


# Generate product analysis
df_product = fetch_product_data()
product_data = analyze_top_products(df_product, run_date=None, top_n=5)
product_plot_path = create_product_comparison_chart(product_data, output_dir='images')
product_analysis = analyze_top_products_with_claude(store_description, news_report, product_data)

# Generate coverage analysis
df_coverage = fetch_magento_ga4_data()
df_coverage_clean = clean_coverage_data(df_coverage)
df_weekly = calculate_weekly_coverage(df_coverage_clean, run_date=None)
coverage_analysis = analyze_weekly_coverage_with_claude(df_weekly, df_coverage_clean, run_date=None)

coverage_plot_path = create_coverage_visualization(df_coverage_clean, df_weekly)






# ---------------------------
# --- GGDOC: Send to GGdoc --
# ---------------------------




def get_credentials():
    """Get service account credentials."""
    creds = None

    # If you have the service account credentials saved as a JSON file, use them
    if os.path.exists(BQ_PATH_KEY):  # Path to your service account JSON key
        creds = service_account.Credentials.from_service_account_file(
            BQ_PATH_KEY,  # Replace with your actual file path
            scopes=['https://www.googleapis.com/auth/documents', 'https://www.googleapis.com/auth/drive']
        )

    return creds

"""
# Define the scopes - need both docs and drive
SCOPES = ['https://www.googleapis.com/auth/documents', 'https://www.googleapis.com/auth/drive']

def get_credentials():
    '''Get and refresh OAuth 2.0 credentials.'''
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_info(
            json.load(open('token.json')), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            client_secrets_file = GCP_SERVICE_ACCOUNT_OAUTH
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file, SCOPES)
            creds = flow.run_local_server(port=8080)

        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds
"""
def execute_with_rate_limiting(request_fn, max_retries=5, initial_delay=1):
    """
    Execute a Google API request with rate limiting and retry logic.
    
    Parameters:
    request_fn: Function that makes the actual API request
    max_retries: Maximum number of retries (default 5)
    initial_delay: Initial delay in seconds (default 1)
    
    Returns:
    The response from the API or raises an exception if all retries fail
    """
    delay = initial_delay
    retries = 0
    
    while retries < max_retries:
        try:
            return request_fn()
        except Exception as e:
            if hasattr(e, 'resp') and hasattr(e.resp, 'status') and e.resp.status == 429:
                # Rate limit exceeded, exponential backoff
                print(f"Rate limit hit, waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                retries += 1
            elif "Quota exceeded" in str(e) or "RATE_LIMIT_EXCEEDED" in str(e):
                # Another form of rate limiting error
                print(f"Quota exceeded, waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                retries += 1
            else:
                # Different error, re-raise it
                raise
    
    # If we get here, we've exhausted our retries
    raise Exception(f"Failed after {max_retries} retries due to rate limiting")
def format_text_for_google_docs(text_content, docs_service, document_id, start_index):
    """
    Format text content with proper Google Docs formatting by:
    - Converting lines starting with "- " to bullet points
    - Converting text between asterisks (*) to bold
    - Preserving special characters like arrows (↓)
    - Supporting emoji characters
    
    Parameters:
    text_content (str): The raw text with markdown-style formatting
    docs_service: The Google Docs service client
    document_id (str): The ID of the document
    start_index (int): The starting index for insertion
    
    Returns:
    int: The new end_index after insertion
    """
    current_index = start_index
    lines = text_content.strip().split('\n')
    
    for line_num, line in enumerate(lines):
        # Check if this is a bullet point line
        is_bullet = line.strip().startswith('- ') or line.strip().startswith('• ')

        
        if is_bullet:
            # Remove the bullet prefix
            if line.strip().startswith('- '):
                line_text = line.strip()[2:]
            else:  # For "• " format
                line_text = line.strip()[2:]  # The • character and the space
        
        else:
            line_text = line
            
        # Process the line to identify bold sections
        bold_ranges = []
        processed_text = ""
        i = 0
        
        while i < len(line_text):
            if line_text[i] == '*':
                # Start of potential bold section
                start_pos = len(processed_text)
                i += 1  # Move past the *
                
                # Find the closing *
                bold_content = ""
                while i < len(line_text) and line_text[i] != '*':
                    bold_content += line_text[i]
                    i += 1
                
                # Add the content (without the asterisks)
                processed_text += bold_content
                
                if i < len(line_text) and line_text[i] == '*':
                    # Found closing *, mark this range as bold
                    end_pos = len(processed_text)
                    bold_ranges.append((start_pos, end_pos))
                    i += 1  # Move past the closing *
                else:
                    # No closing *, treat the opening * as regular text and put the content back
                    processed_text = processed_text  # Keep the content we added
            else:
                # Regular character
                processed_text += line_text[i]
                i += 1
        
        # Insert the processed text for this line
        insert_request = {
            'insertText': {
                'location': {'index': current_index},
                'text': processed_text + '\n'  # Add a newline
            }
        }
        
        try:
            docs_service.documents().batchUpdate(
                documentId=document_id,
                body={'requests': [insert_request]}
            ).execute()
            
            # Now handle the formatting (bullets and bold)
            format_requests = []
            
            # Add bullet formatting if needed
            if is_bullet:
                bullet_request = {
                    'createParagraphBullets': {
                        'range': {
                            'startIndex': current_index,
                            'endIndex': current_index + len(processed_text)
                        },
                        'bulletPreset': 'BULLET_DISC_CIRCLE_SQUARE'
                    }
                }
                format_requests.append(bullet_request)
            
            # Add bold formatting for all identified bold ranges
            for start_pos, end_pos in bold_ranges:
                bold_request = {
                    'updateTextStyle': {
                        'range': {
                            'startIndex': current_index + start_pos,
                            'endIndex': current_index + end_pos
                        },
                        'textStyle': {
                            'bold': True
                        },
                        'fields': 'bold'
                    }
                }
                format_requests.append(bold_request)
            
            # Apply all formatting in a single batch request
            if format_requests:
                docs_service.documents().batchUpdate(
                    documentId=document_id,
                    body={'requests': format_requests}
                ).execute()
            
            # Update current_index for the next line
            current_index += len(processed_text) + 1  # +1 for the newline
            
            # Add a tiny pause between operations to avoid rate limits
            if line_num < len(lines) - 1:  # Don't sleep after the last line
                time.sleep(2)
            
        except Exception as e:
            print(f"Error formatting line: {e}")
            # Try to continue with the next line
            current_index += len(processed_text) + 1
    
    return current_index

def create_weekly_google_doc_report(
    periods, 
    date_range, 
    previous_range, 
    news_report, 
    kpi_analyses, 
    kpi_plot_paths,
    product_analysis,
    product_plot_path,
    coverage_analysis, 
    coverage_plot_path
):
    """Create a Google Doc with the weekly report data and embedded images."""
    import time
    
    try:
        # Define sources for each section
        section_sources = {
            # KPIs
            'total_revenue_eur': 'Source: Magento',
            'transaction_count': 'Source: Magento',
            'sessions': 'Source: GA4',
            'ecr': 'Source: GA4',
            'aov_eur': 'Source: Magento',
            'units_per_order': 'Source: Magento',
            # Other sections
            'products': 'Source: GA4',
            'coverage': 'Source: Magento and GA4'
        }
        
        # Get credentials and initialize services
        creds = get_credentials()
        drive_service = build('drive', 'v3', credentials=creds)
        docs_service = build('docs', 'v1', credentials=creds)
        
        # Original document ID (template)
        original_doc_id = '1Ef39hFe5Ii9maDpvp1ETknlw1Hr-duhGOKhdk40nC5w'
        
        # New document title with date range
        new_title = f"Qwertee Weekly Report - {date_range}"
        
        # Step 1: Create a copy of the document
        print("Creating new document...")
        '''
        copied_file = drive_service.files().copy(
            fileId=original_doc_id,
            body={'name': new_title}
        ).execute()
        '''
        copied_file = drive_service.files().copy(
            fileId=original_doc_id,
            body={
                'name': new_title,  # Set the document's name
                'parents': [FOLDER_ID]  # Specify the parent folder where the document will be stored
            }
        ).execute()
                
        
        new_doc_id = copied_file['id']
        print(f"Created document with ID: {new_doc_id}")
        
        # Step 2: Upload images to Drive
        print("Uploading images to Drive...")
        image_ids = {}
        
        # Upload KPI images
        for kpi_name in kpi_plot_paths.keys():
            if os.path.exists(kpi_plot_paths[kpi_name]):
                try:
                    file_metadata = {'name': f"{kpi_name}_chart.png"}
                    media = MediaFileUpload(kpi_plot_paths[kpi_name], mimetype='image/png')
                    file = drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()
                    
                    # Make the file accessible to the document
                    drive_service.permissions().create(
                        fileId=file['id'],
                        body={'type': 'anyone', 'role': 'reader'},
                        fields='id'
                    ).execute()
                    
                    image_ids[kpi_name] = file['id']
                    print(f"Uploaded image for {kpi_name}")
                    time.sleep(2)  # Add a small delay to avoid hitting rate limits
                except Exception as e:
                    print(f"Error uploading {kpi_name} image: {e}")
        
        # Upload product image
        if os.path.exists(product_plot_path):
            try:
                file_metadata = {'name': "products_chart.png"}
                media = MediaFileUpload(product_plot_path, mimetype='image/png')
                file = drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                # Make the file accessible to the document
                drive_service.permissions().create(
                    fileId=file['id'],
                    body={'type': 'anyone', 'role': 'reader'},
                    fields='id'
                ).execute()
                
                image_ids['products'] = file['id']
                print("Uploaded products image")
                time.sleep(2)  # Add a small delay
            except Exception as e:
                print(f"Error uploading products image: {e}")
        
        # Upload coverage image
        if os.path.exists(coverage_plot_path):
            try:
                file_metadata = {'name': "coverage_chart.png"}
                media = MediaFileUpload(coverage_plot_path, mimetype='image/png')
                file = drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                # Make the file accessible to the document
                drive_service.permissions().create(
                    fileId=file['id'],
                    body={'type': 'anyone', 'role': 'reader'},
                    fields='id'
                ).execute()
                
                image_ids['coverage'] = file['id']
                print("Uploaded coverage image")
                time.sleep(2)  # Add a small delay
            except Exception as e:
                print(f"Error uploading coverage image: {e}")
        
        # Get the current document to find content end
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Step 3: Add title and date range
        print("Adding title section...")
        title_requests = [
            # Add main heading
            {
                'insertText': {
                    'location': {'index': end_index},
                    'text': f'Qwertee Weekly Report\n'
                }
            },
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': end_index,
                        'endIndex': end_index + len(f'Qwertee Weekly Report')
                    },
                    'paragraphStyle': {
                        'namedStyleType': 'HEADING_1'
                    },
                    'fields': 'namedStyleType'
                }
            },
            
            # Add date range as Header 3
            {
                'insertText': {
                    'location': {'index': end_index + len(f'Qwertee Weekly Report\n')},
                    'text': f'{date_range}\n'
                }
            },
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': end_index + len(f'Qwertee Weekly Report\n'),
                        'endIndex': end_index + len(f'Qwertee Weekly Report\n') + len(f'{date_range}')
                    },
                    'paragraphStyle': {
                        'namedStyleType': 'HEADING_3'
                    },
                    'fields': 'namedStyleType'
                }
            },
            
            # Add previous week range as Header 4
            {
                'insertText': {
                    'location': {'index': end_index + len(f'Qwertee Weekly Report\n') + len(f'{date_range}\n')},
                    'text': f'vs previous week {previous_range}\n'
                }
            },
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': end_index + len(f'Qwertee Weekly Report\n') + len(f'{date_range}\n'),
                        'endIndex': end_index + len(f'Qwertee Weekly Report\n') + len(f'{date_range}\n') + len(f'previous week {previous_range}')
                    },
                    'paragraphStyle': {
                        'namedStyleType': 'HEADING_4'
                    },
                    'fields': 'namedStyleType'
                }
            }
        ]
        
        # Execute the batch update for title (all at once)
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': title_requests}
        ).execute()
        
        # Add a pause before next section
        time.sleep(5)
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Step 4: Add Data Sources section
        print("Adding data sources section...")
        # Add data sources heading
        data_sources_heading = [
            {
                'insertText': {
                    'location': {'index': end_index},
                    'text': '💽 Data Sources\n'
                }
            }
        ]
        
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': data_sources_heading}
        ).execute()
        
        # Get updated end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Format the data sources text with bullet points
        data_sources_text = "- Magento 2\n- Google Analytics 4\n\n⚙️ Analysis Parameters\n- All monetary values are in EURO"
        end_index = format_text_for_google_docs(data_sources_text, docs_service, new_doc_id, end_index)
        
        # Add extra newlines
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': [{'insertText': {'location': {'index': end_index}, 'text': '\n\n'}}]}
        ).execute()
        
        # Pause after data sources section
        time.sleep(2)
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Step 5: Add Market Updates section
        print("Adding market updates section...")
        market_updates_heading = [
            # Add Market Updates heading
            {
                'insertText': {
                    'location': {'index': end_index},
                    'text': 'Market Updates\n'
                }
            },
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': end_index,
                        'endIndex': end_index + len('Market Updates')
                    },
                    'paragraphStyle': {
                        'namedStyleType': 'HEADING_2'
                    },
                    'fields': 'namedStyleType'
                }
            }
        ]
        
        # Execute the batch update for market updates heading
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': market_updates_heading}
        ).execute()
        
        # Get updated end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Add the news report with proper formatting
        end_index = format_text_for_google_docs(news_report, docs_service, new_doc_id, end_index)
        
        # Add extra newlines
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': [{'insertText': {'location': {'index': end_index}, 'text': '\n\n'}}]}
        ).execute()
        
        # Pause after market updates section
        time.sleep(2)
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Step 6: Add KPI sections with images in the requested order
        
        # Define the order of KPIs
        kpi_order = [
            'total_revenue_eur',   # Revenue
            'transaction_count',   # Transactions
            'sessions',            # Sessions
            'ecr',                 # eCommerce Conversion Rate
            'aov_eur',             # Average Order Value
            'units_per_order'      # Units per Order
        ]
        
        # KPI display names
        kpi_display_names = {
            'total_revenue_eur': 'Revenue (EUR)',
            'transaction_count': 'Transaction Count',
            'sessions': 'Sessions',
            'ecr': 'eCommerce Conversion Rate',
            'aov_eur': 'Average Order Value (EUR)',
            'units_per_order': 'Units per Order'
        }
        
        # Process each KPI in the specified order
        for kpi_name in kpi_order:
            print(f"Adding KPI section: {kpi_display_names[kpi_name]}...")
            # Create requests for KPI heading
            kpi_heading_requests = [
                # Add KPI heading
                {
                    'insertText': {
                        'location': {'index': end_index},
                        'text': f"KPI Analysis: {kpi_display_names[kpi_name]}\n"
                    }
                },
                {
                    'updateParagraphStyle': {
                        'range': {
                            'startIndex': end_index,
                            'endIndex': end_index + len(f"KPI Analysis: {kpi_display_names[kpi_name]}")
                        },
                        'paragraphStyle': {
                            'namedStyleType': 'HEADING_2'
                        },
                        'fields': 'namedStyleType'
                    }
                }
            ]
            
            # Execute the batch update for KPI heading
            docs_service.documents().batchUpdate(
                documentId=new_doc_id,
                body={'requests': kpi_heading_requests}
            ).execute()
            
            # Get updated document and end index
            document = docs_service.documents().get(documentId=new_doc_id).execute()
            end_index = document['body']['content'][-1]['endIndex'] - 1
            
            # Add KPI image if available
            if kpi_name in image_ids:
                try:
                    # Get page width from the document properties (or default to standard width)
                    page_width = 612  # Default letter size in points (8.5" x 72)
                    
                    try:
                        document_styles = docs_service.documents().get(
                            documentId=new_doc_id, 
                            fields='documentStyle'
                        ).execute()
                        
                        if 'documentStyle' in document_styles and 'pageSize' in document_styles['documentStyle']:
                            page_width = document_styles['documentStyle']['pageSize']['width']['magnitude']
                    except Exception as e:
                        print(f"Error getting page size: {e}")
                    
                    # Calculate image size to fit page width
                    image_width = min(500, page_width - 100)  
                    image_height = (image_width * 0.6)
                    
                    image_request = [
                        {
                            'insertInlineImage': {
                                'location': {'index': end_index},
                                'uri': f"https://drive.google.com/uc?export=view&id={image_ids[kpi_name]}",
                                'objectSize': {
                                    'height': {
                                        'magnitude': image_height,
                                        'unit': 'PT'
                                    },
                                    'width': {
                                        'magnitude': image_width,
                                        'unit': 'PT'
                                    }
                                }
                            }
                        },
                        # Add spacing after image
                        {
                            'insertText': {
                                'location': {'index': end_index + 1},  # +1 for the image
                                'text': '\n'
                            }
                        }
                    ]
                    
                    docs_service.documents().batchUpdate(
                        documentId=new_doc_id,
                        body={'requests': image_request}
                    ).execute()
                    
                    # Get updated document and end index after adding image
                    document = docs_service.documents().get(documentId=new_doc_id).execute()
                    end_index = document['body']['content'][-1]['endIndex'] - 1
                    
                    # Add appropriate source caption
                    source_text = section_sources.get(kpi_name, 'Source: Magento and GA4')
                    source_requests = [
                        {
                            'insertText': {
                                'location': {'index': end_index},
                                'text': f'{source_text}\n\n'
                            }
                        },
                        {
                            'updateParagraphStyle': {
                                'range': {
                                    'startIndex': end_index,
                                    'endIndex': end_index + len(source_text)
                                },
                                'paragraphStyle': {
                                    'namedStyleType': 'HEADING_5'
                                },
                                'fields': 'namedStyleType'
                            }
                        }
                    ]
                    
                    docs_service.documents().batchUpdate(
                        documentId=new_doc_id,
                        body={'requests': source_requests}
                    ).execute()
                    
                    # Add a pause after image insertion
                    time.sleep(2)
                except Exception as e:
                    print(f"Error inserting {kpi_name} image: {e}")
                    # Add a note that the image couldn't be added
                    error_note = [
                        {
                            'insertText': {
                                'location': {'index': end_index},
                                'text': f"[Image for {kpi_display_names[kpi_name]} could not be added automatically]\n\n"
                            }
                        }
                    ]
                    docs_service.documents().batchUpdate(
                        documentId=new_doc_id,
                        body={'requests': error_note}
                    ).execute()
            
            # Get updated document and end index
            document = docs_service.documents().get(documentId=new_doc_id).execute()
            end_index = document['body']['content'][-1]['endIndex'] - 1
            
            # Add the KPI analysis text
            end_index = format_text_for_google_docs(kpi_analyses[kpi_name], docs_service, new_doc_id, end_index)
            
            # Add extra newlines
            docs_service.documents().batchUpdate(
                documentId=new_doc_id,
                body={'requests': [{'insertText': {'location': {'index': end_index}, 'text': '\n\n'}}]}
            ).execute()
            
            # Pause after each KPI section
            time.sleep(2)
            
            # Get updated document and end index
            document = docs_service.documents().get(documentId=new_doc_id).execute()
            end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Step 7: Add Product Analysis section with image
        print("Adding product analysis section...")
        product_heading_requests = [
            # Add Products heading
            {
                'insertText': {
                    'location': {'index': end_index},
                    'text': 'Top Products Analysis\n'
                }
            },
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': end_index,
                        'endIndex': end_index + len('Top Products Analysis')
                    },
                    'paragraphStyle': {
                        'namedStyleType': 'HEADING_2'
                    },
                    'fields': 'namedStyleType'
                }
            }
        ]
        
        # Execute the batch update for products heading
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': product_heading_requests}
        ).execute()
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Add products image if available
        if 'products' in image_ids:
            try:
                # Calculate image size to fit page width
                page_width = 612  # Default letter size in points (8.5" x 72)
                image_width = min(500, page_width - 100)  
                image_height = (image_width * 0.6)  # Maintain aspect ratio
                
                product_image_request = [
                    {
                        'insertInlineImage': {
                            'location': {'index': end_index},
                            'uri': f"https://drive.google.com/uc?export=view&id={image_ids['products']}",
                            'objectSize': {
                                'height': {
                                    'magnitude': image_height,
                                    'unit': 'PT'
                                },
                                'width': {
                                    'magnitude': image_width,
                                    'unit': 'PT'
                                }
                            }
                        }
                    },
                    # Add spacing after image
                    {
                        'insertText': {
                            'location': {'index': end_index + 1},  # +1 for the image
                            'text': '\n'
                        }
                    }
                ]
                
                docs_service.documents().batchUpdate(
                    documentId=new_doc_id,
                    body={'requests': product_image_request}
                ).execute()
                
                # Get updated document and end index after adding image
                document = docs_service.documents().get(documentId=new_doc_id).execute()
                end_index = document['body']['content'][-1]['endIndex'] - 1
                
                # Add appropriate source caption
                source_text = section_sources.get('products', 'Source: Magento and GA4')
                source_requests = [
                    {
                        'insertText': {
                            'location': {'index': end_index},
                            'text': f'{source_text}\n\n'
                        }
                    },
                    {
                        'updateParagraphStyle': {
                            'range': {
                                'startIndex': end_index,
                                'endIndex': end_index + len(source_text)
                            },
                            'paragraphStyle': {
                                'namedStyleType': 'HEADING_5'
                            },
                            'fields': 'namedStyleType'
                        }
                    }
                ]
                
                docs_service.documents().batchUpdate(
                    documentId=new_doc_id,
                    body={'requests': source_requests}
                ).execute()
                
                # Pause after image insertion
                time.sleep(2)
                
            except Exception as e:
                print(f"Error inserting products image: {e}")
                # Add a note that the image couldn't be added
                error_note = [
                    {
                        'insertText': {
                            'location': {'index': end_index},
                            'text': "[Products chart could not be added automatically]\n\n"
                        }
                    }
                ]
                docs_service.documents().batchUpdate(
                    documentId=new_doc_id,
                    body={'requests': error_note}
                ).execute()
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Add the products analysis text
        end_index = format_text_for_google_docs(product_analysis, docs_service, new_doc_id, end_index)
        
        # Add extra newlines
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': [{'insertText': {'location': {'index': end_index}, 'text': '\n\n'}}]}
        ).execute()
        
        # Pause after products section
        time.sleep(2)
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Step 8: Add Coverage Analysis section with image
        print("Adding coverage analysis section...")
        coverage_heading_requests = [
            # Add Coverage heading
            {
                'insertText': {
                    'location': {'index': end_index},
                    'text': 'Coverage Analysis: Magento vs GA4\n'
                }
            },
            {
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': end_index,
                        'endIndex': end_index + len('Coverage Analysis: Magento vs GA4')
                    },
                    'paragraphStyle': {
                        'namedStyleType': 'HEADING_2'
                    },
                    'fields': 'namedStyleType'
                }
            }
        ]
        
        # Execute the batch update for coverage heading
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': coverage_heading_requests}
        ).execute()
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Add coverage image if available
        if 'coverage' in image_ids:
            try:
                # Calculate image size to fit page width
                page_width = 612  # Default letter size in points (8.5" x 72)
                image_width = min(500, page_width - 100)  
                image_height = (image_width * 0.6)  # Maintain aspect ratio
                
                coverage_image_request = [
                    {
                        'insertInlineImage': {
                            'location': {'index': end_index},
                            'uri': f"https://drive.google.com/uc?export=view&id={image_ids['coverage']}",
                            'objectSize': {
                                'height': {
                                    'magnitude': image_height,
                                    'unit': 'PT'
                                },
                                'width': {
                                    'magnitude': image_width,
                                    'unit': 'PT'
                                }
                            }
                        }
                    },
                    # Add spacing after image
                    {
                        'insertText': {
                            'location': {'index': end_index + 1},  # +1 for the image
                            'text': '\n'
                        }
                    }
                ]
                
                docs_service.documents().batchUpdate(
                    documentId=new_doc_id,
                    body={'requests': coverage_image_request}
                ).execute()
                
                # Get updated document and end index after adding image
                document = docs_service.documents().get(documentId=new_doc_id).execute()
                end_index = document['body']['content'][-1]['endIndex'] - 1
                
                # Add appropriate source caption
                source_text = section_sources.get('coverage', 'Source: Magento and GA4')
                source_requests = [
                    {
                        'insertText': {
                            'location': {'index': end_index},
                            'text': f'{source_text}\n\n'
                        }
                    },
                    {
                        'updateParagraphStyle': {
                            'range': {
                                'startIndex': end_index,
                                'endIndex': end_index + len(source_text)
                            },
                            'paragraphStyle': {
                                'namedStyleType': 'HEADING_5'
                            },
                            'fields': 'namedStyleType'
                        }
                    }
                ]
                
                docs_service.documents().batchUpdate(
                    documentId=new_doc_id,
                    body={'requests': source_requests}
                ).execute()
                
                # Pause after image insertion
                time.sleep(2)
                
            except Exception as e:
                print(f"Error inserting coverage image: {e}")
                # Add a note that the image couldn't be added
                error_note = [
                    {
                        'insertText': {
                            'location': {'index': end_index},
                            'text': "[Coverage chart could not be added automatically]\n\n"
                        }
                    }
                ]
                docs_service.documents().batchUpdate(
                    documentId=new_doc_id,
                    body={'requests': error_note}
                ).execute()
        
        # Get updated document and end index
        document = docs_service.documents().get(documentId=new_doc_id).execute()
        end_index = document['body']['content'][-1]['endIndex'] - 1
        
        # Add the coverage analysis text
        end_index = format_text_for_google_docs(coverage_analysis, docs_service, new_doc_id, end_index)

        # Add extra newline
        docs_service.documents().batchUpdate(
            documentId=new_doc_id,
            body={'requests': [{'insertText': {'location': {'index': end_index}, 'text': '\n'}}]}
        ).execute()
        
        print(f"Weekly report document created successfully with ID: {new_doc_id}")
        return new_doc_id
        
    except Exception as e:
        print(f"Error creating weekly report: {e}")
        if "RATE_LIMIT_EXCEEDED" in str(e) or "Quota exceeded" in str(e):
            print("Hit rate limit - you may need to wait before trying again or request a quota increase")
        return None
    
# Create the Google Doc report
doc_id = create_weekly_google_doc_report(
    periods=periods,
    date_range=date_range,
    previous_range=previous_range,
    news_report=news_report,
    kpi_analyses=kpi_analyses,
    kpi_plot_paths=kpi_plot_paths,

    product_analysis=product_analysis,
    product_plot_path=product_plot_path,
    coverage_analysis=coverage_analysis,
    coverage_plot_path=coverage_plot_path
)

# ---------------------------
# --- SLACK - SEND TO SLACK ---
# ---------------------------

def send_slack_report_with_image(text_content, plot_path=False, title=False):
    """Send a Slack message with text content and an image attachment"""
    slack_client = WebClient(token=SLACK_TOKEN)
    
    try:
        if title:
            title_msg = slack_client.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=title
                )
            
        if plot_path:
            # First upload the image file
            file_upload = slack_client.files_upload_v2(
                file=plot_path,
                # initial_comment=title,
                channels=[SLACK_CHANNEL_ID]
            )
            
            # Optionally delete the image file after sending
            # os.remove(plot_path)

            time.sleep(10)
        # Then send the analysis as a separate message
        if text_content:
            text_msg = slack_client.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=text_content
            )
        
        print("Report sent successfully")
        
        return True
    except SlackApiError as e:
        print(f"Error sending message with image: {e}")
        if hasattr(e, 'response') and 'error' in e.response:
            print(f"Error details: {e.response['error']}")
        return False

# Send Intro
send_slack_report_with_image(intro)
send_slack_report_with_image("                             ")

# Send News
send_slack_report_with_image(news_report, False, "🌏 *MARKET UPDATES*")
send_slack_report_with_image("                             ")

# Define the section order and display names for Slack in the requested order
slack_sections = [
    ('total_revenue_eur', '➡️ REVENUE (EUR)'),       
    ('transaction_count', '➡️ TRANSACTIONS'),   
    ('sessions', '➡️ SESSIONS'),                    
    ('ecr', '➡️ eCR'),   
    ('aov_eur', '➡️ AVERAGE VALUE per ORDER (EUR)'),    
    ('units_per_order', '➡️ AVERAGE ITEMS per ORDER')       
]

# Send each section with its visualization
for kpi_name, display_name in slack_sections:
    # Create a title with the KPI name
    kpi_title = f"*{display_name}*"
    
    # Get the analysis text
    kpi_text = kpi_analyses[kpi_name]
    
    # Get the plot path
    plot_path = kpi_plot_paths[kpi_name]
    
    # Send to Slack
    send_slack_report_with_image(kpi_text, plot_path, kpi_title)
    send_slack_report_with_image("                             ")

# Send top products analysis
send_slack_report_with_image(product_analysis, product_plot_path, "🛍️ *TOP PRODUCTS ANALYSIS*")
send_slack_report_with_image("                             ")

# Send coverage analysis
send_slack_report_with_image(coverage_analysis, coverage_plot_path, "📈 *COVERAGE ANALYSIS: MAGENTO vs GA4*")
send_slack_report_with_image("                             ")

# Send GGdoc link
send_slack_report_with_image(f"You want to save the report as PDF and send to the client? Check and upload as PDF here: https://docs.google.com/document/d/{doc_id}/ ")
send_slack_report_with_image("                             ")








