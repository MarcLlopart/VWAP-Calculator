import clickhouse_connect
from typing import Dict, List, Any, Tuple, Optional
import os 
from dotenv import load_dotenv 
import yfinance as yf
from datetime import datetime
import pandas as pd
import gspread

load_dotenv ()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH")

def get_client():
    client = clickhouse_connect.get_client(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            secure=False
        )
    return client

def run_query(query: str):
    client = get_client()
    result = client.query(query)
    return result.result_rows, result.column_names

def download_financial_data(ticker: str):

    print(f"Downloading {ticker} data...")
    df = yf.download(ticker, start='2025-01-01', end=datetime.now(), progress=False, auto_adjust=True)

    # Handle multi-level columns from yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Create a DataFrame with just the close prices
    price_df = pd.DataFrame({
        'date': df.index,
        'open': df['Open'].values,
        'high': df['High'].values,
        'low': df['Low'].values,
        'close': df['Close'].values,
        'volume': df['Volume'].values
    })
    return price_df

def calculate_vwap(df: pd.DataFrame):
    # Calculate VWAP
    df['date'] = pd.to_datetime(df['date'])
    df['quarter'] = df['date'].dt.to_period('Q')
    df['avg_price'] = (df['high'] + df['low'] + df['close'] + df['open']) / 4

    df['pv'] = df['avg_price'] * df['volume']
    df['cumulative_pv'] = df.groupby('quarter')['pv'].cumsum()
    df['cumulative_volume'] = df.groupby('quarter')['volume'].cumsum()

    # Calculate VWAP
    df['vwap'] = df['cumulative_pv'] / df['cumulative_volume']

    # Filter for quarter ends
    df_quarter_ends = df[pd.to_datetime(df['date']).dt.is_quarter_end]

    # Convert date to string format for Google Sheets
    df_quarter_ends = df_quarter_ends.copy()
    df_quarter_ends['date'] = df_quarter_ends['date'].dt.strftime('%Y-%m-%d')
    df_quarter_ends['quarter'] = df_quarter_ends['quarter'].astype(str)
    
    return df_quarter_ends

def upload_to_sheets(df: pd.DataFrame, sheet_name: str):
    # Authenticate with Google Sheets
    sa = gspread.service_account(filename=CREDENTIALS_PATH)
    sh = sa.open('AF Staking Rewards')
    try:
        worksheet = sh.worksheet(sheet_name)
        print(f"Using existing '{sheet_name}' sheet")
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=sheet_name, rows=100, cols=20)
        print(f"Created new '{sheet_name}' sheet")

    # Clear existing data
    worksheet.clear()

    # Write data to the sheet
    worksheet.update(range_name='A1', values=[df.columns.values.tolist()] + df.values.tolist())
    print(f"Uploaded data to '{sheet_name}' sheet")
