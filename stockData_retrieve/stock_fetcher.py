import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
import time

# --- Google Coding Style ---

# Define the table name (used inside the date-stamped DB file)
TABLE_NAME = "twse_daily_price"
# TWSE Daily Trading Summary JSON API endpoint
TWSE_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date}&type=ALLBUT0999"

def get_target_date_str() -> str:
  """
  Generates a reliable past trading day in YYYYMMDD format for the TWSE API.
  Note: This part must be refined in a production environment to skip weekends/holidays.
  """
  # For demonstration reliability, we use a fixed past trading day (2024-09-27).
  # Please adjust the date to a recent trading day when running this.
  target_date = datetime(2024, 9, 27)
  return target_date.strftime('%Y%m%d')

def fetch_twse_daily_summary(target_date: str) -> pd.DataFrame:
  """
  Fetches the complete daily trading summary for all listed stocks from TWSE.
  
  Args:
    target_date: Date in YYYYMMDD format (e.g., '20240927').
    
  Returns:
    A DataFrame containing all available data, or an empty DataFrame if failed.
  """
  url = TWSE_URL.format(date=target_date)
  print(f"Requesting TWSE market summary for {target_date}...")
  
  try:
    # Set User-Agent to prevent server rejection
    headers = {'User-Agent': 'Mozilla/5.0'} 
    response = requests.get(url, headers=headers)
    response.raise_for_status() # Raise exception for bad status codes
    data = response.json()
    
    # Check for valid data key and content
    if 'data9' not in data or not data['data9']:
      print(f"Warning: TWSE returned no trading data (可能是假日或非交易日)。")
      return pd.DataFrame()
      
    # 'fields9' contains column names; 'data9' contains the actual records (all data fields)
    columns = data['fields9']
    raw_data = data['data9']
    
    # Create DataFrame with all available columns
    df = pd.DataFrame(raw_data, columns=columns)
    print(f"Successfully fetched {len(df)} records.")
    
    # --- Data Cleaning and Normalization for Struct Design ---
    
    # Identify numeric columns for cleaning (removing commas)
    numeric_cols_to_clean = [
      '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', 
      '漲跌價差', '成交筆數'
    ]
    
    for col in numeric_cols_to_clean:
      if col in df.columns:
        # Remove commas and non-numeric characters
        df[col] = df[col].astype(str).str.replace(',', '').str.strip()
        # Convert to numeric, setting invalid values (like '-') to NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add/Rename Key Columns for consistency and struct mapping
    # Trade_Date (String), Stock_ID (String)
    df.insert(0, 'Trade_Date', datetime.strptime(target_date, '%Y%m%d').strftime('%Y-%m-%d'))
    df.rename(columns={'證券代號': 'Stock_ID', '收盤價': 'Close_Price', '成交股數': 'Volume'}, inplace=True)
    
    # Volume (Integer), Close_Price (Float)
    df['Volume'] = df['Volume'].fillna(0).astype(int)
    
    # Final filter to keep only standard stock codes (e.g., 4-digit numbers)
    df = df[df['Stock_ID'].str.match(r'^\d{4}$')]
    
    return df
    
  except requests.RequestException as e:
    print(f"Error making request to TWSE: {e}")
    return pd.DataFrame()


def save_to_sqlite(df: pd.DataFrame, db_file: str, table_name: str):
  """
  Saves the DataFrame to the specific SQLite database file.
  
  Args:
    db_file: The date-stamped path to the SQLite database file.
    table_name: The name of the table to save the data into.
  """
  if df.empty:
    print("DataFrame is empty. Skipping save operation.")
    return

  # Create engine connection to the date-stamped file
  engine = create_engine(f"sqlite:///{db_file}")

  # Since the DB file is new every day, 'if_exists' can be 'replace' or 'fail'.
  # We use 'replace' to ensure a clean table structure creation.
  df.to_sql(table_name, engine, if_exists='replace', index=False)
  
  print(f"--- Save Complete ---")
  print(f"Successfully saved {len(df)} rows to NEW file: '{db_file}' table '{table_name}'.")

def main():
  """Main function to run the daily fetching process."""
  target_date_ymd = get_target_date_str()
  
  # --- Step 1: Dynamic DB File Naming ---
  # DATABASE_FILE will be like 'stock_data_20240927.db'
  DATABASE_FILE = f"stock_data_{target_date_ymd}.db"
  
  print(f"Database file for this run: {DATABASE_FILE}")
  
  # Fetch and retrieve all data
  market_df = fetch_twse_daily_summary(target_date_ymd)

  # Show data structure and type for future struct/printf practice
  if not market_df.empty:
    print("\n--- Data Sample (for struct & printf practice) ---")
    
    # 範例欄位：Stock_ID (String), Close_Price (Float), Volume (Integer)
    sample = market_df.head(3)
    print("Stock_ID | Close Price | Volume (Int)")
    for _, row in sample.iterrows():
        # Matches C printf format: %s, %.2f, %d/%.0f
        print(f"{row['Stock_ID']:<8} | {row['Close_Price']:<11.2f} | {row.get('Volume', 0):>10.0f}")

  # Save the data to the date-stamped SQLite file
  save_to_sqlite(market_df, DATABASE_FILE, TABLE_NAME)


if __name__ == "__main__":
  main()