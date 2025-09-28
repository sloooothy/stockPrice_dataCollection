import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
import time
import pytz 
import holidays # Library for accurate holiday checking

# --- Google Coding Style ---

# Define constants
TABLE_NAME = "twse_daily_price"
TWSE_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date}&type=ALLBUT0999"
# The folder where the script is running and files are stored
DATA_FOLDER = "stockData_retrieve" 
TZ_TAIPEI = pytz.timezone('Asia/Taipei')

# Define Taiwan holidays instance once for efficiency
# We check a range of years to cover past and future data checks
TW_HOLIDAYS = holidays.TW(years=range(datetime.now().year - 2, datetime.now().year + 2))

# --- Helper Functions ---

def is_trading_day(target_date: datetime) -> bool:
  """
  Checks if the given date is a trading day (not a weekend and not a market holiday).
  
  Args:
    target_date: The date to check.
    
  Returns:
    True if it is a trading day, False otherwise.
  """
  date_only = target_date.date()
  
  # 1. Check for weekend (Saturday=5, Sunday=6)
  if target_date.weekday() >= 5:
    return False
  
  # 2. Check for public holidays using the holidays library
  if date_only in TW_HOLIDAYS:
    return False
    
  return True

def find_last_trading_day(current_date: datetime) -> datetime:
  """
  Finds the most recent trading day using the holidays library.
  
  Args:
    current_date: The date to start the check from.
    
  Returns:
    The datetime object of the last trading day.
  """
  # Start checking backwards from the current date (inclusive)
  target_date = current_date
  
  # Note: The logic starts checking from the current date and finds the *previous* trading day 
  # if the current day is not a trading day.
  
  # Iterate backwards until a trading day is found
  while not is_trading_day(target_date):
    target_date -= timedelta(days=1)
    
  return target_date

def is_data_fetched(target_date: datetime) -> bool:
  """Checks if the data file for the target date already exists."""
  date_str = target_date.strftime('%Y%m%d')
  file_path = os.path.join(DATA_FOLDER, f"stock_data_{date_str}.db")
  return os.path.exists(file_path)

def fetch_twse_daily_summary(target_date: str) -> pd.DataFrame:
  """
  Fetches the complete daily trading summary for all listed stocks from TWSE.
  """
  url = TWSE_URL.format(date=target_date)
  print(f"Requesting TWSE market summary for {target_date}...")
  
  try:
    headers = {'User-Agent': 'Mozilla/5.0'} 
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    if 'data9' not in data or not data['data9']:
      print(f"Warning: TWSE returned no trading data.")
      return pd.DataFrame()
      
    columns = data['fields9']
    raw_data = data['data9']
    df = pd.DataFrame(raw_data, columns=columns)
    
    # --- Data Cleaning and Normalization ---
    numeric_cols_to_clean = [
      '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', 
      '漲跌價差', '成交筆數'
    ]
    
    for col in numeric_cols_to_clean:
      if col in df.columns:
        df[col] = df[col].astype(str).str.replace(',', '').str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df.insert(0, 'Trade_Date', datetime.strptime(target_date, '%Y%m%d').strftime('%Y-%m-%d'))
    df.rename(columns={'證券代號': 'Stock_ID', '收盤價': 'Close_Price', '成交股數': 'Volume'}, inplace=True)
    
    df['Volume'] = df['Volume'].fillna(0).astype(int)
    df = df[df['Stock_ID'].str.match(r'^\d{4}$')]
    
    print(f"Successfully fetched {len(df)} valid records.")
    return df
    
  except requests.RequestException as e:
    print(f"Error making request to TWSE: {e}")
    return pd.DataFrame()


def save_to_sqlite(df: pd.DataFrame, target_date: datetime):
  """
  Saves the DataFrame to the date-stamped SQLite file in the defined folder.
  """
  if df.empty:
    print("DataFrame is empty. Skipping save operation.")
    return

  date_str = target_date.strftime('%Y%m%d')
  # Critical modification: Specify the DATA_FOLDER for correct output path
  db_file_path = os.path.join(DATA_FOLDER, f"stock_data_{date_str}.db")
  
  # Ensure the directory exists (important for GitHub Actions)
  os.makedirs(DATA_FOLDER, exist_ok=True)
  
  engine = create_engine(f"sqlite:///{db_file_path}")
  df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
  
  print(f"--- Save Complete ---")
  print(f"New file saved: '{db_file_path}'.")

# --- MAIN ORCHESTRATION ---

def main():
  """
  Main function to execute the scheduled intelligent fetching process.
  """
  # Get current time in Taipei timezone
  now_tst = datetime.now(TZ_TAIPEI)
  today_tst = now_tst.replace(hour=0, minute=0, second=0, microsecond=0)
  
  # Data is usually available after 16:00 TST (4:00 PM)
  CUTOFF_TIME = now_tst.replace(hour=16, minute=0, second=0, microsecond=0)
  
  target_fetch_date = None
  
  # Determine the actual date we should be checking and fetching for
  if is_trading_day(today_tst) and now_tst >= CUTOFF_TIME:
    # Scenario A: Trading day and past 4 PM (data should be available for today)
    target_check_date = today_tst
  elif is_trading_day(today_tst) and now_tst < CUTOFF_TIME:
    # Scenario B: Trading day but before 4 PM (fetch yesterday's data if missing)
    target_check_date = find_last_trading_day(today_tst - timedelta(days=1))
  else:
    # Scenario C: Non-Trading Day (fetch last trading day's data)
    target_check_date = find_last_trading_day(today_tst)
    
  # --- Fetching Logic ---
  
  if is_data_fetched(target_check_date):
    print(f"Data for {target_check_date.strftime('%Y-%m-%d')} already exists. Skipping fetch.")
    final_output_date = target_check_date
  else:
    print(f"Data for {target_check_date.strftime('%Y-%m-%d')} not found. Initiating fetch.")
    
    # Check if the target date is in the future (Should not happen with current logic, but safety check)
    if target_check_date.date() > now_tst.date():
        print("Error: Target date is in the future. Aborting fetch.")
        final_output_date = find_last_trading_day(now_tst)
    else:
        market_df = fetch_twse_daily_summary(target_check_date.strftime('%Y%m%d'))
        if not market_df.empty:
            save_to_sqlite(market_df, target_check_date)
            
            # Data Sample for printf practice
            print("\nData Sample (for struct & printf practice):")
            sample = market_df.head(3)
            print("Stock_ID | Close Price | Volume (Int)")
            for _, row in sample.iterrows():
                print(f"{row['Stock_ID']:<8} | {row['Close_Price']:<11.2f} | {row.get('Volume', 0):>10.0f}")
                
        final_output_date = target_check_date

  
  # --- FINAL OUTPUT ---
  print(f"\n--- PROGRAM STATUS ---")
  print(f"The date for the most recently COMPLETED data is: {final_output_date.strftime('%Y-%m-%d')}")


if __name__ == "__main__":
  main()