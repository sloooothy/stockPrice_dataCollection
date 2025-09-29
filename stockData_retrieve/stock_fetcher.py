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
# TWSE URL 取得所有股票的每日行情
TWSE_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date}&type=ALLBUT0999"
# The folder where the script is running and files are stored
DATA_FOLDER = "stockData" 
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
  """
  target_date = current_date
  
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
    # 強化 User-Agent 與 Referer，避免被伺服器過濾
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/' 
    } 
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # mother data
    if 'tables' in data and data['tables']:
        raw_data=data['tables'][8]
        fields=raw_data['fields'] #取得data欄位
        print(fields)
        stock_data=raw_data['data']

        # 3. 將清單資料和欄位名稱轉換成 DataFrame
        df = pd.DataFrame(stock_data, columns=fields)
        # --- 關鍵修正：Data Cleaning and Normalization ---
        # 這裡的步驟確保了 'Stock_ID', 'Close_Price', 'Volume' 這些欄位會被正確建立。
        
        # 找出需要清理逗號和轉型的數值欄位
        numeric_cols_to_clean = ['證券代號', '證券名稱', '成交股數', '成交筆數', 
                                 '成交金額', '開盤價', '最高價', '最低價', '收盤價', 
                                 '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', 
                                 '最後揭示賣價', '最後揭示賣量', '本益比']
        
        for col in numeric_cols_to_clean:
          if col in df.columns:
            # Remove commas, clean up non-numeric strings, and convert to numeric
            df[col] = df[col].astype(str).str.replace(',', '').str.replace('--', '0').str.replace('-', '0').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce') 

        # 插入交易日期
        date_formatted = datetime.strptime(target_date, '%Y%m%d').strftime('%Y-%m-%d')
        df.insert(0, 'Trade_Date', date_formatted)
        
        # 重新命名欄位 (這是讓 main 函數成功的關鍵)
        df.rename(columns={'證券代號': 'Stock_ID', '收盤價': 'Close_Price', '成交股數': 'Volume'}, inplace=True)
        
        # 處理成交量，確保是整數且空值為 0
        df['Volume'] = df['Volume'].fillna(0).astype(int)
        # 過濾，只保留四位數字的股票代號
        df = df[df['Stock_ID'].astype(str).str.match(r'^\d{4}$')]
        
        print(f"Successfully processed and cleaned {len(df)} valid records.")
        return df

    else:
       return pd.DataFrame()
    

  except requests.RequestException as e:
    print(f"Error making request to TWSE: {e}")
    return pd.DataFrame()


def save_to_sqlite(df: pd.DataFrame, target_date: datetime):
  """
  Saves the DataFrame to the date-stamped SQLite file in the defined folder, 
  using an absolute path for reliability in GitHub Actions.
  """
  if df.empty:
    print("DataFrame is empty. Skipping save operation.")
    return

  date_str = target_date.strftime('%Y%m%d')
  
  # 確保資料夾存在
  os.makedirs(DATA_FOLDER, exist_ok=True)
  
  # 使用絕對路徑，確保 SQLite 知道在哪裡創建檔案
  db_file_path_rel = os.path.join(DATA_FOLDER, f"stock_data_{date_str}.db")
  db_file_path_abs = os.path.abspath(db_file_path_rel)
  
  print(f"DEBUG: Calculated ABSOLUTE Path: {db_file_path_abs}") 
  
  # 使用絕對路徑連接資料庫
  engine = create_engine(f"sqlite:///{db_file_path_abs}")
  df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
  
  print(f"--- Save Complete ---")
  print(f"New file saved successfully at: '{db_file_path_abs}'.")
  
  # 額外加入 CSV 儲存 (根據你的要求)
  #csv_file_path = os.path.join(DATA_FOLDER, f"stock_data_{date_str}.csv")
  #df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
  #print(f"CSV file saved successfully at: '{csv_file_path}'.")

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
  
  # 偵錯：打印環境時間
  print(f"\n--- DEBUG: Environment Time Check ---")
  print(f"Action Runner Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
  print(f"Taipei Time (TST): {now_tst.strftime('%Y-%m-%d %H:%M:%S')}")
  print(f"Is past 4 PM TST: {now_tst >= CUTOFF_TIME}")
  print(f"Is today a trading day: {is_trading_day(today_tst)}")

  target_fetch_date = None
  
  # 決定要檢查和擷取的目標日期
  if is_trading_day(today_tst) and now_tst >= CUTOFF_TIME:
    target_check_date = today_tst # 情境 A: 交易日且已過收盤後
  elif is_trading_day(today_tst) and now_tst < CUTOFF_TIME:
    target_check_date = find_last_trading_day(today_tst - timedelta(days=1)) # 情境 B: 交易日但未收盤
  else:
    target_check_date = find_last_trading_day(today_tst) # 情境 C: 非交易日
    
  # --- Fetching Logic ---
  
  if is_data_fetched(target_check_date):
    print(f"Data for {target_check_date.strftime('%Y-%m-%d')} already exists. Skipping fetch.")
    final_output_date = target_check_date
  else:
    print(f"Data for {target_check_date.strftime('%Y-%m-%d')} not found. Initiating fetch.")
    
    if target_check_date.date() > now_tst.date():
        print("Error: Target date is in the future. Aborting fetch.")
        final_output_date = find_last_trading_day(now_tst)
    else:
        # target_check_date 已經是 datetime 物件，轉成 YYYYMMDD 字串給 API
        market_df = fetch_twse_daily_summary(target_check_date.strftime('%Y%m%d')) 
        if not market_df.empty:
            save_to_sqlite(market_df, target_check_date)
            
            # Data Sample for printf practice (包含不同的型別：字串、浮點數、整數)
            print("\nData Sample (for struct & printf practice):")
            # 確保 'Close_Price' 和 'Volume' 存在且是正確的數字型別
            if all(col in market_df.columns for col in ['Stock_ID', 'Close_Price', 'Volume']):
                sample = market_df.head(3)
                print("Stock_ID | Close Price | Volume (Int)")
                # 使用你要求的 printf 格式字串練習
                for _, row in sample.iterrows():
                    # %-8s (左對齊字串), %-11.2f (左對齊浮點數保留兩位), %10.0f (右對齊整數)
                    # Note: We use .format() or f-string for Python, which maps to C's printf style.
                    print(f"{row['Stock_ID']:<8} | {row['Close_Price']:<11.2f} | {row.get('Volume', 0):>10.0f}")
            else:
                print("Error: Required columns ('Stock_ID', 'Close_Price', 'Volume') not found in DataFrame.")
                
        final_output_date = target_check_date

  
  # --- FINAL OUTPUT ---
  print(f"\n--- PROGRAM STATUS ---")
  print(f"The date for the most recently COMPLETED data is: {final_output_date.strftime('%Y-%m-%d')}")


if __name__ == "__main__":
  main()