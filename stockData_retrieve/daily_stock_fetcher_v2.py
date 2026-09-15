import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
import time
import pytz 
import holidays

# --- 設定與常數 ---
TABLE_NAME = "stock_kline"
DATA_FOLDER = "stockData" 
TZ_TAIPEI = pytz.timezone('Asia/Taipei')
TW_HOLIDAYS = holidays.TW(years=range(datetime.now().year - 2, datetime.now().year + 2))

# --- 輔助函數：日期檢查 ---
def is_trading_day(target_date: datetime) -> bool:
    if target_date.weekday() >= 5:
        return False
    if target_date.date() in TW_HOLIDAYS:
        return False
    return True

def find_last_trading_day(current_date: datetime) -> datetime:
    target_date = current_date
    while not is_trading_day(target_date):
        target_date -= timedelta(days=1)
    return target_date

# --- 資料抓取與清洗 ---
def fetch_twse_all_data(target_date_str: str) -> pd.DataFrame:
    """
    抓取 TWSE 個股行情與三大法人買賣超，並進行欄位合併與轉換
    target_date_str 格式: YYYYMMDD
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/' 
    }
    
    # 1. 抓取個股價格與成交量 (MI_INDEX)
    url_price = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={target_date_str}&type=ALLBUT0999"
    # 2. 抓取三大法人買賣超 (T86)
    url_chip = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={target_date_str}&selectType=ALLBUT0999"
    
    print(f"Requesting TWSE market data for {target_date_str}...")
    
    try:
        # 取得行情資料
        res_price = requests.get(url_price, headers=headers)
        data_price = res_price.json()
        
        if 'tables' not in data_price or not data_price['tables']:
            print(f"No price data found for {target_date_str}.")
            return pd.DataFrame()
            
        # 取得一般股票表格 (通常為 Index 8)
        raw_price = data_price['tables'][8]
        df_price = pd.DataFrame(raw_price['data'], columns=raw_price['fields'])
        
        # 沉澱並清洗價格欄位
        df_price = df_price[['證券代號', '證券名稱', '開盤價', '最高價', '最低價', '收盤價', '成交股數']]
        df_price.columns = ['ticker_raw', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        # 補上 TW 股市字尾 (例如 2330.TW)
        df_price['Ticker'] = df_price['ticker_raw'].astype(str).str.strip() + ".TW"
        
        # 數值轉型與清理
        num_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in num_cols:
            df_price[col] = df_price[col].astype(str).str.replace(',', '').str.replace('--', '0').str.strip()
            df_price[col] = pd.to_numeric(df_price[col], errors='coerce').fillna(0)
            
        # 避開 TWSE API rate limit
        time.sleep(3)
        
        # 取得籌碼資料 (三大法人)
        res_chip = requests.get(url_chip, headers=headers)
        data_chip = res_chip.json()
        
        df_chip = pd.DataFrame()
        if 'data' in data_chip:
            df_chip = pd.DataFrame(data_chip['data'], columns=data_chip['fields'])
            # 外資買賣超、投信買賣超、自營商買賣超欄位提取
            # TWSE T86 欄位：外陸資買賣超股數(不含外資自營商)、投信買賣超股數、自營商買賣超股數
            df_chip['Ticker'] = df_chip['證券代號'].astype(str).str.strip() + ".TW"
            
            # 清理買賣超張數/股數欄位
            for col in ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']:
                if col in df_chip.columns:
                    df_chip[col] = df_chip[col].astype(str).str.replace(',', '').str.strip()
                    df_chip[col] = pd.to_numeric(df_chip[col], errors='coerce').fillna(0)
                else:
                    df_chip[col] = 0
            
            df_chip = df_chip[['Ticker', '外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']]
            df_chip.columns = ['Ticker', 'ForeignNetBuy', 'TrustNetBuy', 'DealerNetBuy']

        # 合併價格與三大法人籌碼資料
        if not df_chip.empty:
            merged_df = pd.merge(df_price, df_chip, on='Ticker', how='left').fillna(0)
        else:
            merged_df = df_price
            merged_df['ForeignNetBuy'] = 0
            merged_df['TrustNetBuy'] = 0
            merged_df['DealerNetBuy'] = 0

        # 計算 RetailVolume (估算散戶成交量：總成交量 - 三大法人絕對買賣超總和，若小於0則取預設)
        institutional_vol = merged_df['ForeignNetBuy'].abs() + merged_df['TrustNetBuy'].abs() + merged_df['DealerNetBuy'].abs()
        merged_df['RetailVolume'] = (merged_df['Volume'] - institutional_vol).clip(lower=0).astype(int)

        # 加入 Date 欄位 (YYYY-MM-DD)
        date_formatted = datetime.strptime(target_date_str, '%Y%m%d').strftime('%Y-%m-%d')
        merged_df['Date'] = date_formatted

        # 整理成目標規格的欄位順序
        final_cols = ['Date', 'Ticker', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume', 
                      'ForeignNetBuy', 'TrustNetBuy', 'DealerNetBuy', 'RetailVolume']
        
        merged_df = merged_df[final_cols]
        # 只保留 4 位數股票代碼之標的
        merged_df = merged_df[merged_df['Ticker'].str.match(r'^\d{4}\.TW$')]
        
        return merged_df

    except Exception as e:
        print(f"Error processing TWSE data: {e}")
        return pd.DataFrame()

# --- 儲存雙檔案 (SQLite + CSV) ---
def save_data_files(df: pd.DataFrame, target_date: datetime):
    if df.empty:
        print("DataFrame is empty. Skipping save operation.")
        return

    date_str = target_date.strftime('%Y%m%d')
    
    # 定義個別資料夾路徑
    csv_folder = os.path.join("stockData", "csv")
    sql_folder = os.path.join("stockData", "sql")
    
    # 確保兩大資料夾均存在
    os.makedirs(csv_folder, exist_ok=True)
    os.makedirs(sql_folder, exist_ok=True)
    
    # 1. 儲存 CSV 檔案 (寫入 stockData/csv/ 資料夾)
    csv_file_path = os.path.abspath(os.path.join(csv_folder, f"stock_data_{date_str}.csv"))
    df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
    print(f"1. CSV File successfully saved to: '{csv_file_path}'")

    # 2. 儲存 SQL/SQLite DB 檔案 (寫入 stockData/sql/ 資料夾)
    db_file_path = os.path.abspath(os.path.join(sql_folder, f"stock_data_{date_str}.db"))
    engine = create_engine(f"sqlite:///{db_file_path}")
    
    db_df = df.rename(columns={
        'Date': 'date', 'Ticker': 'ticker', 'Open': 'open', 'High': 'high', 
        'Low': 'low', 'Close': 'close', 'Volume': 'volume', 
        'ForeignNetBuy': 'foreign_net_buy', 'TrustNetBuy': 'trust_net_buy', 
        'DealerNetBuy': 'dealer_net_buy', 'RetailVolume': 'retail_volume'
    }).drop(columns=['Name'], errors='ignore')
    
    db_df.to_sql("stock_kline", engine, if_exists='replace', index=False)
    print(f"2. DB File successfully saved to: '{db_file_path}'")
    
    
# --- 主執行邏輯 ---
def main():
    now_tst = datetime.now(TZ_TAIPEI)
    today_tst = now_tst.replace(hour=0, minute=0, second=0, microsecond=0)
    CUTOFF_TIME = now_tst.replace(hour=16, minute=0, second=0, microsecond=0)
    
    if is_trading_day(today_tst) and now_tst >= CUTOFF_TIME:
        target_check_date = today_tst
    elif is_trading_day(today_tst) and now_tst < CUTOFF_TIME:
        target_check_date = find_last_trading_day(today_tst - timedelta(days=1))
    else:
        target_check_date = find_last_trading_day(today_tst)
        
    date_str = target_check_date.strftime('%Y%m%d')
    
    print(f"Initiating fetch process for date: {target_check_date.strftime('%Y-%m-%d')}")
    market_df = fetch_twse_all_data(date_str)
    
    if not market_df.empty:
        save_data_files(market_df, target_check_date)
        print("\nProcess finished successfully.")
    else:
        print("\nProcess failed or no data fetched.")

if __name__ == "__main__":
    main()
