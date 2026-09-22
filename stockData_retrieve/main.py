import os
import time
import requests
import pandas as pd
from datetime import datetime

# --- 1. TWSE 日資料抓取與清洗 ---
def fetch_twse_all_data(target_date_str: str) -> pd.DataFrame:
    """
    抓取 TWSE 個股行情與三大法人買賣超，並進行欄位合併與轉換
    target_date_str 格式: YYYYMMDD
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/' 
    }
    
    url_price = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={target_date_str}&type=ALLBUT0999"
    url_chip = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={target_date_str}&selectType=ALLBUT0999"
    
    print(f"Requesting TWSE market data for {target_date_str}...")
    
    try:
        # 取得行情資料
        res_price = requests.get(url_price, headers=headers)
        data_price = res_price.json()
        
        if 'tables' not in data_price or not data_price['tables']:
            print(f"No price data found for {target_date_str}.")
            return pd.DataFrame()
            
        raw_price = data_price['tables'][8]
        df_price = pd.DataFrame(raw_price['data'], columns=raw_price['fields'])
        
        df_price = df_price[['證券代號', '證券名稱', '開盤價', '最高價', '最低價', '收盤價', '成交股數']]
        df_price.columns = ['ticker_raw', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        df_price['Ticker'] = df_price['ticker_raw'].astype(str).str.strip() + ".TW"
        
        num_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in num_cols:
            df_price[col] = df_price[col].astype(str).str.replace(',', '').str.replace('--', '0').str.strip()
            df_price[col] = pd.to_numeric(df_price[col], errors='coerce').fillna(0)
            
        time.sleep(3) # 避開 Rate Limit
        
        # 取得三大法人籌碼資料
        res_chip = requests.get(url_chip, headers=headers)
        data_chip = res_chip.json()
        
        df_chip = pd.DataFrame()
        if 'data' in data_chip:
            df_chip = pd.DataFrame(data_chip['data'], columns=data_chip['fields'])
            df_chip['Ticker'] = df_chip['證券代號'].astype(str).str.strip() + ".TW"
            
            for col in ['外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']:
                if col in df_chip.columns:
                    df_chip[col] = df_chip[col].astype(str).str.replace(',', '').str.strip()
                    df_chip[col] = pd.to_numeric(df_chip[col], errors='coerce').fillna(0)
                else:
                    df_chip[col] = 0
            
            df_chip = df_chip[['Ticker', '外陸資買賣超股數(不含外資自營商)', '投信買賣超股數', '自營商買賣超股數']]
            df_chip.columns = ['Ticker', 'ForeignNetBuy', 'TrustNetBuy', 'DealerNetBuy']

        if not df_chip.empty:
            merged_df = pd.merge(df_price, df_chip, on='Ticker', how='left').fillna(0)
        else:
            merged_df = df_price
            merged_df['ForeignNetBuy'] = 0
            merged_df['TrustNetBuy'] = 0
            merged_df['DealerNetBuy'] = 0

        # 計算 RetailVolume
        institutional_vol = merged_df['ForeignNetBuy'].abs() + merged_df['TrustNetBuy'].abs() + merged_df['DealerNetBuy'].abs()
        merged_df['RetailVolume'] = (merged_df['Volume'] - institutional_vol).clip(lower=0).astype(int)

        date_formatted = datetime.strptime(target_date_str, '%Y%m%d').strftime('%Y-%m-%d')
        merged_df['Date'] = date_formatted

        final_cols = ['Date', 'Ticker', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume', 
                      'ForeignNetBuy', 'TrustNetBuy', 'DealerNetBuy', 'RetailVolume']
        
        merged_df = merged_df[final_cols]
        merged_df = merged_df[merged_df['Ticker'].str.match(r'^\d{4}\.TW$')]
        
        return merged_df

    except Exception as e:
        print(f"Error processing TWSE data: {e}")
        return pd.DataFrame()

# --- 2. 儲存為單一 JSON 檔案 ---
def save_data_to_json(df: pd.DataFrame, file_prefix: str, date_suffix: str):
    """將 DataFrame 轉存為 JSON 檔案，存放在 stockData/json/ 下"""
    if df.empty:
        print("DataFrame is empty. Skipping save operation.")
        return

    json_folder = os.path.join("stockData", "json")
    os.makedirs(json_folder, exist_ok=True)
    
    try:
        json_file_path = os.path.abspath(os.path.join(json_folder, f"{file_prefix}_{date_suffix}.json"))
        # orient='records' 將每列轉為 JSON 物件格式，force_ascii=False 確保中文檔名與內容正常呈現
        df.to_json(json_file_path, orient='records', force_ascii=False, indent=4)
        print(f"JSON File successfully saved to: '{json_file_path}'")
    except Exception as e:
        print(f"Error saving JSON: {e}")

# --- TODO 區塊：擴充其他資料類型 ---

def fetch_monthly_data(year: int, month: int) -> pd.DataFrame:
    # TODO: 月資料抓取邏輯 (例如營收、月 K 棒等)
    print(f"[TODO] Fetching monthly data for {year}/{month}...")
    return pd.DataFrame()

def fetch_quarterly_data(year: int, quarter: int) -> pd.DataFrame:
    # TODO: 季資料抓取邏輯 (例如季報、財報分析等)
    print(f"[TODO] Fetching quarterly data for {year} Q{quarter}...")
    return pd.DataFrame()

def fetch_temp_data(param: str = None) -> pd.DataFrame:
    # TODO: 臨時資料/特定事件抓取邏輯
    print(f"[TODO] Fetching temporary data with param: {param}...")
    return pd.DataFrame()