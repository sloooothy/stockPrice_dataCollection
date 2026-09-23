import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import io

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
		
		df_price['Ticker'] = df_price['ticker_raw'].astype(str).str.strip()+".TW"
		
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



# -----------------------------------------------------------------------------
# 1. 日資料抓取與清洗 (TWSE 行情 + 三大法人) -> 對應 dailyData
# -----------------------------------------------------------------------------
def fetch_daily_data(target_date_str: str) -> pd.DataFrame:
    """
    抓取指定日期的 TWSE 每日行情與三大法人籌碼
    target_date_str 格式: YYYYMMDD (例如 '20260320')
    
    [成功擷取欄位]:
      - sid, priceDate, OpenPrice, ClosePrice, highPrice, lowPrice
      - ttlVolume, ForeignNetBuy, TrustNetBuy, DealerNetBuy, RetailVolume (估算值)
      
    [未擷取 / 需額外擴充欄位]:
      - 精準 RetailVolume / 融資券異動: 需額外呼叫 TWSE 'MI_MARGIN' (融資融券餘額) API
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/'
    }
    
    url_price = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={target_date_str}&type=ALLBUT0999"
    url_chip = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={target_date_str}&selectType=ALLBUT0999"
    
    try:
        res_p = requests.get(url_price, headers=headers)
        data_p = res_p.json()
        
        if 'tables' not in data_p or not data_p['tables']:
            return pd.DataFrame()
            
        raw_p = data_p['tables'][8]
        df_p = pd.DataFrame(raw_p['data'], columns=raw_p['fields'])
        df_p = df_p[['證券代號', '開盤價', '收盤價', '最高價', '最低價', '成交股數']]
        df_p.columns = ['sid', 'OpenPrice', 'ClosePrice', 'highPrice', 'lowPrice', 'ttlVolume']
        df_p['sid'] = df_p['sid'].astype(str).str.strip()
        
        for col in ['OpenPrice', 'ClosePrice', 'highPrice', 'lowPrice', 'ttlVolume']:
            df_p[col] = df_p[col].astype(str).str.replace(',', '').str.replace('--', '0').str.strip()
            df_p[col] = pd.to_numeric(df_p[col], errors='coerce').fillna(0)
            
        time.sleep(3) # 避開 API 限流
        
        res_c = requests.get(url_chip, headers=headers)
        data_c = res_c.json()
        
        df_c = pd.DataFrame()
        if 'data' in data_c:
            df_c = pd.DataFrame(data_c['data'], columns=data_c['fields'])
            df_c['sid'] = df_c['證券代號'].astype(str).str.strip()
            
            mapping = {
                '外陸資買賣超股數(不含外資自營商)': 'ForeignNetBuy',
                '投信買賣超股數': 'TrustNetBuy',
                '自營商買賣超股數': 'DealerNetBuy'
            }
            for raw_col, target_col in mapping.items():
                if raw_col in df_c.columns:
                    df_c[target_col] = df_c[raw_col].astype(str).str.replace(',', '').str.strip()
                    df_c[target_col] = pd.to_numeric(df_c[target_col], errors='coerce').fillna(0)
                else:
                    df_c[target_col] = 0
            df_c = df_c[['sid', 'ForeignNetBuy', 'TrustNetBuy', 'DealerNetBuy']]
            
        if not df_c.empty:
            df = pd.merge(df_p, df_c, on='sid', how='left').fillna(0)
        else:
            df = df_p
            df['ForeignNetBuy'], df['TrustNetBuy'], df['DealerNetBuy'] = 0, 0, 0
            
        # 計算散戶預估量 (成交量 - 三大法人買賣超絕對值總和)
        inst_vol = df['ForeignNetBuy'].abs() + df['TrustNetBuy'].abs() + df['DealerNetBuy'].abs()
        df['RetailVolume'] = (df['ttlVolume'] - inst_vol).clip(lower=0).astype(int)
        
        df['priceDate'] = datetime.strptime(target_date_str, '%Y%m%d').strftime('%Y-%m-%d')
        df = df[df['sid'].str.match(r'^\d{4}$')] # 只保留 4 碼一般股票
        
        cols = ['sid', 'priceDate', 'OpenPrice', 'ClosePrice', 'highPrice', 'lowPrice', 
                'ttlVolume', 'ForeignNetBuy', 'TrustNetBuy', 'DealerNetBuy', 'RetailVolume']
        return df[cols]
        
    except Exception as e:
        print(f"Error fetching daily data for {target_date_str}: {e}")
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# 2. 月度資料抓取 (公開資訊觀測站 MOPS 營收) -> 對應 monthlyData 營收部分
# -----------------------------------------------------------------------------
def fetch_monthly_revenue(year: int, month: int) -> pd.DataFrame:
    """
    抓取指定年份與月份的全上市公司營收 (MOPS)
    year: 西元年 (如 2025)
    month: 月份 (1-12)
    
    [成功擷取欄位]:
      - sid, year, month
      - revenue, revenueMoM, revenueYoY, accRevenue, accRevenueYoY
      
    [未擷取 / 需額外擴充欄位]:
      - 董監內部人籌碼 (MOPS): insiderHoldingRatio, insiderPledgeRatio
        (需另寫爬蟲爬取 MOPS 董監持股明細「stapap1_s1」頁面)
      - 股權分散表 (TDCC): totalShareholders, largeShareholderRatio, retailShareholderRatio
        (需另寫爬蟲對接 TDCC 臺灣集中保管結算所 API / 網頁)
    """
    roc_year = year - 1911
    url = f"https://mopsov.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{month}_0.html"
    #print(f"fetch data from {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        res = requests.get(url, headers=headers)
        res.encoding = 'big5'
        # ✅ 使用 io.StringIO 將字串轉為類檔案物件 (File-like object)
        dfs = pd.read_html(io.StringIO(res.text))
        revenue_list = []
        #print(f"共抓到 {len(dfs)} 個表格")
        for df in dfs:
            # 1. 檢查表格欄位數
            if df.shape[1] < 11:
                continue

            # 2. 先處理多層欄位：若為 MultiIndex，取最後一層並去除所有空白
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(-1)

            # 將欄位名稱統一轉成字串並去除空白（防止網頁上的「公司 代號」有空格）
            df.columns = df.columns.astype(str).str.replace(r"\s+", "", regex=True)

            # 3. 修正過濾條件：直接對『欄位標題』進行檢查
            if "公司代號" in df.columns or "公司名稱" in df.columns:

                # 4. 確保欄位中有「公司代號」且進行 4 位數股票代碼篩選
                if "公司代號" in df.columns:
                    # 去除公司代號欄位中的空白
                    df["公司代號"] = (
                        df["公司代號"].astype(str).str.strip()
                    )

                    # 篩選出 4 位數的股票代號（排除掉小計、合計等文字列）
                    valid_rows = df[
                        df["公司代號"].str.match(r"^\d{4}$", na=False)
                    ]

                    if not valid_rows.empty:
                        revenue_list.append(valid_rows)
                    
        if not revenue_list:
            return pd.DataFrame()
            
        full_df = pd.concat(revenue_list, ignore_index=True)
        #print(full_df.head())
        
        result = pd.DataFrame()
        result['sid'] = full_df['公司代號'].astype(str).str.strip()
        result['year'] = year
        result['month'] = month
        
        def clean_num(val):
            # 確保傳入的是 Series，並將內部字串的逗號與空白清除後轉為數字
            return pd.to_numeric(
                val.astype(str).str.replace(",", "").str.strip(), errors="coerce"
            )
            
        result['revenue'] = (clean_num(full_df['當月營收']) * 1000).fillna(0).astype('int64')
        result['revenueMoM'] = clean_num(full_df['上月比較增減(%)']).fillna(0)
        result['revenueYoY'] = clean_num(full_df['去年同月增減(%)']).fillna(0)
        result['accRevenue'] = (clean_num(full_df['當月累計營收']) * 1000).fillna(0).astype('int64')
        result['accRevenueYoY'] = clean_num(full_df['前期比較增減(%)']).fillna(0)
        
        return result
        
    except Exception as e:
        print(f"Error fetching monthly revenue for {year}/{month}: {e}")
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# 3. 季度財報資料抓取 (公開資訊觀測站 MOPS 綜合損益表) -> 對應 quarterData 部分指標
# -----------------------------------------------------------------------------
def fetch_quarterly_financials(year: int, quarter: int) -> pd.DataFrame:
    """
    抓取指定季度全上市公司綜合損益表 (MOPS)
    year: 西元年 (如 2025)
    quarter: 季度 (1-4)
    
    [成功擷取欄位]:
      - sid, year, quarter, eps, operatingMargin (估算值)
      
    [未擷取 / 需額外擴充欄位]:
      - 獲利能力指標: epsYoY, grossMargin (毛利率), netMargin (純益率), roe, roa
      - 財務結構與安全度 (需另寫爬蟲爬取 MOPS 資產負債表):
        bookValuePerShare, debtRatio, currentRatio, quickRatio, inventoryTurnoverDays
      - 現金流量品質 (需另寫爬蟲爬取 MOPS 現金流量表):
        operatingCashFlow, freeCashFlow, cashFlowRatio
    """
    roc_year = year - 1911
    url = f"https://mops.twse.com.tw/mops/web/ajax_t163sb04"
    
    payload = {
        'encodeRequestParam': '1',
        'step': '1',
        'firstin': '1',
        'off': '1',
        'queryType': 'ii',
        'TYPEK': 'sii',
        'year': str(roc_year),
        'season': f"0{quarter}"
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        res = requests.post(url, data=payload, headers=headers)
        dfs = pd.read_html(res.text)
        
        fin_list = []
        for df in dfs:
            if '公司代號' in df.columns:
                fin_list.append(df)
                
        if not fin_list:
            return pd.DataFrame()
            
        full_df = pd.concat(fin_list, ignore_index=True)
        
        result = pd.DataFrame()
        result['sid'] = full_df['公司代號'].astype(str).str.strip()
        result['year'] = year
        result['quarter'] = quarter
        
        def clean_num(series):
            return pd.to_numeric(series.astype(str).str.replace(',', '').str.strip(), errors='coerce').fillna(0)

        if '基本每股盈餘（元）' in full_df.columns:
            result['eps'] = clean_num(full_df['基本每股盈餘（元）'])
        elif '基本每股盈餘' in full_df.columns:
            result['eps'] = clean_num(full_df['基本每股盈餘'])
        else:
            result['eps'] = 0.0

        if '營業利益' in full_df.columns and '營業收入' in full_df.columns:
            rev = clean_num(full_df['營業收入'])
            op_inc = clean_num(full_df['營業利益'])
            result['operatingMargin'] = (op_inc / rev * 100).round(2).fillna(0)
        else:
            result['operatingMargin'] = 0.0

        return result[result['sid'].str.match(r'^\d{4}$')]
        
    except Exception as e:
        print(f"Error fetching quarterly financials for {year} Q{quarter}: {e}")
        return pd.DataFrame()

# --- TODO 區塊：擴充其他資料類型 ---
def fetch_temp_data(param: str = None) -> pd.DataFrame:
	# TODO: 臨時資料/特定事件抓取邏輯
	print(f"[TODO] Fetching temporary data with param: {param}...")
	return pd.DataFrame()