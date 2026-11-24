from dotenv import load_dotenv
from MongoDB_op import writeDB_byPD
from pd_dataOp import save_pddata_to_json
import stockTimer 
import ReqFetcher

from bs4 import BeautifulSoup
import numpy as np

import argparse
import pandas as pd
import time
from datetime import datetime

import os
 
# -----------------------------------------------------------------------------
# 1. 日資料抓取與清洗 (TWSE 行情 + 三大法人) -> 對應 dailyData
# -----------------------------------------------------------------------------
def retrive_daily_data(target_date_str,debug_mode):
    """
    抓取指定日期的 TWSE 每日行情與三大法人籌碼
    target_date_str 格式: YYYYMMDD (例如 '20260320')
    
    """
    fetcher = ReqFetcher.ReqFetcher(timeout=10)
    
    # 每日行情
    url_price = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={target_date_str}&type=ALLBUT0999"
    try:
        data_p = fetcher.req_json(url_price)
        df_p = pd.DataFrame()
        if data_p['stat']=="OK": #成功查詢
            raw_p=data_p['tables'][8]
            df_raw = pd.DataFrame(raw_p['data'], columns=raw_p['fields'])

            # 欄位mapping
            field_mapping = {
                "sid": "證券代號",
                "closePrice": "收盤價",
                "ttlVolume":"成交股數",
                "price_pm": "漲跌(+/-)",
                "price_diff": "漲跌價差",
            }
            
            # 1. 以 sid（證券代號）為基準，先過濾掉空值並清理前後空白
            # 此階段不先限制 4 碼，保留完整資料以便後續與另一份資料合併後再統一處理
            raw_sid_col = field_mapping["sid"]
            df_base = df_raw[df_raw[raw_sid_col].notna()].copy()
            df_base[raw_sid_col] = df_base[raw_sid_col].astype(str).str.strip()
            df_base = df_base[df_base[raw_sid_col] != ""].copy()

            # 2. 依 mapping 擷取所需原始欄位並重新命名為目標欄位
            source_cols = list(field_mapping.values())
            target_cols = list(field_mapping.keys())

            df_p = df_base[source_cols].copy()
            df_p.columns = target_cols
            df_p["sid"] = df_p["sid"].astype(str).str.strip()

            # 3. 在數值清理前，先用原始的漲跌符號（price_pm）與漲跌價差（price_diff）合併出 priceChange
            # 避免 price_pm 經過數值轉換後遺失 "+" / "-" 或 HTML 顏色資訊
            df_p["priceChange"] = combine_price_change(df_p["price_pm"], df_p["price_diff"])

            # 4. 批次清理價格與成交量等數值欄位（排除 sid 與原始漲跌欄位）
            # 移除千分位逗號與 "--" 佔位符，無法轉為數值者補 0
            numeric_cols = [
                col for col in target_cols if col not in ("sid", "price_pm", "price_diff")
            ]

            for col in numeric_cols:
                df_p[col] = (
                    df_p[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("--", "0", regex=False)
                    .str.strip()
                )
                df_p[col] = pd.to_numeric(df_p[col], errors="coerce").fillna(0)

            # 5. 視需求移除暫存的 price_pm / price_diff，保留乾淨的輸出欄位
            df_p = df_p.drop(columns=["price_pm", "price_diff"])
     
        else:
            print(f"每日行情: {data_p['stat']}, 使用日期:{target_date_str}")
            
        time.sleep(3) # 避開 API 限流
    
        # 三大法人籌碼
        url_chip = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={target_date_str}&selectType=ALLBUT0999"   
        data_c = fetcher.req_json(url_chip)
        df_c = pd.DataFrame()
        if data_p['stat']=="OK": #成功查詢
            if 'data' in data_c:
                df_c = pd.DataFrame(data_c['data'], columns=data_c['fields'])
                # 欄位mapping
                mapping = {
                    'sid':'證券代號',
                    'foreignNetBuy':'外陸資買賣超股數(不含外資自營商)',
                    'trustNetBuy':'投信買賣超股數' ,
                    'dealerNetBuy':'自營商買賣超股數'
                }
                #處理代號欄位
                df_c["sid"] = df_c[mapping["sid"]].astype(str).str.strip()

                # 處理法人買賣超數值欄位（排除 sid）
                for target_col, raw_col in mapping.items():
                    if target_col == "sid":
                        continue
                    if raw_col in df_c.columns:
                        df_c[target_col] = (
                            df_c[raw_col]
                            .astype(str)
                            .str.replace(",", "", regex=False)
                            .str.replace("--", "0", regex=False)
                            .str.strip()
                        )
                        df_c[target_col] = pd.to_numeric(df_c[target_col], errors="coerce").fillna(0)
                    else:
                        df_c[target_col] = 0

                # 依 mapping keys 篩選最終欄位
                df_c = df_c[list(mapping.keys())]

        else:
            print(f"三大法人籌碼: {data_p['stat']}, 使用日期:{target_date_str}")
                    

        #合併表 df_c and df_p       
        if not df_c.empty and not df_p.empty:
            df = pd.merge(df_p, df_c, on='sid', how='left').fillna(0)
            #補充日期及篩選4碼資料欄位    
            df['priceDate'] = datetime.strptime(target_date_str, '%Y%m%d').strftime('%Y%m%d')
            df = df[df['sid'].str.match(r'^\d{4}$')] # 只保留 4 碼一般股票
        
            return df

    except Exception as e:
        print(f"Error fetching daily data for {target_date_str}: {e}")
        exit()
            
            
def fetch_daily_data(debug=False):
    parser = argparse.ArgumentParser(prog='fetcg_daily.py',
        description='抓取證交所各公司收盤資料及三大法人籌碼',
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--day', '-d', type=int, choices=range(1, 32), help="指定資料日 (1-31)")
    parser.add_argument('--year', '-Y', type=int, help="指定西元年份 (例如: 2026)")
    parser.add_argument('--month', '-M', type=int, help="指定月份 (1-12)")
    args = parser.parse_args()
    
    # 進行輸入檢查
    yyyy,mmmm,dddd=stockTimer.validate_daily_date(args.year,args.month,args.day)
    target_date_str=f"{yyyy}{mmmm}{dddd}"
    # 取得本日大盤資料
    res=retrive_daily_data(target_date_str, debug)
        
    # 寫入DB
    load_dotenv()
    DB_NAME = os.environ.get("DB_NAME")
    DAILY_COLLECTION = os.environ.get("DAILY_COLLECTION")
    if debug:
        file_res=  save_pddata_to_json(res,file_prefix=DAILY_COLLECTION,date_suffix=target_date_str)  
        if file_res: 
            print(f"寫入檔案成功: {DAILY_COLLECTION}{target_date_str}.json")
            
    db_res=writeDB_byPD(DB_NAME, DAILY_COLLECTION, pdframe=res, indexlist=['sid','priceDate'])
    if db_res: 
        print("寫入DB成功")

# 漲跌幅處理===========================================================================================            
def _extract_sign(val) -> int:
    """從漲跌(+/-)欄位萃取方向：上漲 1、下跌 -1、其餘 0"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0

    text = str(val).strip()
    if not text:
        return 0

    style_str = ""
    inner_text = text

    if "<" in text and ">" in text:
        soup = BeautifulSoup(text, "html.parser")
        tag = soup.find(["p", "span", "font", "div"]) or soup
        style_str = (tag.get("style") or "").lower().replace(" ", "")
        inner_text = tag.get_text(strip=True)

    if "+" in inner_text or "＋" in inner_text:
        return 1
    if "-" in inner_text or "－" in inner_text or "−" in inner_text:
        return -1

    if "color:red" in style_str or "color:#ff" in style_str or "color:#e" in style_str:
        return 1
    if "color:green" in style_str or "color:darkgreen" in style_str or "color:#008" in style_str:
        return -1

    return 0


def _to_abs_float(val) -> float:
    """將漲跌價差轉為非負浮點數，無法解析或為 -- 則回傳 0.0"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0

    cleaned = (
        str(val)
        .replace(",", "")
        .replace("--", "0")
        .replace("+", "")
        .replace("-", "")
        .strip()
    )
    if not cleaned:
        return 0.0

    try:
        return abs(float(cleaned))
    except ValueError:
        return 0.0


def combine_price_change(A: pd.Series, B: pd.Series) -> pd.Series:
    """
    將兩個 pd.Series（A: 漲跌(+/-), B: 漲跌價差）合併為單一帶正負號的數值 Series
    """
    # 確保 index 對齊
    s_dir, s_diff = A.align(B, join="left")

    signs = s_dir.apply(_extract_sign)
    diffs = s_diff.apply(_to_abs_float)

    # 若 B 本身已帶負號且 A 沒抓到方向，也做一層防呆
    raw_b_num = pd.to_numeric(
        s_diff.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )
    signs = np.where(
        (signs == 0) & raw_b_num.notna() & (raw_b_num < 0),
        -1,
        np.where((signs == 0) & raw_b_num.notna() & (raw_b_num > 0), 1, signs),
    )

    result = signs * diffs
    # 避免出現 -0.0
    result = np.where(result == 0, 0.0, result)

    return pd.Series(result, index=s_dir.index, name="priceChange")

# ===========================================================================================               
if __name__ == "__main__":
    debug_mode=True
    fetch_daily_data(debug_mode)
    
        
        
    

