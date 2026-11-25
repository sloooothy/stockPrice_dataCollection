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

OUTPUT_COLUMNS = [
    "sid",
    "closePrice",
    "ttlVolume",
    "priceChange",
    "priceDate",
    "foreignNetBuy",
    "trustNetBuy",
    "dealerNetBuy",
    "market",
]

def _empty_standard_df() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)
    
def _parse_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("--", "0", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)
# -----------------------------------------------------------------------------
# 1. 日資料抓取與清洗 (TWSE 行情 + 三大法人) -> 對應 dailyData
# -----------------------------------------------------------------------------

def fetch_twse_daily_data(fetcher, target_date_str: str, debug_mode: bool = False) -> pd.DataFrame:
    """抓取指定日期（YYYYMMDD）的 TWSE 每日行情與三大法人籌碼，回傳標準 DataFrame"""
    formatted_date = datetime.strptime(target_date_str, "%Y%m%d").strftime("%Y%m%d")
    df_p = pd.DataFrame()
    df_c = pd.DataFrame()

    # 1. TWSE 每日行情
    url_price = (
        f"https://www.twse.com.tw/exchangeReport/MI_INDEX"
        f"?response=json&date={target_date_str}&type=ALLBUT0999"
    )
    try:
        data_p = fetcher.req_json(url_price)
        if data_p and data_p.get("stat") == "OK":
            raw_p = data_p["tables"][8]
            df_raw = pd.DataFrame(raw_p["data"], columns=raw_p["fields"])

            field_mapping = {
                "sid": "證券代號",
                "closePrice": "收盤價",
                "ttlVolume": "成交股數",
                "price_pm": "漲跌(+/-)",
                "price_diff": "漲跌價差",
            }
            raw_sid_col = field_mapping["sid"]
            df_base = df_raw[df_raw[raw_sid_col].notna()].copy()
            df_base[raw_sid_col] = df_base[raw_sid_col].astype(str).str.strip()
            df_base = df_base[df_base[raw_sid_col] != ""].copy()

            df_p = df_base[list(field_mapping.values())].copy()
            df_p.columns = list(field_mapping.keys())
            df_p["sid"] = df_p["sid"].astype(str).str.strip()
            df_p["priceChange"] = combine_price_change(df_p["price_pm"], df_p["price_diff"])

            numeric_cols = ["closePrice", "ttlVolume", "priceChange"]
            for col in numeric_cols:
                df_p[col] = (
                    df_p[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("--", "0", regex=False)
                    .str.strip()
                )
                df_p[col] = pd.to_numeric(df_p[col], errors="coerce").fillna(0)

            df_p = df_p.drop(columns=["price_pm", "price_diff"])
        else:
            stat = data_p.get("stat") if isinstance(data_p, dict) else "NO_DATA"
            print(f"TWSE 每日行情: {stat}, 使用日期:{target_date_str}")
    except Exception as e:
        print(f"Error fetching TWSE price for {target_date_str}: {e}")
    finally:
        time.sleep(3)

    # 2. TWSE 三大法人籌碼
    url_chip = (
        f"https://www.twse.com.tw/rwd/zh/fund/T86"
        f"?response=json&date={target_date_str}&selectType=ALLBUT0999"
    )
    try:
        data_c = fetcher.req_json(url_chip)
        if data_c and data_c.get("stat") == "OK" and "data" in data_c:
            df_c = pd.DataFrame(data_c["data"], columns=data_c["fields"])
            mapping = {
                "sid": "證券代號",
                "foreignNetBuy": "外陸資買賣超股數(不含外資自營商)",
                "trustNetBuy": "投信買賣超股數",
                "dealerNetBuy": "自營商買賣超股數",
            }
            df_c["sid"] = df_c[mapping["sid"]].astype(str).str.strip()
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
            df_c = df_c[list(mapping.keys())]
        else:
            stat = data_c.get("stat") if isinstance(data_c, dict) else "NO_DATA"
            print(f"TWSE 三大法人籌碼: {stat}, 使用日期:{target_date_str}")
    except Exception as e:
        print(f"Error fetching TWSE chip for {target_date_str}: {e}")
    finally:
        time.sleep(3)

    if df_p.empty or df_c.empty:
        return _empty_standard_df()

    df = pd.merge(df_p, df_c, on="sid", how="left").fillna(0)
    df["priceDate"] = formatted_date
    df["market"] = "TWSE"
    df = df[df["sid"].str.match(r"^\d{4}$")].copy()

    if debug_mode:
        print(f"TWSE 完成 {target_date_str}：{len(df)} 筆")

    return df[OUTPUT_COLUMNS]

# -----------------------------------------------------------------------------
# 2. 日資料抓取與清洗 (OTC 行情 + 三大法人) -> 對應 dailyData
# -----------------------------------------------------------------------------
def fetch_otc_daily_data(fetcher, target_date_str: str, debug_mode: bool = False) -> pd.DataFrame:
    """抓取指定日期的 OTC 每日行情與三大法人籌碼，回傳標準 DataFrame（priceDate 為西元 YYYYMMDD）"""
    dt = datetime.strptime(target_date_str, "%Y%m%d")
    date_str_twse = dt.strftime("%Y%m%d")
    date_str_otc = dt.strftime("%Y/%m/%d")

    df_p = pd.DataFrame()
    df_c = pd.DataFrame()

    # 1. OTC 每日行情
    url_price = (
        f"https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
        f"?date={date_str_otc}&type=EW&id=&response=json&order=0&sort=asc"
    )
    try:
        data_p = fetcher.req_json(url_price)
        tables_p = data_p.get("tables") if isinstance(data_p, dict) else None
        if tables_p and len(tables_p) > 0:
            raw_p = tables_p[0]
            clean_fields = [str(col).strip() for col in raw_p.get("fields", [])]
            df_raw = pd.DataFrame(raw_p.get("data", []), columns=clean_fields)

            if debug_mode:
                print("OTC price columns:", df_raw.columns.tolist())

            field_mapping = {
                "sid": "代號",
                "closePrice": "收盤",
                "ttlVolume": "成交股數",
                "priceChange": "漲跌",
            }
            raw_sid_col = field_mapping["sid"]
            if raw_sid_col in df_raw.columns:
                df_base = df_raw[df_raw[raw_sid_col].notna()].copy()
                df_base[raw_sid_col] = df_base[raw_sid_col].astype(str).str.strip()
                df_base = df_base[df_base[raw_sid_col] != ""].copy()

                df_p = df_base[list(field_mapping.values())].copy()
                df_p.columns = list(field_mapping.keys())
                df_p["sid"] = df_p["sid"].astype(str).str.strip()
                df_p["priceChange"] = df_p["priceChange"].apply(parse_price_change)

                for col in ["closePrice", "ttlVolume", "priceChange"]:
                    df_p[col] = _parse_numeric_series(df_p[col])
            else:
                print(f"OTC 每日行情缺少代號欄位，使用日期:{date_str_otc}")
        else:
            print(f"OTC 每日行情無資料，使用日期:{date_str_otc}")
    except Exception as e:
        print(f"Error fetching OTC price for {date_str_otc}: {e}")
    finally:
        time.sleep(3)

    # 2. OTC 三大法人籌碼（依明細欄位索引：外資合計買賣超=10、投信買賣超=13、自營商合計買賣超=22）
    url_chip = (
        f"https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"
        f"?type=Daily&sect=AL&date={date_str_otc}&id=&response=json"
    )
    try:
        data_c = fetcher.req_json(url_chip)
        tables_c = data_c.get("tables") if isinstance(data_c, dict) else None
        if tables_c and len(tables_c) > 0:
            raw_c = tables_c[0]
            rows_c = raw_c.get("data") or []
            if rows_c:
                chip_rows = []
                for row in rows_c:
                    sid = str(row[0]).strip() if len(row) > 0 else ""
                    if not sid:
                        continue
                    chip_rows.append({
                        "sid": sid,
                        "foreignNetBuy": row[10] if len(row) > 10 else 0,
                        "trustNetBuy": row[13] if len(row) > 13 else 0,
                        "dealerNetBuy": row[22] if len(row) > 22 else 0,
                    })

                df_c = pd.DataFrame(chip_rows)
                if not df_c.empty:
                    df_c["sid"] = df_c["sid"].astype(str).str.strip()
                    for col in ["foreignNetBuy", "trustNetBuy", "dealerNetBuy"]:
                        df_c[col] = _parse_numeric_series(df_c[col])
            else:
                print(f"OTC 三大法人籌碼無資料，使用日期:{date_str_otc}")
        else:
            print(f"OTC 三大法人籌碼無資料，使用日期:{date_str_otc}")
    except Exception as e:
        print(f"Error fetching OTC chip for {date_str_otc}: {e}")
    finally:
        time.sleep(3)

    if df_p.empty or df_c.empty:
        return _empty_standard_df()

    df = pd.merge(df_p, df_c, on="sid", how="left").fillna(0)
    df["priceDate"] = date_str_twse
    df["market"] = "OTC"
    df = df[df["sid"].str.match(r"^\d{4}$")].copy()

    if debug_mode:
        print(f"OTC 完成 {date_str_otc}：{len(df)} 筆")

    return df[OUTPUT_COLUMNS]
    
    
# otc opendata ==============================================================================
def fetch_otc_daily_data_recent(fetcher, debug_mode: bool = False) -> pd.DataFrame:
    """抓取最近一期 OTC 每日行情與三大法人籌碼，回傳標準 DataFrame（priceDate 為西元 YYYYMMDD）"""
    otc_price = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
    otc_chip = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
    df_p = pd.DataFrame()
    df_c = pd.DataFrame()

    # 1. OTC 每日行情
    try:
        data_p = fetcher.req_json(otc_price)
        if data_p:
            df_raw = pd.DataFrame(data_p)
            if debug_mode:
                print("OTC price columns:", df_raw.columns.tolist())

            field_mapping = {
                "sid": "SecuritiesCompanyCode",
                "closePrice": "Close",
                "ttlVolume": "TradingShares",
                "priceChange": "Change",
                "priceDate": "Date",
            }
            raw_sid_col = field_mapping["sid"]
            if raw_sid_col in df_raw.columns:
                df_base = df_raw[df_raw[raw_sid_col].notna()].copy()
                df_base[raw_sid_col] = df_base[raw_sid_col].astype(str).str.strip()
                df_base = df_base[df_base[raw_sid_col] != ""].copy()

                df_p = df_base[list(field_mapping.values())].copy()
                df_p.columns = list(field_mapping.keys())
                df_p["sid"] = df_p["sid"].astype(str).str.strip()
                df_p["priceDate"] = (
                    df_p["priceDate"].astype(str).str.strip().apply(roc_to_ad_str)
                )
                df_p["priceChange"] = df_p["priceChange"].apply(parse_price_change)

                numeric_cols = ["closePrice", "ttlVolume", "priceChange"]
                for col in numeric_cols:
                    df_p[col] = (
                        df_p[col]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.replace("--", "0", regex=False)
                        .str.strip()
                    )
                    df_p[col] = pd.to_numeric(df_p[col], errors="coerce").fillna(0)
        else:
            print("OTC 每日行情無資料")
    except Exception as e:
        print(f"抓取或處理櫃買每日行情失敗：{e}")
    finally:
        time.sleep(3)

    # 2. OTC 三大法人籌碼
    try:
        data_c = fetcher.req_json(otc_chip)
        if data_c:
            df_raw_c = pd.DataFrame(data_c)
            mapping = {
                "sid": "SecuritiesCompanyCode",
                "foreignNetBuy": "Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference",
                "trustNetBuy": "SecuritiesInvestmentTrustCompanies-Difference",
                "dealerNetBuy": "Dealers-Difference",
            }
            raw_sid_col = mapping["sid"]
            if raw_sid_col in df_raw_c.columns:
                df_c = df_raw_c.copy()
                df_c["sid"] = df_c[raw_sid_col].astype(str).str.strip()
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
                df_c = df_c[list(mapping.keys())]
        else:
            print("OTC 三大法人籌碼無資料")
    except Exception as e:
        print(f"抓取或處理櫃買三大法人籌碼失敗：{e}")
    finally:
        time.sleep(3)

    if df_p.empty or df_c.empty:
        return _empty_standard_df()

    df = pd.merge(df_p, df_c, on="sid", how="left").fillna(0)
    df["market"] = "OTC"
    df = df[df["sid"].str.match(r"^\d{4}$")].copy()

    if debug_mode:
        otc_dates = df["priceDate"][df["priceDate"] != ""]
        otc_date = otc_dates.mode().iloc[0] if not otc_dates.empty else "未知"
        print(f"OTC 完成（資料日期 {otc_date}）：{len(df)} 筆")

    return df[OUTPUT_COLUMNS]

#===========================================================================================merge
def retrieve_daily_data(target_date_str: str, debug_mode: bool = False) -> pd.DataFrame:
    """
    整合 TWSE 與 OTC 每日資料：確認兩邊日期與目標日期一致後再合併回傳。
    若 OTC 尚未更新或日期不符，則不混入舊資料（可依需求改為只回傳 TWSE）。
    """
    fetcher = ReqFetcher.ReqFetcher(timeout=10)
    dt = datetime.strptime(target_date_str, "%Y%m%d")
    target_ad_str = dt.strftime("%Y%m%d")

    df_twse = fetch_twse_daily_data(fetcher, target_date_str, debug_mode=debug_mode)
    df_otc = fetch_otc_daily_data(fetcher, target_date_str, debug_mode=debug_mode)

    frames = []
    otc_merged_count = 0

    if not df_twse.empty:
        frames.append(df_twse)

    if not df_otc.empty:
        valid_otc_dates = df_otc["priceDate"][df_otc["priceDate"] != ""]
        otc_actual_date = valid_otc_dates.mode().iloc[0] if not valid_otc_dates.empty else ""

        if otc_actual_date == target_ad_str:
            df_otc = df_otc[df_otc["priceDate"] == target_ad_str].copy()
            frames.append(df_otc)
            otc_merged_count = len(df_otc)
            if debug_mode:
                print(f"日期一致（{target_ad_str}），納入 OTC 資料")
        else:
            print(
                f"OTC 日期不一致（櫃買實際日期: {otc_actual_date or '未知'}，"
                f"目標日期: {target_ad_str}），略過 OTC 合併"
            )

    if not frames:
        return _empty_standard_df()

    df_all = pd.concat(frames, ignore_index=True)

    # 若希望「TWSE 與 OTC 都必須有符合目標日期的資料才合併」，可取消下方註解：
    # if df_twse.empty or otc_merged_count == 0:
    #     return _empty_standard_df()

    print(
            f"完成 {target_ad_str}：TWSE {len(df_twse)} 筆、"
            f"OTC {otc_merged_count} 筆、合計 {len(df_all)} 筆"
    )

    return df_all
    
# 日期字串處理==========================================================================================  
def roc_to_ad_str(roc_date) -> str:
    """
    將民國年日期字串轉換為西元 YYYYMMDD 字串
    支援格式範例：'1151122'、'115/11/22'、'115-11-22'
    """
    if pd.isna(roc_date):
        return ""

    # 移除空白與常見分隔符號（/、-、.）
    cleaned = str(roc_date).strip().replace("/", "").replace("-", "").replace(".", "")

    # 民國年通常為 6 碼（如 990101）或 7 碼（如 1151122）
    if not cleaned.isdigit() or len(cleaned) not in (6, 7):
        return ""

    roc_year = int(cleaned[:-4])
    month_day = cleaned[-4:]
    ad_year = roc_year + 1911

    return f"{ad_year:04d}{month_day}"
    
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
    
# for otc    
def parse_price_change(val) -> float:
    """
    將漲跌字串（如 '+1.50'、'-0.53'、'---'、'--'）轉為浮點數，平盤或無漲跌回傳 0.0
    """
    if pd.isna(val):
        return 0.0

    s = str(val).replace(",", "").strip()
    # 處理沒變（---）、佔位符（--）或空字串
    if s in ("", "-", "--", "---", "X", "x"):
        return 0.0

    # 明確去除開頭的 '+' 號（'-' 保留由 float 處理）
    if s.startswith("+"):
        s = s[1:].strip()

    try:
        return float(s)
    except ValueError:
        return 0.0
# ===========================================================================================  

def fetch_daily_data(debug=False):
    parser = argparse.ArgumentParser(prog='fetcg_daily.py',
        description='抓取證交所/櫃買中心各公司收盤資料及三大法人籌碼',
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--day', '-d', type=int, choices=range(1, 32), help="指定資料日 (1-31)")
    parser.add_argument('--year', '-Y', type=int, help="指定西元年份 (例如: 2026)")
    parser.add_argument('--month', '-M', type=int, help="指定月份 (1-12)")
    args = parser.parse_args()
    
    # 進行輸入檢查
    yyyy,mmmm,dddd=stockTimer.validate_daily_date(args.year,args.month,args.day)
    target_date_str=f"{yyyy}{mmmm}{dddd}"
    # 取得本日大盤資料
    res=retrieve_daily_data(target_date_str, debug)
        
    # 寫入DB
    load_dotenv()
    DB_NAME = os.environ.get("DB_NAME")
    DAILY_COLLECTION = os.environ.get("DAILY_COLLECTION")
    if not debug and not res.empty:
        file_res=  save_pddata_to_json(res,file_prefix=DAILY_COLLECTION,date_suffix=target_date_str)  
        if file_res: 
            print(f"寫入檔案成功: {DAILY_COLLECTION}_{target_date_str}.json")
            
        db_res=writeDB_byPD(DB_NAME, DAILY_COLLECTION, pdframe=res, indexlist=['sid','priceDate'])
        if db_res: 
            print("寫入DB成功")
# ===========================================================================================               
if __name__ == "__main__":
    debug_mode=False
    fetch_daily_data(debug_mode)
    
        
        
    

