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
    抓取指定日期的 TWSE 與最近一期 OTC 每日行情與三大法人籌碼，並合併為單一 DataFrame
    target_date_str 格式: YYYYMMDD (例如 '20260320')
    """
    fetcher = ReqFetcher.ReqFetcher(timeout=10)
    formatted_date = datetime.strptime(target_date_str, "%Y%m%d").strftime("%Y%m%d")

    df_twse = pd.DataFrame()
    df_otc = pd.DataFrame()

    # ==================== TWSE ====================
    url_price = (
        f"https://www.twse.com.tw/exchangeReport/MI_INDEX"
        f"?response=json&date={target_date_str}&type=ALLBUT0999"
    )
    df_p_twse = pd.DataFrame()
    df_c_twse = pd.DataFrame()

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

            source_cols = list(field_mapping.values())
            target_cols = list(field_mapping.keys())

            df_p_twse = df_base[source_cols].copy()
            df_p_twse.columns = target_cols
            df_p_twse["sid"] = df_p_twse["sid"].astype(str).str.strip()

            df_p_twse["priceChange"] = combine_price_change(
                df_p_twse["price_pm"], df_p_twse["price_diff"]
            )

            numeric_cols = [
                col for col in target_cols if col not in ("sid", "price_pm", "price_diff")
            ]
            for col in numeric_cols:
                df_p_twse[col] = (
                    df_p_twse[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("--", "0", regex=False)
                    .str.strip()
                )
                df_p_twse[col] = pd.to_numeric(df_p_twse[col], errors="coerce").fillna(0)

            df_p_twse = df_p_twse.drop(columns=["price_pm", "price_diff"])
        else:
            stat = data_p.get("stat") if isinstance(data_p, dict) else "NO_DATA"
            print(f"TWSE 每日行情: {stat}, 使用日期:{target_date_str}")

    except Exception as e:
        print(f"Error fetching TWSE price for {target_date_str}: {e}")

    finally:
        time.sleep(3)

    # TWSE 三大法人籌碼
    url_chip = (
        f"https://www.twse.com.tw/rwd/zh/fund/T86"
        f"?response=json&date={target_date_str}&selectType=ALLBUT0999"
    )
    try:
        data_c = fetcher.req_json(url_chip)
        if data_c and data_c.get("stat") == "OK" and "data" in data_c:
            df_c_twse = pd.DataFrame(data_c["data"], columns=data_c["fields"])
            mapping = {
                "sid": "證券代號",
                "foreignNetBuy": "外陸資買賣超股數(不含外資自營商)",
                "trustNetBuy": "投信買賣超股數",
                "dealerNetBuy": "自營商買賣超股數",
            }
            df_c_twse["sid"] = df_c_twse[mapping["sid"]].astype(str).str.strip()

            for target_col, raw_col in mapping.items():
                if target_col == "sid":
                    continue
                if raw_col in df_c_twse.columns:
                    df_c_twse[target_col] = (
                        df_c_twse[raw_col]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.replace("--", "0", regex=False)
                        .str.strip()
                    )
                    df_c_twse[target_col] = (
                        pd.to_numeric(df_c_twse[target_col], errors="coerce").fillna(0)
                    )
                else:
                    df_c_twse[target_col] = 0

            df_c_twse = df_c_twse[list(mapping.keys())]
        else:
            stat = data_c.get("stat") if isinstance(data_c, dict) else "NO_DATA"
            print(f"TWSE 三大法人籌碼: {stat}, 使用日期:{target_date_str}")

    except Exception as e:
        print(f"Error fetching TWSE chip for {target_date_str}: {e}")

    finally:
        time.sleep(3)

    if not df_p_twse.empty and not df_c_twse.empty:
        df_twse = pd.merge(df_p_twse, df_c_twse, on="sid", how="left").fillna(0)
        df_twse["priceDate"] = formatted_date
        df_twse["market"] = "TWSE"
        df_twse = df_twse[df_twse["sid"].str.match(r"^\d{4}$")].copy()

    # ==================== OTC ====================
    otc_price = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    df_p_otc = pd.DataFrame()
    df_c_otc = pd.DataFrame()

    try:
        data_p_otc = fetcher.req_json(otc_price)
        if data_p_otc:
            df_raw_otc = pd.DataFrame(data_p_otc)
            field_mapping = {
                "sid": "SecuritiesCompanyCode",
                "closePrice": "Close",
                "ttlVolume": "TradingShares",
                "priceChange": "Change",
                "priceDate": "Date",
            }

            raw_sid_col = field_mapping["sid"]
            if raw_sid_col in df_raw_otc.columns:
                df_base_otc = df_raw_otc[df_raw_otc[raw_sid_col].notna()].copy()
                df_base_otc[raw_sid_col] = (
                    df_base_otc[raw_sid_col].astype(str).str.strip()
                )
                df_base_otc = df_base_otc[df_base_otc[raw_sid_col] != ""].copy()

                source_cols = list(field_mapping.values())
                target_cols = list(field_mapping.keys())

                df_p_otc = df_base_otc[source_cols].copy()
                df_p_otc.columns = target_cols
                df_p_otc["sid"] = df_p_otc["sid"].astype(str).str.strip()
                df_p_otc["priceDate"] = df_p_otc["priceDate"].astype(str).str.strip()
                df_p_otc["priceDate"] = df_p_otc["priceDate"].apply(roc_to_ad_str)

                numeric_cols = [
                    col for col in target_cols if col not in ("sid", "priceDate")
                ]
                for col in numeric_cols:
                    df_p_otc[col] = (
                        df_p_otc[col]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.replace("--", "0", regex=False)
                        .str.strip()
                    )
                    df_p_otc[col] = (
                        pd.to_numeric(df_p_otc[col], errors="coerce").fillna(0)
                    )
        else:
            print(f"OTC 每日行情無資料, 使用日期:{target_date_str}")

    except Exception as e:
        print(f"抓取或處理櫃買每日行情失敗：{e}")

    finally:
        time.sleep(3)

    # OTC 三大法人籌碼
    otc_chip = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
    try:
        data_c_otc = fetcher.req_json(otc_chip)
        if data_c_otc:
            df_raw_c_otc = pd.DataFrame(data_c_otc)
            mapping = {
                "sid": "SecuritiesCompanyCode",
                "foreignNetBuy": "Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference",
                "trustNetBuy": "SecuritiesInvestmentTrustCompanies-Difference",
                "dealerNetBuy": "Dealers-Difference",
            }
            raw_sid_col = mapping["sid"]
            if raw_sid_col in df_raw_c_otc.columns:
                df_c_otc = df_raw_c_otc.copy()
                df_c_otc["sid"] = df_c_otc[raw_sid_col].astype(str).str.strip()

                for target_col, raw_col in mapping.items():
                    if target_col == "sid":
                        continue
                    if raw_col in df_c_otc.columns:
                        df_c_otc[target_col] = (
                            df_c_otc[raw_col]
                            .astype(str)
                            .str.replace(",", "", regex=False)
                            .str.replace("--", "0", regex=False)
                            .str.strip()
                        )
                        df_c_otc[target_col] = (
                            pd.to_numeric(df_c_otc[target_col], errors="coerce").fillna(0)
                        )
                    else:
                        df_c_otc[target_col] = 0

                df_c_otc = df_c_otc[list(mapping.keys())]
        else:
            print(f"OTC 三大法人籌碼無資料, 使用日期:{target_date_str}")

    except Exception as e:
        print(f"抓取或處理櫃買三大法人籌碼失敗：{e}")

    finally:
        time.sleep(3)

    if not df_p_otc.empty and not df_c_otc.empty:
        df_otc = pd.merge(df_p_otc, df_c_otc, on="sid", how="left").fillna(0)
        # 若 OTC OpenAPI 回傳的 Date 是民國年或斜線格式，可視需要另外轉成 YYYYMMDD；
        # 這裡先確保有 priceDate 欄位並過濾 4 碼代號
        if "priceDate" not in df_otc.columns or df_otc["priceDate"].eq("").all():
            df_otc["priceDate"] = formatted_date
        df_otc["market"] = "OTC"
        df_otc = df_otc[df_otc["sid"].str.match(r"^\d{4}$")].copy()

    # ==================== 合併 TWSE + OTC ====================
    frames = [d for d in (df_twse, df_otc) if not d.empty]
    if not frames:
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True)

    if debug_mode:
        print(
            f"完成 {target_date_str}：TWSE {len(df_twse)} 筆、OTC {len(df_otc)} 筆、合計 {len(df_all)} 筆"
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
    res=retrive_daily_data(target_date_str, debug)
        
    # 寫入DB
    load_dotenv()
    DB_NAME = os.environ.get("DB_NAME")
    DAILY_COLLECTION = os.environ.get("DAILY_COLLECTION")
    if not debug:
        file_res=  save_pddata_to_json(res,file_prefix=DAILY_COLLECTION,date_suffix=target_date_str)  
        if file_res: 
            print(f"寫入檔案成功: {DAILY_COLLECTION}{target_date_str}.json")
            
        db_res=writeDB_byPD(DB_NAME, DAILY_COLLECTION, pdframe=res, indexlist=['sid','priceDate'])
        if db_res: 
            print("寫入DB成功")
# ===========================================================================================               
if __name__ == "__main__":
    debug_mode=False
    fetch_daily_data(debug_mode)
    
        
        
    

