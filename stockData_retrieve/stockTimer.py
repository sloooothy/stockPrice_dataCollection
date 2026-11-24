from datetime import date, datetime, timedelta
from ReqFetcher import ReqFetcher
# 常數設定 
today = date.today()
# --- 參數與日期驗證邏輯 ---
def validate_daily_date(year, month, day, yr_roc=False):
    """
    驗證輸入的日日期字串 (YYYYMMDD)。若無輸入或無效/未來日期，回傳最近一個交易日。
    """
    try:
        if year is not None and month is not None and day is not None:
            if(yr_roc):#民國年轉換為西元年 => for datetime轉換
                year+=1911
            # ✅ 轉成 .date() 比較，避開時區錯誤    
            parsed_date = datetime(year, month, day)
            if parsed_date.date() <= today :
                return year,month,day
        else:
            print(f"[Timer Warning] 未取得日期參數，預設本日為交易日")
            
    except ValueError:
            print(f"E [Timer Warning] 輸入日期格式錯誤 ({year}/{month}/{day})。預設本日為交易日")

    return get_default_trading_date(roc=yr_roc)
  
def get_default_trading_date(roc=False):
    
    # 取得今天的西元日期作為預設值
    default_month = today.month
    default_date = today.day
    default_year = today.year
    if roc: #轉民國年回傳 (in roc, out roc)
        default_year=default_year-1911
    
    return default_year,default_month,default_date
    
#==================================================================================================================
