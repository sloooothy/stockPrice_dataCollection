from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import holidays

from ReqFetcher import ReqFetcher

# --- 參數與日期驗證邏輯 ---
def validate_daily_date(year, month, day, yr_roc=False):
    """
    驗證輸入的日日期字串 (YYYYMMDD)。若無輸入或無效/未來日期，回傳最近一個交易日。
    """
    # 常數設定 
    today = date.today()
    try:
        if year is not None and month is not None and day is not None:
            if(yr_roc):#民國年轉換為西元年 => for datetime轉換
                year+=1911
            # ✅ 轉成 .date() 比較，避開時區錯誤    
            parsed_date = datetime(year, month, day)
            if parsed_date.date() <= today :
                if(yr_roc):#回傳轉回原始
                    year-=1911
                return year,month,day
        else:
            print(f"[Timer Warning] 未取得日期參數，進入交易日檢查機制取得最近交易日")
            
    except ValueError:
            print(f"E [Timer Warning] 輸入日期格式錯誤 ({year}/{month}/{day})。 進入交易日檢查機制取得最近交易日")

    return get_default_trading_date(roc=yr_roc)
    
#==================================================================================================================
def _parse_date(raw: str) -> date | None:
    text = raw.strip().replace("/", "-").replace(".", "-")
    parts = text.split("-")
    try:
        if len(parts) == 3:
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            if y < 1911:
                y += 1911
            return date(y, m, d)
        if len(text) == 8 and text.isdigit():
            y, m, d = int(text[:4]), int(text[4:6]), int(text[6:8])
            if y < 1911:
                y += 1911
            return date(y, m, d)
    except ValueError:
        return None
    return None


def _get_twse_holidays(year: int) -> set[date] | None:
    url = "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
    try:
        resp = requests.get(url, params={"date": f"{year}0101", "response": "json"}, timeout=5)
        resp.raise_for_status()
        rows = resp.json().get("data") or []
        result = set()
        for row in rows:
            for cell in row:
                parsed = _parse_date(str(cell))
                if parsed and parsed.year == year:
                    result.add(parsed)
                    break
        return result
    except Exception:
        return None


def is_trading_day(target_date: date) -> bool:
    """傳入 date 物件，判斷是否為台股交易日（回傳 True / False）"""
    if not isinstance(target_date, date):
        return False

    if target_date.weekday() >= 5:
        return False

    twse_holidays = _get_twse_holidays(target_date.year)
    if twse_holidays is not None:
        return target_date not in twse_holidays

    tw_holidays = holidays.TW(years=target_date.year)
    return target_date not in tw_holidays


def get_default_trading_date(roc: bool = False, max_lookback_days: int = 30) -> tuple[int, int, int]:
    """
    取得預設交易日：
    - 若現在為台股開盤前（09:00 前），先往回一天再找最近交易日
    - 若基準日本身不是交易日，也會一路往前推到最近一個交易日
    """
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    target_date = now.date()

    # 09:00 開盤前先往回一天
    if now.time() < time(9, 0):
        target_date -= timedelta(days=1)

    # 往前推到最近一個交易日
    checked_days = 0
    while not is_trading_day(target_date) and checked_days < max_lookback_days:
        target_date -= timedelta(days=1)
        checked_days += 1

    default_year = target_date.year - 1911 if roc else target_date.year
    return default_year, target_date.month, target_date.day
#==================================================================================================================
'''
def validate_or_get_monthly_date(year: int = None, month: int = None, now_tst: datetime = None) -> tuple[int, int]:
    """
    驗證輸入的年月。若尚未到達該月資料開盤/結算日，自動倒退至最近可行月份。
    """
    pass

def validate_or_get_quarterly_date(year: int = None, quarter: int = None, now_tst: datetime = None) -> tuple[int, int]:
    """
    驗證輸入的年與季 (-Y, -Q)。若尚未到達該季資料可取得時間，自動推算最近可行季度。
    """
    pass
'''