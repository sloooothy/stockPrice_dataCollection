from datetime import datetime, timedelta
import holidays

# 常數設定
TW_HOLIDAYS = holidays.TW(years=range(datetime.now().year - 2, datetime.now().year + 2))

# --- 基礎交易日檢查 ---
def is_trading_day(target_date: datetime) -> bool:
	"""檢查是否為台股交易日 (排除週末與國定假日)"""
	if target_date.weekday() >= 5:
		return False
	if target_date.date() in TW_HOLIDAYS:
		return False
	return True

def find_last_trading_day(current_date: datetime) -> datetime:
	"""往前尋找最近一個交易日"""
	target_date = current_date
	while not is_trading_day(target_date):
		target_date -= timedelta(days=1)
	return target_date

def get_default_trading_date(now_tst: datetime) -> datetime:
	"""判斷今天收盤後或是歷史最後交易日"""
	today_tst = now_tst.replace(hour=0, minute=0, second=0, microsecond=0)
	cutoff_time = now_tst.replace(hour=16, minute=0, second=0, microsecond=0)
	
	if is_trading_day(today_tst) and now_tst >= cutoff_time:
		return today_tst
	elif is_trading_day(today_tst) and now_tst < cutoff_time:
		return find_last_trading_day(today_tst - timedelta(days=1))
	else:
		return find_last_trading_day(today_tst)

# --- 參數與日期驗證邏輯 ---
def validate_or_get_daily_date(input_date_str: str = None, now_tst: datetime = None) -> datetime:
	"""
	驗證輸入的日日期 (YYYYMMDD)。若無輸入或無效/未來日期，回傳最近一個交易日。
	"""
	if now_tst is None:
		now_tst = datetime.now()

	if input_date_str:
		try:
			parsed_date = datetime.strptime(input_date_str, '%Y%m%d')
			# 不可大於今天，且必須是交易日
			if parsed_date <= now_tst and is_trading_day(parsed_date):
				return parsed_date
			else:
				print(f"[Timer Warning] 輸入日期 {input_date_str} 非有效交易日或超過今日，自動尋找最近交易日...")
		except ValueError:
			print(f"[Timer Warning] 輸入日期格式錯誤 ({input_date_str})，預期格式 YYYYMMDD。自動尋找最近交易日...")

	return get_default_trading_date(now_tst)

def validate_or_get_monthly_date(year: int = None, month: int = None, now_tst: datetime = None) -> tuple[int, int]:
	"""
	驗證輸入的年月。若尚未到達該月資料開盤/結算日（例如每月12號前尚無上月資料），自動倒退至最近可行月份。
	"""
	if now_tst is None:
		now_tst = datetime.now()

	# 每月 12 號前可能尚無上個月完整月報/月資料，預設可行月份為上上個月或上個月
	if now_tst.day < 12:
		# 扣除 2 個月推算安全月份
		first_of_this_month = now_tst.replace(day=1)
		last_safe_date = (first_of_this_month - timedelta(days=1)).replace(day=1) - timedelta(days=1)
	else:
		# 扣除 1 個月
		first_of_this_month = now_tst.replace(day=1)
		last_safe_date = first_of_this_month - timedelta(days=1)

	default_year, default_month = last_safe_date.year, last_safe_date.month

	if year and month:
		try:
			target_date = datetime(year, month, 1)
			if target_date <= last_safe_date:
				return year, month
			else:
				print(f"[Timer Warning] {year}/{month} 資料尚未發布或超過可行月份，切換至最近可行月份 {default_year}/{default_month}")
		except ValueError:
			print(f"[Timer Warning] 輸入年月格式錯誤，切換至最近可行月份 {default_year}/{default_month}")

	return default_year, default_month

def validate_or_get_quarterly_date(year: int = None, quarter: int = None, now_tst: datetime = None) -> tuple[int, int]:
	"""
	驗證輸入的年與季 (-Y, -Q)。若尚未到達該季資料可取得時間，自動推算最近可行季度。
	"""
	if now_tst is None:
		now_tst = datetime.now()

	# 簡易推算前一季 (以季結算安全期為主)
	current_quarter = (now_tst.month - 1) // 3 + 1
	if current_quarter == 1:
		default_year, default_quarter = now_tst.year - 1, 4
	else:
		default_year, default_quarter = now_tst.year, current_quarter - 1

	if year and quarter:
		if 1 <= quarter <= 4:
			if year < now_tst.year or (year == now_tst.year and quarter < current_quarter):
				return year, quarter
			else:
				print(f"[Timer Warning] {year} Q{quarter} 尚未結束或資料未公開，切換至最近可行季度 {default_year} Q{default_quarter}")
		else:
			print(f"[Timer Warning] 季度參數無效 ({quarter})，必須為 1~4。切換至最近可行季度 {default_year} Q{default_quarter}")

	return default_year, default_quarter