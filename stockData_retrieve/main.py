import argparse
import pytz
from datetime import datetime

import stockTimer as timer
import dataCollection as collector

TZ_TAIPEI = pytz.timezone('Asia/Taipei')

def main(args_list=None):
	parser = argparse.ArgumentParser(description="TWSE Stock Data Fetcher")
	
	# 命令列參數設定
	parser.add_argument('--type', '-t', choices=['daily', 'monthly', 'quarterly', 'temp'], default='daily', help="資料類型 (預設: daily)")
	parser.add_argument('--daily', '-d', type=str, help="指定日資料日期 (YYYYMMDD)")
	parser.add_argument('--year', '-Y', type=int, help="指定年份 (例如: 2026)")
	parser.add_argument('--month', '-M', type=int, help="指定月份 (1-12)")
	parser.add_argument('--quarter', '-Q', type=int, choices=[1, 2, 3, 4], help="指定季度 (1-4)")
	parser.add_argument('--param', '-p', type=str, help="臨時資料用的自訂參數")

	args = parser.parse_args(args_list) # 如果有傳入 args_list 就解析 args_list，否則解析命令列輸入
	now_tst = datetime.now(TZ_TAIPEI)

	# 1. 日資料 (Daily)
	if args.type == 'daily':
		valid_date = timer.validate_or_get_daily_date(args.daily, now_tst)
		date_str = valid_date.strftime('%Y%m%d')
		
		print(f"Initiating fetch process for Daily date: {valid_date.strftime('%Y-%m-%d')}")
		market_df = collector.fetch_twse_all_data(date_str)
		
		if not market_df.empty:
			collector.save_data_to_json(market_df, "stock_daily", date_str)
			print("\nProcess finished successfully.")
		else:
			print("\nProcess failed or no data fetched.")

	# 2. 月資料 (Monthly) - TODO
	elif args.type == 'monthly':
		valid_year, valid_month = timer.validate_or_get_monthly_date(args.year, args.month, now_tst)
		print(f"Target Monthly Date: {valid_year}/{valid_month:02d}")
		
		# TODO: 呼叫 collector.fetch_monthly_data(valid_year, valid_month) 並存檔
		# monthly_df = collector.fetch_monthly_data(valid_year, valid_month)
		monthly_df = collector.fetch_monthly_revenue(valid_year, valid_month)
		if not monthly_df.empty:
			collector.save_data_to_json(monthly_df, "stock_monthly", f"{valid_year}{valid_month:02d}")

	# 3. 季資料 (Quarterly) - TODO
	elif args.type == 'quarterly':
		valid_year, valid_quarter = timer.validate_or_get_quarterly_date(args.year, args.quarter, now_tst)
		print(f"Target Quarterly Date: {valid_year} Q{valid_quarter}")
		
		# TODO: 呼叫 collector.fetch_quarterly_data(valid_year, valid_quarter) 並存檔
		# quarterly_df = collector.fetch_quarterly_data(valid_year, valid_quarter)
		quarterly_df = collector.fetch_quarterly_financials(valid_year, valid_quarter)
		if not quarterly_df.empty:
			collector.save_data_to_json(quarterly_df, "stock_quarterly", f"{valid_year}_Q{valid_quarter}")

	# 4. 臨時資料 (Temp) - TODO
	elif args.type == 'temp':
		print(f"Target Temp Task with param: {args.param}")
		# TODO: 呼叫 collector.fetch_temp_data(args.param) 並存檔
		temp_df = collector.fetch_temp_data(args.param)
		if not temp_df.empty:
			collector.save_data_to_json(temp_df, "stock_temp", now_tst.strftime('%Y%m%d_%H%M%S'))

if __name__ == "__main__":
	main()
	
	# 範例：連續執行不同任務
	#print("--- 執行日資料 ---")
	#main(['-t', 'daily', '-d', '20260320'])
	
	#print("\n--- 執行月資料 ---")
	#main(['-t', 'monthly', '-Y', '2026', '-M', '2'])
	
	#print("\n--- 執行季資料 ---")
	#main(['-t', 'quarterly', '-Y', '2025', '-Q', '4'])
