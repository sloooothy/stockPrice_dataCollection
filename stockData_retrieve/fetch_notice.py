import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from io import StringIO
import time
import random
import argparse
import datetime
from pymongo.errors import ServerSelectionTimeoutError

from bs4 import BeautifulSoup

import csv
import re
import os

from MongoDB_op import upsertDB_byQuery
from dotenv import load_dotenv

load_dotenv()
DB_NAME = os.environ.get("DB_NAME")
NOTICE_COLLECTION = os.environ.get("NOTICE_COLLECTION")

def _init_driver():
    options = Options()
    options.add_argument("--headless=new") # 若需看過程(debug)請把本行註解
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    return driver

def parse_single_financial_block(block):
    """專門解析單一區塊文字的輔助函式"""
    # 1. 事實發生日
    announce_date_str = ""
    date_match = re.search(r"1\.事實發生日[：:\s]*([1-9]\d{2}/\d{2}/\d{2})", block)
    if date_match:
        announce_date_str = date_match.group(1).strip()

    # 2. 股票編號與公司名稱
    co_match = re.search(r"本資料由\s*[\(（](?:上市公司|上櫃公司)[\)）]\s*(\d{4})\s+([^\s]+)", block)
    if not co_match:
        return None
    stock_code = co_match.group(1)
    company_name = co_match.group(2)

    # 3. 財務業務資訊區塊處理
    fin_pos = block.find("財務業務資訊")
    if fin_pos != -1:
        line_end_pos = block.find("\n", fin_pos)
        fin_block = block[line_end_pos + 1:] if line_end_pos != -1 else block[fin_pos:]
    else:
        fin_block = block

    # 4. 最近月份
    month_str = ""
    m_search = re.search(r"11\d年\d{1,2}月|11\d/\d{2}", fin_block)
    if m_search:
        month_str = convert_date_format(m_search.group(0))

    # 5. EPS
    eps_val = ""
    eps_match = re.search(r"每股盈餘[\s\S]*?([-\(\)\（\）\.\d]*\d[-\(\)\（\）\.\d]*)", fin_block)
    if eps_match:
        raw_eps = eps_match.group(1).strip()
        eps_val = convert_negative_number(raw_eps)

    return {
        "sid": stock_code,
        "sname": company_name,
        "announceDate": announce_date_str,
        "notice_date": month_str,
        "eps": eps_val,
    }
    

def execute_and_save_mops_data(driver, wait, year, month, day):
    """執行 MOPS 查詢，邊爬邊即時 upsert 寫入 MongoDB，避免記憶體爆掉"""
    main_window = driver.current_window_handle
    driver.get("https://mops.twse.com.tw/mops/#/web/t05st02")
    
    try:
        # 填表與查詢邏輯 (與前面相同)
        year_input = wait.until(EC.element_to_be_clickable((By.ID, "year")))
        year_input.clear()
        for char in str(year):
            year_input.send_keys(char)
            time.sleep(random.uniform(0.1, 0.2))

        month_el = wait.until(EC.element_to_be_clickable((By.ID, "month")))
        Select(month_el).select_by_value(str(int(month)))

        formatted_day = f"{int(day):02d}"
        day_select_el = wait.until(
            lambda d: d.find_element(By.ID, "day") if not d.find_element(By.ID, "day").get_attribute("disabled") else False
        )
        Select(day_select_el).select_by_value(formatted_day)
        
        search_btn = driver.find_element(By.ID, "searchBtn")
        driver.execute_script("arguments[0].click();", search_btn)
        
        # 等待渲染
        found_data = False
        final_html = ""
        for i in range(15):
            time.sleep(2)
            page_text = driver.execute_script("return document.body.innerText")
            rows = driver.execute_script("return document.querySelectorAll('tr').length")
            if (rows > 2 or "發言日期" in page_text or "主旨" in page_text):
                final_html = driver.page_source
                found_data = True
                break

        if not found_data:
            print("⚠️ 未偵測到預期的資料結構。")
            return

        soup = BeautifulSoup(final_html, 'html.parser')
        rows = soup.find_all('tr')
        
        target_indices = []
        includ_keywords = ['公司有價證券']
        exclud_Keywords = ['公司債']

        for idx, row in enumerate(rows):
            subject_td = row.find('td', {'data-title': '主旨:'})
            if subject_td:
                subject_text = subject_td.get_text(strip=True)
                if any(kw in subject_text for kw in includ_keywords) and all(ekw not in subject_text for ekw in exclud_Keywords):
                    target_indices.append(idx)

        if not target_indices:
            print("⚠️ 沒有找到符合條件的新聞。")
            return

        print(f"📋 共找到 {len(target_indices)} 筆符合條件的新聞，開始邊爬邊寫入 MongoDB...")

        # 2. 迴圈：抓取單筆 -> 解析單筆 -> 即時 Upsert
        for target_index in target_indices:
            try:
                main_window = driver.current_window_handle
                selenium_rows = driver.find_elements(By.CSS_SELECTOR, "tr")
                if target_index >= len(selenium_rows):
                    continue
                
                target_row = selenium_rows[target_index]
                view_btn = target_row.find_element(By.CSS_SELECTOR, "button")
                
                driver.execute_script("arguments[0].click();", view_btn)
                time.sleep(2)
                
                all_windows = driver.window_handles
                if len(all_windows) > 1:
                    for window in all_windows:
                        if window != main_window:
                            driver.switch_to.window(window)
                            break
                    
                    # 取得單篇詳細內容
                    detail_content = driver.execute_script("return document.body.innerText")
                    
                    # 解析單篇內容成 Dict
                    single_data = parse_single_financial_block(detail_content)
                    
                    if single_data and single_data.get("sid"):
                        # 🎯 關鍵：即時進行 Upsert 寫入資料庫
                        # 假設用 「股票代號 + 公告月份」 作為唯一辨識條件
                        filter_query = {
                            "sid": single_data["sid"], 
                            "notice_date": single_data["notice_date"]
                        }
                        #collection.update_one(filter_query, {"$set": single_data}, upsert=True)
                        upsertDB_byQuery(DB_NAME, NOTICE_COLLECTION, {"$set": single_data}, filter_query)
                        print(f"💾 [即時寫入] 公司 {single_data['sid']} ({single_data['sname']}) 資料已同步！")
                    
                    driver.close()
                    driver.switch_to.window(main_window)
                    time.sleep(1)
                    
            except Exception as e:
                print(f"❌ 處理索引 {target_index} 時發生錯誤：{e}")
                if len(driver.window_handles) > 1:
                    driver.close()
                driver.switch_to.window(main_window)

    except Exception as e:
        print(f"💥 執行過程發生錯誤: {e}")



def fetch_note(mops_year: str, mops_month: str, mops_day:str):
    driver = _init_driver()
    wait = WebDriverWait(driver, 30)

    try:
        execute_and_save_mops_data(driver, wait, mops_year, mops_month, mops_day)
    finally:
        driver.quit()

# ── 1. 參數解析與時間邏輯判定 ──────────────────────────────
def get_target_date():
    """
    fetch input dates (includes year month date), defaults to today if missing or invalid.
    '-yy', '--year', type=int, help='指定民國年分 (如: 115)'
    '-mm', '--month', type=int, help='指定月份 (如: 1-12)'
    '-dd', '--date', type=int, help='指定日期 (如: 1)'
    """
    parser = argparse.ArgumentParser(
        prog='fetcg_notice.py',
        description='自動抓取證交所公司新聞',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-yy', '--year', type=int, help='指定民國年分 (如: 115)')
    parser.add_argument('-mm', '--month', type=int, help='指定月份 (如: 1-12)')
    parser.add_argument('-dd', '--date', type=int, help='指定日期 (如: 1)')
    args = parser.parse_args()

    # 取得今天的西元日期，並換算成民國年格式作為預設值
    today = datetime.date.today()
    default_roc_year = today.year - 1911
    default_month = today.month
    default_date = today.day

    # 檢查是否年、月、日皆有填寫
    if args.year is not None and args.month is not None and args.date is not None:
        try:
            west_year = args.year + 1911  # 民國年轉西元年
            
            # 利用 datetime 驗證日期是否存在（如 2月30日會引發 ValueError）
            target_date = datetime.date(west_year, args.month, args.date)
            
            # 驗證成功，回傳輸入的年月日
            return args.year, args.month, args.date

        except ValueError:
            # 日期不合法，印出提示並回傳今天的日期
            print(f"⚠️ 輸入的日期不合法，將自動採用今天的日期：{default_roc_year}年{default_month}月{default_date}日")
            return default_roc_year, default_month, default_date
    else:
        # 參數未完整填寫時，印出提示並回傳今天的日期
        print(f"ℹ️ 未完整填寫日期參數，將自動採用今天的日期：{default_roc_year}年{default_month}月{default_date}日")
        return default_roc_year, default_month, default_date

def convert_date_format(date_str):
    # 將「115年7月」或類似格式統一轉換為「115/07」
    match = re.search(r"(\d+)\D+(\d+)", date_str)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)  # 補齊兩位數，例如 7 變成 07
        return f"{year}/{month}"
    return date_str


def convert_negative_number(num_str):
    # 將有括號的數字（例如 (0.13)）轉換為負數（-0.13）
    num_str = num_str.strip()
    if num_str.startswith("(") and num_str.endswith(")"):
        return "-" + num_str[1:-1]
    return num_str

##  debug
if __name__ == "__main__":
    yyyy, mmmm, dddd=get_target_date()
    #date_str = f"{yyyy}/{mmmm:02d}/{dddd:02d}"
    fetch_note(yyyy, mmmm, dddd)
    

           
        
        

