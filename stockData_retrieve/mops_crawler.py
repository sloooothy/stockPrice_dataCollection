import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class MopsCrawler:
    def __init__(self, headless=True):
        """
        初始化爬蟲物件
        :param headless: 是否隱藏瀏覽器視窗 (預設為 True 隱藏)
        """
        self.options = webdriver.ChromeOptions()
        if headless:
            self.options.add_argument('--headless')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--window-size=1920,1080')
        # 防止網站偵測是自動化工具的設定
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        # 💡 新增這兩行以解決管道與記憶體權限問題
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        
        self.driver = None

    def _start_driver(self):
        """啟動瀏覽器（若尚未啟動）"""
        if not self.driver:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), 
                options=self.options
            )

    def fetch_business(self, stock_code):
        """
        查詢單一股票代碼的主要經營業務
        :param stock_code: 股票代碼 (str 或 int)
        :return: 主要經營業務內容 (str)
        """
        self._start_driver()
        target_url = "https://mopsov.twse.com.tw/mops/web/t05st03"
        
        try:
            # 💡 每次查詢都重新載入網頁，清除上一筆的殘留資料與舊表格
            # 如果不在目標網頁，進行導向
            if self.driver.current_url != target_url:
                self.driver.get(target_url)
                time.sleep(1.5) # 給網頁短暫載入的時間
            
            # 1. 定位輸入框、清空並輸入新代號
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "co_id"))
            )
            search_input.click()
            search_input.clear()
            search_input.send_keys(str(stock_code))
            
            # 2. 💡 關鍵保險：在觸發新 AJAX 查詢前，先用 JS 把畫面上舊的表格區域清空！
            # 這樣能強迫畫面進入「等待載入」狀態，絕不會誤抓上一筆資料
            self.driver.execute_script("""
                var oldTable = document.getElementById('table01');
                if (oldTable) { oldTable.innerHTML = ''; }
            """)
            
            # 3. 觸發 AJAX 查詢
            self.driver.execute_script("doAction(); ajax1(document.form1, 'table01');")
            
            # 4. 等待新的「主要經營業務」欄位重新長出來且有內容
            xpath_target_th = "//th[contains(text(), '主要經營業務')]"
            
            def get_business_content(driver):
                try:
                    td = driver.find_element(By.XPATH, f"{xpath_target_th}/following-sibling::td")
                    text = td.text.strip()
                    if text and len(text) > 0:
                        return text
                except:
                    pass
                return False

            # 等待 AJAX 把新資料刷出來
            business_text = WebDriverWait(self.driver, 12).until(get_business_content)
            
            return business_text.strip()

        except Exception as e:
            return f"Error [{stock_code}]: 無法取得資料，原因可能為代碼不存在或請求遭阻擋。"

    def close(self):
        """關閉瀏覽器並釋放資源"""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
