import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

class MopsCrawler:
    def __init__(self, headless=True, timeout=30):
        """
        初始化爬蟲物件
        :param headless: 是否隱藏瀏覽器視窗 (預設為 True 隱藏)
        :param timeout: WebDriverWait 的預設等待秒數 (預設 30 秒)
        """
        self.timeout = timeout  # 💡 把 timeout 存成屬性
        self.options = Options()
        
        # 1. Headless 模式
        if headless:
            self.options.add_argument("--headless=new")
            
        # 2. 系統與資源優化
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--window-size=1920,1080")
        
        # 3. 反爬蟲與防偵測設定
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        
        # 4. 模擬真實瀏覽器 User-Agent
        self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # 5. 初始化變數
        self.driver = None
        self.wait = None
        
        # 6. 直接啟動瀏覽器與設定 wait
        self._start_driver()

    def _start_driver(self):
        """啟動瀏覽器（若尚未啟動）"""
        if not self.driver:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), 
                options=self.options
            )
            
            # 透過 CDP 進階覆蓋 navigator.webdriver
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            })
            
        # 💡 使用 self.timeout 來建立 WebDriverWait
        self.wait = WebDriverWait(self.driver, self.timeout)

    def close(self):
        """關閉瀏覽器並釋放資源"""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()