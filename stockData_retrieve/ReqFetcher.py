import json
from pathlib import Path
import requests

# ==========================================
# 使用範例總覽
# 1. 使用預設 referer（https://www.twse.com.tw/），分開呼叫 req_json 與 save_to_json：
#    fetcher = ReqFetcher(timeout=10)
#    data = fetcher.req_json("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
#    fetcher.save_to_json(data, "data/twse_stock_day_all.json")
#
# 2. 帶查詢參數 params 抓特定條件資料：
#    data_with_params = fetcher.req_json(
#        url="https://example.com/api/v1/trades",
#        params={"date": "20261123", "symbol": "2330"},
#    )
#
# 3. 覆蓋預設 referer 或關閉 Referer：
#    other_fetcher = ReqFetcher(referer="https://www.tpex.org.tw/")
#    no_ref_fetcher = ReqFetcher(referer=None)
#
# 4. 設定 extra_headers（例如 Token 或自訂標頭），並用 fetch_and_save 一次完成抓取與存檔：
#    api_fetcher = ReqFetcher(
#        timeout=15,
#        extra_headers={
#            "Authorization": "Bearer YOUR_API_TOKEN",
#            "X-Custom-Source": "market-collector",
#        },
#    )
#    saved_path = api_fetcher.fetch_and_save(
#        url="https://example.com/api/v1/records",
#        filepath="data/records.json",
#        params={"limit": 100, "page": 1},
#    )
# ==========================================

class ReqFetcher:
    def __init__(
        self,
        timeout=10,
        referer="https://www.twse.com.tw/",
        extra_headers=None,
    ):
        """
        初始化 HTTP 請求工具

        :param timeout: 請求逾時秒數，避免網路卡住時無限等待
        :param referer: 來源網址，預設為證交所網址；不需要時可傳入 None，或改傳其他來源網址
        :param extra_headers: 額外或覆蓋用的 HTTP headers 字典（選填）
        """
        self.timeout = timeout

        # 建立預設 headers，模擬一般桌面瀏覽器請求 JSON API
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }

        if referer:
            self.headers["Referer"] = referer

        if extra_headers:
            self.headers.update(extra_headers)

    def req_json(self, url, params=None):
        """
        發送 GET 請求並回傳解析後的 JSON 資料

        :param url: API 或 OpenData 網址
        :param params: 網址查詢參數（例如 {"date": "20261123"}），可為 None
        :return: 解析後的 JSON（通常是 dict 或 list）
        """
        res = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=self.timeout,
        )
        # 若 HTTP 狀態碼不是 2xx（例如 403、404、500），直接拋出例外
        res.raise_for_status()
        return res.json()

    def save_to_json(self, data, filepath, indent=2):
        """
        將資料存成 UTF-8 編碼的 JSON 檔

        :param data: 要寫入的 Python 資料結構
        :param filepath: 存檔路徑（字串或 Path 物件）
        :param indent: JSON 縮排空格數；設為 None 可減小檔案體積
        :return: 實際寫入的 Path 物件
        """
        path = Path(filepath)
        # 若目標資料夾不存在，自動建立父層目錄
        path.parent.mkdir(parents=True, exist_ok=True)
        # ensure_ascii=False 讓中文字元直接以可讀文字儲存，而不是 \uXXXX
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        return path

    def fetch_and_save(self, url, filepath, params=None, indent=2):
        """
        一站式抓取 JSON 資料並存檔

        :param url: API 或 OpenData 網址
        :param filepath: 存檔路徑
        :param params: 網址查詢參數（選填）
        :param indent: JSON 縮排空格數
        :return: 實際寫入的 Path 物件
        """
        data = self.req_json(url, params=params)
        return self.save_to_json(data, filepath, indent=indent)