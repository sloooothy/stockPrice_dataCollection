# 台灣股市資料擷取 (Stock Collection)

> 建立自動化爬蟲排程擷取台股每日交易、月度營收與季度財報，此為以Github驅動的基本資料庫存。

---

## 📌 專案架構與開發目標

- [x] **1. 股票標的清單與每日數據源研究**
  * 使用 MOPS每日交易資訊，擷取全部上市（不含權證=ALLBUT0999）/上櫃股票之市場交易資訊。：
    `https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={YYYYMMDD}&type=ALLBUT0999`
  * 📅 預定開發項目
	* **每日報價數據**：每日 16:00 前自動擷取個股(外資、投信、自營商)買賣超資訊。
	* **月營收數據**：每月 12 號夜間 23:00 自動擷取上市櫃公司月營收。
	* **季營收數據**：3個月一次 自動擷取上市櫃公司月營收。

- [ ] **2. 交易時間與日曆模組**
  * 結合 Python `holidays` 套件，自動判斷台灣國定假日與非交易日。
   * 提供 `get_latest_trading_day()` 函數回傳最近一個有效交易日。

- [x] **3. GitHub Actions 排程自動化**
  * 設定每日收盤後（16:00 UTC+8）自動觸發爬蟲與資料消化流程。
  * 設定每月12日自動觸發爬蟲與資料消化流程。

- [ ] **4. 原始資料存儲 (Raw Data saving)**
  * **路徑**：`./stockData/`
  * 提供json格式資料存儲，作為中間資料供以確認。
  
- [x] **5. MongoDB寫入 (MongoDB Atlas storage)**
  * 線上儲存資料，至mongoDB atlas cluster
  
- [x] **6. Discord線上通知 (Discord Notification)**
  * 將抓到的資料，彙整完畢後傳送到discord進行通知
  * 測試用頻道: 
  * 目前僅提供每日若有特殊狀況要求公開前一個月的財務狀況者，提供公司名稱/代號 及 每股盈餘

---
## Datadase規劃設計
* **DB schema**: 參考db_schema.md，逐步修正中
---
