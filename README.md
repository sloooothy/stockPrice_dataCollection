# 台灣股市資料擷取 (Stock Collection)

> 建立自動化爬蟲排程擷取台股每日交易、月度營收與季度財報，此為以Github驅動的基本資料庫存。

---

## 📌 專案架構與開發進度

- [x] **1. 股票標的清單與每日數據源研究**
  * 使用 TWSE 每日交易彙總表 API：
    `https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={YYYYMMDD}&type=ALLBUT0999`

- [x] **2. 擷取資料標的**
  * 擷取全部上市（不含權證=ALLBUT0999）/上櫃股票之市場交易資訊。

- [ ] **3. 交易時間與日曆模組**
  * 結合 Python `holidays` 套件，自動判斷台灣國定假日與非交易日。
   * 提供 `get_latest_trading_day()` 函數回傳最近一個有效交易日。

- [x] **4. GitHub Actions 排程自動化**
  * 設定每日收盤後（16:00 UTC+8）自動觸發爬蟲與資料消化流程。
  * 設定每月12日自動觸發爬蟲與資料消化流程。

- [x] **5. 原始資料存儲 (Raw Data Pipeline)**
  * **路徑**：`./stockData/`
  * **CSV Format**：供 MongoDB 批次匯入使用。
  * **DB File**：併入 `.db` 檔案（支援 SQLite / PostgreSQL）。

---

## 📅 預定開發項目
* **每日報價數據**：每日 16:00 前自動擷取個股(外資、投信、自營商)買賣超資訊。
* **月營收數據**：每月 12 號夜間 23:00 自動擷取上市櫃公司月營收。
* **季營收數據**：3個月一次 自動擷取上市櫃公司月營收。
* **DB schema**: 詳閱db_schema.md
---
