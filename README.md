# 台灣股市資料擷取 (Stock Collection System)

> 建立自動化爬蟲排程擷取台股每日交易、月度營收與季度財報，並提供動態網頁介面供使用者進行策略選股。

---

## 📌 專案架構與開發進度

- [x] **1. 股票標的清單與每日數據源研究**
  * 使用 TWSE 每日交易彙總表 API：
    `https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={YYYYMMDD}&type=ALLBUT0999`

- [ ] **2. 擷取資料功能**
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
---

## 🗄️ 選股資料庫規格 
### 0. 資料庫名稱: `stock_analysis_db`

### 1. `stock_info` - 股票基本資訊表
* `sid` *(VARCHAR)* **[PK]** - 股票代號
* `s_name` *(VARCHAR)* - 股票名稱
* `s_mainDuty` *(TEXT)* - 主要業務說明
* `s_status` *(VARCHAR)* - 營業狀況（正常上市、變更交易、停牌等）

---

### 2. `dailyData` - 每日交易與籌碼表
* `sid` *(VARCHAR)* **[CPK]** - 股票代號
* `priceDate` *(DATE)* **[CPK]** - 擷取股價日期 (`YYYY-MM-DD`)
* `OpenPrice` *(DECIMAL)* - 開盤價
* `ClosePrice` *(DECIMAL)* - 當日收盤價格
* `highPrice` *(DECIMAL)* - 最高價
* `lowPrice` *(DECIMAL)* - 最低價
* `ttlVolume` *(BIGINT)* - 當日總成交量
* `ForeignNetBuy` *(BIGINT)* - 外資當日買賣超淨額
* `TrustNetBuy` *(BIGINT)* - 投信當日買賣超淨額
* `DealerNetBuy` *(BIGINT)* - 自營商當日買賣超淨額
* `RetailVolume` *(BIGINT)* - 散戶交易量 / 融資券異動

---

### 3. `monthlyData` - 月度基本面與籌碼表
* `sid` *(VARCHAR)* **[CPK]** - 股票代號
* `year` *(SMALLINT)* **[CPK]** - 資料擷取年份 (`YYYY`)
* `month` *(TINYINT)* **[CPK]** - 資料擷取月份 (`1-12`)

* **企業營收 (MOPS)**
  * `revenue` *(BIGINT)* - 當月合併營收金額
  * `revenueMoM` *(DECIMAL)* - 單月營收月增率 (%)
  * `revenueYoY` *(DECIMAL)* - 單月營收年增率 (%)
  * `accRevenue` *(BIGINT)* - 本年迄今累計營收金額
  * `accRevenueYoY` *(DECIMAL)* - 累計營收年增率 (%)

* **董監內部人籌碼 (MOPS)**
  * `insiderHoldingRatio` *(DECIMAL)* - 董監事及內部人持股比例 (%)
  * `insiderPledgeRatio` *(DECIMAL)* - 董監事持股設質比例 (%)

* **股權分散表 (TDCC)**
  * `totalShareholders` *(INT)* - 總股東人數
  * `largeShareholderRatio` *(DECIMAL)* - 大戶持股比例 (%)（如 1000 張以上）
  * `retailShareholderRatio` *(DECIMAL)* - 散戶持股比例 (%)（如 50 張以下）

---

### 4. `quarterData` - 季度財務報表
* `sid` *(VARCHAR)* **[CPK]** - 股票代號
* `year` *(SMALLINT)* **[CPK]** - 資料擷取年份 (`YYYY`)
* `quarter` *(TINYINT)* **[CPK]** - 資料擷取季度 (`1-4`)

* **獲利能力指標 (損益表)**
  * `eps` *(DECIMAL)* - 每股盈餘
  * `epsYoY` *(DECIMAL)* - EPS 年增率 (%)
  * `grossMargin` *(DECIMAL)* - 毛利率 (%)
  * `operatingMargin` *(DECIMAL)* - 營業利益率 (%)
  * `netMargin` *(DECIMAL)* - 稅後淨利率 (%)
  * `roe` *(DECIMAL)* - 股東權益報酬率 (%)
  * `roa` *(DECIMAL)* - 總資產報酬率 (%)

* **財務結構與安全度 (資產負債表)**
  * `bookValuePerShare` *(DECIMAL)* - 每股淨值
  * `debtRatio` *(DECIMAL)* - 負債比率 (%)
  * `currentRatio` *(DECIMAL)* - 流動比率 (%)
  * `quickRatio` *(DECIMAL)* - 速動比率 (%)
  * `inventoryTurnoverDays` *(INT)* - 存貨週轉天數

* **現金流量品質 (現金流量表)**
  * `operatingCashFlow` *(BIGINT)* - 營業活動現金流量
  * `freeCashFlow` *(BIGINT)* - 自由現金流量
  * `cashFlowRatio` *(DECIMAL)* - 營業現金流對淨利比 (%)

---

### 5. `tradingNotice` - 被迫公告資訊
* **sid** (VARCHAR) - 股票代號 [Composite Primary Key]
* **noticeDate** (DATE) - 公告擷取日期 (YYYY-MM-DD) [Composite Primary Key]
* **eventDate** (DATE) - 事實發生日 (YYYY-MM-DD)

* **單月最新自結數 (僅存月報尚未公佈的最新月份)**
  * **selfMonth** (VARCHAR) - 自結月份 (例如: '2026-07')
  * **selfMonthRevenue** (BIGINT) - 當月自結營收 (百萬元)
  * **selfMonthNetProfit** (BIGINT) - 當月歸屬母公司淨利 (百萬元)
  * **selfMonthEPS** (DECIMAL) - 當月自結 EPS (元)
  * **selfMonthEPSYoY** (DECIMAL) - 當月 EPS 年增率 (%)
