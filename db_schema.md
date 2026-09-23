## 🗄️ 選股資料庫規格 

### 簡易版 `twse_daily_market_data` - TWSE 每日個股行情與籌碼資料表

* `Date` *(DATE / VARCHAR)* **[PK]** - 資料日期（格式：`YYYY-MM-DD`）
* `Ticker` *(VARCHAR)* **[PK]** - 股票代號（附帶 `.TW` 後綴，例如：`2330.TW`）
* `Name` *(VARCHAR)* - 股票名稱
* `Open` *(DECIMAL / FLOAT)* - 開盤價
* `High` *(DECIMAL / FLOAT)* - 最高價
* `Low` *(DECIMAL / FLOAT)* - 最低價
* `Close` *(DECIMAL / FLOAT)* - 收盤價
* `Volume` *(BIGINT / INT)* - 成交股數
* `ForeignNetBuy` *(BIGINT / INT)* - 外陸資買賣超股數（不含外資自營商）
* `TrustNetBuy` *(BIGINT / INT)* - 投信買賣超股數
* `DealerNetBuy` *(BIGINT / INT)* - 自營商買賣超股數
* `RetailVolume` *(BIGINT / INT)* - 估算散戶成交量（總成交股數扣除三大法人買賣超絕對值總和，下限為 0）


### 0. 資料庫名稱: `stock_analysis_db`

### 1. `stock_info` - 股票基本資訊表
* `sid` *(VARCHAR)* **[PK]** - 股票代號
* `s_name` *(VARCHAR)* - 股票名稱
* `s_mainDuty` *(TEXT)* - 主要業務說明
* `s_status` *(VARCHAR)* - 營業狀況（正常上市、變更交易、停牌等）

---
### 2. `dailyData` - 每日交易與籌碼表

#### 2.1 原始預定資料規格
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

#### 2.2 實作狀況與缺口說明
* ✅ **已成功實作抓取：** `sid`, `priceDate`, `OpenPrice`, `ClosePrice`, `highPrice`, `lowPrice`, `ttlVolume`, `ForeignNetBuy`, `TrustNetBuy`, `DealerNetBuy`, `RetailVolume` (由成交量減三大法人計算估算值)
* ⚠️ **需額外擴充抓取：** 精準 `RetailVolume` / 融資券異動（需額外串接 TWSE `MI_MARGIN` 融資融券餘額 API）

---

### 3. `monthlyData` - 月度基本面與籌碼表

#### 3.1 原始預定資料規格
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

#### 3.2 實作狀況與缺口說明
* ✅ **已成功實作抓取：**
  * **企業營收 (MOPS)：** `revenue`, `revenueMoM`, `revenueYoY`, `accRevenue`, `accRevenueYoY`
* ⚠️ **需額外擴充抓取：**
  * **董監內部人籌碼 (MOPS)：** `insiderHoldingRatio`, `insiderPledgeRatio`（需爬取 MOPS 董監持股頁面）
  * **股權分散表 (TDCC)：** `totalShareholders`, `largeShareholderRatio`, `retailShareholderRatio`（需串接 TDCC 臺灣集中保管結算所 API / 網頁）

---

### 4. `quarterData` - 季度財務報表

#### 4.1 原始預定資料規格
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

#### 4.2 實作狀況與缺口說明
* ✅ **已成功實作抓取：**
  * **獲利能力指標 (損益表)：** `eps`, `operatingMargin`（由營業利益與營收估算）
* ⚠️ **需額外擴充抓取：**
  * **獲利能力指標：** `epsYoY`, `grossMargin`, `netMargin`, `roe`, `roa`
  * **財務結構與安全度：** `bookValuePerShare`, `debtRatio`, `currentRatio`, `quickRatio`, `inventoryTurnoverDays`（需爬取 MOPS 資產負債表與相關指標頁面）
  * **現金流量品質：** `operatingCashFlow`, `freeCashFlow`, `cashFlowRatio`（需爬取 MOPS 現金流量表）
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
