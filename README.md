# 程式計畫：台灣股市資料擷取與選股系統
實現簡易動態網頁，依使用者需求進行篩選之介面
#### 主要目標
- [x] 1. 股票標的清單來源研究：可提供每日交易彙總表 API/網站。
	來源:TWSE，"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date}&type=ALLBUT0999"
	* date={date}: 這是指定要查詢哪一天的交易資料。您需要將 {date} 替換成實際的日期，格式通常是 YYYYMMDD（例如：20251010）。
	* type=ALLBUT0999: 這是指定要查詢哪一類的市場資訊。ALLBUT0999 表示查詢 全部上市股票（不含權證）的所有市場資訊。

- [ ] 2. 時間模組
	* 取得最近一個有效交易日: 配合holidays 套件，確認最近一個台灣的交易日

- [x] 3. 擷取排程：利用github action達成排程執行。
	* 每日收盤後 (例如 16:00) 

- [x] 4. 原始(raw)資料儲存：將股價相關資料存為 DB 及 CSV檔案，消化後轉入資料庫。
	* 存於stockData 資料夾下
	* CSV: mongodb用
	* DB: .db檔案，SQLite, PostgressDB等使用

- [ ] 5. 選股資料庫結構：依據(collection, Table) 的 (field, attribute)結構
	* stock_info: sid(股票代號), s_name(股票名稱), s_mainDuty(基本業務), s_status(營業狀況)
	* dailyPrice: priceDate(擷取股價日期), sid(股票代號), price(當日收盤價格)

## 預定項目
	* (每日)三大法人進出資料擷取 ：每日交易日傍晚 (例如 18:00 前)，依據有效標的清單，擷取法人進出數據。
	* (每月)月營收資料擷取 ：每月X號夜間11點，依據當前有效的標的清單，擷取月營收。
	* 選股策略設定與篩選
	* 模組化
