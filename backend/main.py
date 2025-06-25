from fastapi import FastAPI
import yfinance as yf

app = FastAPI()

@app.get("/price/{symbol}")
async def get_stock_price(symbol: str):
    try:
        stock = yf.Ticker(symbol)
        price = stock.info.get("regularMarketPrice")
        
        if price is None:
            return {"error": "找不到該股票代碼或資料來源異常"}
        
        return {
            "symbol": symbol.upper(),
            "price": price
        }
        
    except Exception as e:
        return {"error": str(e)}
