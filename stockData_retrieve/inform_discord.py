import os
import requests
from google import genai
from dotenv import load_dotenv

from MongoDB_op import readDB_byQuery
from datetime import date

def get_lastestModel(manual=False):
    client = genai.Client()
    latest_flash_model = "gemini-3.8-flash"
    
    if(not manual):
        # 1. 動態列出所有支援 generate_content 的模型
        models = client.models.list()

        # 2. 自動過濾出包含 "flash" 且支援文字生成的最 新模型
        for m in models:
            # 找尋名稱包含 flash 的模型
            if "flash" in m.name.lower():
                latest_flash_model = m.name
                break  # 通常列表排在前面的就是最新或最推薦的版本

        print(f"自動偵測到的最新 Flash 模型：{latest_flash_model}")
    
    return latest_flash_model
        
def gen_AnalyzedRes(GEMINI_API_KEY,stockInfo):
    stock_ID=stockInfo['sid']+".TW"
    sname=stockInfo['sname']
    
    #  呼叫 Gemini API 進行分析
    client = genai.Client(api_key=GEMINI_API_KEY)

    prompt = f"""
        你是一位專業的台股分析師。請針對 {sname}({stock_ID})
        分析今天的狀況
        1. 1 個技術面/籌碼面優點
        2. 1 個潛在風險或缺點
        3. 簡短結論（50字以內）
        語氣簡明扼要，適合手機閱讀。
    """

    response = client.models.generate_content(
        model=get_lastestModel(True),
        contents=prompt
    )

    return response.text  

def discord_sendpost(DISCORD_WEBHOOK_URL, msg):
    payload = {
            "content": msg
        }
        
    res = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    if res.status_code == 204:
        print("訊息已成功發送到 Discord！")
    else:
        print("發送失敗：", res.text)


def send_info(ai_req=False):
    # 載入環境變數
    load_dotenv()
    DB_NAME = os.environ.get("DB_NAME")
    NOTICE_COLLECTION = os.environ.get("NOTICE_COLLECTION")
    #今日日期
    today = date.today()
    roc_year = today.year - 1911
    query={"announceDate": f"{roc_year}/{today.strftime('%m/%d')}"}
    
    res_list=readDB_byQuery(DB_NAME,NOTICE_COLLECTION,query)
    #print(res_list)
    for stockInfo in res_list: #抓取股票資訊
        message=""
        if (ai_req):
            GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")# gemini api
            ai_analysis=gen_AnalyzedRes(GEMINI_API_KEY,stockInfo)
            # 4. 組成 Discord 訊息並發送
            message = f"📊 **【台股分析報告】**\n\n{ai_analysis}"
        else:
            message=f"""
            【財務公告訊息】
            公告日期:{stockInfo['announceDate']}
            公司名稱:{stockInfo['sname']}
            公司代號:{stockInfo['sid']}
            EPS:{stockInfo['eps']}
            EPS公告月份:{stockInfo['notice_date']}
            """
        DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL") #webhook
        discord_sendpost(DISCORD_WEBHOOK_URL, message)
        
            
if __name__ == "__main__":            
    send_info()