import os
import requests
from google import genai
from dotenv import load_dotenv

from MongoDB_op import readDB_byQuery


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
        
def gen_AnalyzedRes(prompt):
    #read api key
    load_dotenv()
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")# gemini api
    
    #  呼叫 Gemini API 進行分析
    client = genai.Client(api_key=GEMINI_API_KEY)

    response = client.models.generate_content(
        model=get_lastestModel(True),
        contents=prompt
    )

    return response.text  

def discord_sendpost(DISCORD_WEBHOOK_URL, msg):
    payload = {
            "content": msg
        }
    #send to discord by post
    res = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    if res.status_code == 204:
        print("訊息已成功發送到 Discord！")
    else:
        print("發送失敗：", res.text)



        