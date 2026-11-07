import os
from pymongo import MongoClient

# 連線並寫入資料（ MongoDB 超過 60 天閒置會被刪除）

client = MongoClient(os.environ.get("MONGODB_URI"))

def write2DB_one(db_name, colleciotn_name,wr_data):
    db = client[db_name]
    collection = db[colleciotn_name]

    result=collection.insert_one(wr_data)
    # debug 透過 inserted_id 嚴格確認是否寫入成功
    if result.inserted_id:
        print(f"資料寫入成功！ MongoDB 產生的 ID: {result.inserted_id}")
    else:
        print("寫入可能失敗，未取得 inserted_id。")

    # 5. 關閉連線
    client.close()
    
    
test_data={"status": "success", "message": "daily sync"}
write2DB_one("t_db", "t_collect", test_data)