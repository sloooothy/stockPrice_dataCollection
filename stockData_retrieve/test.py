import os
from pymongo import MongoClient

# 連線並寫入資料（ MongoDB 超過 60 天閒置會被刪除）

client = MongoClient(os.environ.get("MONGODB_URI"))

def write2DB_one(db_name, colleciotn_name,wr_data):
    db = client[db_name]
    collection = db[colleciotn_name]

    collection.insert_one(wr_data)
    client.close()
    print("資料寫入成功！")
    
    
test_data={"status": "success", "message": "daily sync"}
write2DB_one("t_db", "t_collect", test_data)