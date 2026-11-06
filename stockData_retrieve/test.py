import os
from pymongo import MongoClient

# 連線並寫入資料（可防止 MongoDB 超過 60 天閒置被刪除）
client = MongoClient(os.environ.get("MONGODB_URI"))
db = client["test_database"]
collection = db["test_collection"]

collection.insert_one({"status": "success", "message": "daily sync"})
client.close()
print("資料寫入成功！")