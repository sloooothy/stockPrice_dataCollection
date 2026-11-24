import os
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import pandas as pd

def _initDB():
    try:
        load_dotenv()  # 這行一定要在讀取環境變數之前執行！
        client = MongoClient(os.environ.get("MONGODB_URI"))
        return client
          
    except ServerSelectionTimeoutError:
         print("🛑 錯誤：ServerSelectionTimeoutError ")

# 寫入單筆資料 (line by line)
def write2DB_one(db_name, colleciotn_name,wr_data):
    db_client=_initDB()
    try:
        db = db_client[db_name]
        collection = db[colleciotn_name]  
        
        result=collection.insert_one(wr_data)
        # debug 透過 inserted_id 嚴格確認是否寫入成功
        if result.inserted_id:
            print(f"資料寫入成功！ MongoDB 產生的 ID: {result.inserted_id}")
        else:
            print("寫入可能失敗，未取得 inserted_id。")
    finally:
        # 關閉連線
        db_client.close()
        return True
        

# by query upsert    
def upsertDB_byQuery(db_name, colleciotn_name, wr_data, filter_query):
    db_client=_initDB()
    try:
        db = db_client[db_name]
        collection = db[colleciotn_name]  
        
        result=collection.update_one(filter_query, wr_data, upsert=True)
        # debug 透過 inserted_id 嚴格確認是否寫入成功
        # 判斷寫入結果
        '''
        if result.upserted_id:
            # 狀況 A：資料庫原本沒有，這是全新插入的
            print(f"✨ [新增成功] 建立了新文件，MongoDB ID: {result.upserted_id}")

        elif result.modified_count > 0:
            # 狀況 B：資料本來就存在，且內容有被更新
            print(f"🔄 [更新成功] 找到了舊資料，已成功更新內容（異動數: {result.modified_count}）")

        elif result.matched_count > 0:
            # 狀況 C：資料本來就存在，但你帶入的新資料跟舊資料「一模一樣」，所以沒有實際去改動它
            print(f"💤 [資料無異動] 找到相同資料，但內容無變化，略過更新。")

        else:
            # 狀況 D：理論上用了 upsert=True 不太會走到這裡，但以防萬一
            print("⚠️ 寫入狀態未知，請檢查條件。")
        '''
    finally:
        # 關閉連線
        db_client.close()
        return True
        
# DB 讀出
def readDB_byQuery(db_name, colleciotn_name, query_filter={}):
    results_list=[]
    db_client=_initDB()
    try:
        db = db_client[db_name]
        collection = db[colleciotn_name]  
        # 檢查符合條件的檔案數量
        count = collection.count_documents(query_filter)

        if count > 0:
            print(f"✅ 找到 {count} 筆資料！")
            # 再去執行 find() 把資料抓下來
            cursor = collection.find(query_filter)
            results_list = list(cursor)
        else:
            print("⚠️ 找不到任何資料。")

    finally:
        # 關閉連線
        db_client.close()
    
    return results_list
    
# DB 寫入pd frame
def writeDB_byPD(
    db_name,
    colleciotn_name,
    pdframe: pd.DataFrame,
    indexlist: list = None,
    chunk_size: int = 1000,
) -> bool:
    
    db_client = _initDB()

    try:
        if pdframe is None or pdframe.empty:
            print(f"輸入資料為空，並未寫入任何資料")
            return False

        # 若未指定，預設 sid 作為更新準則
        if not indexlist:
            indexlist = ["sid"]

        # 確認指定的欄位都存在於 DataFrame 中
        missing_cols = [col for col in indexlist if col not in pdframe.columns]
        if missing_cols:
            raise ValueError(f"DataFrame 缺少指定的 index 欄位: {missing_cols}")

        db = db_client[db_name]
        collection = db[colleciotn_name]

        records = pdframe.to_dict(orient="records")
        operations = []

        for row in records:
            # 依 indexlist 動態組裝 filter 條件
            filter_query = {}
            for col in indexlist:
                val = row[col]
                # 如果是 sid，順手去除前後空白並確保字串型別，避免比對落差
                if col == "sid" and val is not None:
                    val = str(val).strip()
                filter_query[col] = val

            operations.append(
                UpdateOne(
                    filter=filter_query,
                    update={"$set": row},
                    upsert=True,
                )
            )

        for i in range(0, len(operations), chunk_size):
            batch = operations[i : i + chunk_size]
            collection.bulk_write(batch, ordered=False)

        return True

    except Exception as e:
        print(f"寫入 DB 失敗 [{db_name}.{colleciotn_name}]: {e}")
        return False

    finally:
        if db_client is not None:
            db_client.close()
    
'''
# DB write in test
test_data={"status": "success", "message": "daily sync"}
write2DB_one("t_db", "t_collect", test_data)
'''