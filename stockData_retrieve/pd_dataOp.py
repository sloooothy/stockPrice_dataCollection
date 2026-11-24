from pathlib import Path
import pandas as pd

# ---  儲存為單一 JSON 檔案 ---
def save_pddata_to_json(df: pd.DataFrame,file_prefix: str, date_suffix: str, savepath="stockData/json") -> bool:
    """
    將 DataFrame 精簡且快速地匯出為單一 JSON 檔案（預設存放於 stockData/json/）。

    Args:
        df (pd.DataFrame): 要輸出的資料表；若為 None 或空表則直接回傳 False。
        file_prefix (str): 檔名前綴，例如 "daily_price" 或 "stock_info"。
        date_suffix (str): 日期或版本後綴，例如 "20261125"。

    Returns:
        bool: 寫入成功回傳 True；資料為空或發生例外回傳 False。

    Example:
        >>> ok = save_data_to_json(df_clean, "daily_price", "20261125")
        >>> if not ok:
        ...     print("JSON 匯出失敗或無資料")
    """
    if df is None or df.empty:
        return False #無資料寫回

    out_path = Path(savepath) / f"{file_prefix}_{date_suffix}.json"

    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(out_path, orient="records", force_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving JSON: {e}")
        return False




