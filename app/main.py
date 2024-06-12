from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import polars as pl
import pandas as pd
from app.modules.data_setter import set_base_data, set_flag, set_lag_flag, sorter
from app.modules.data_getter import sheet_name_type, SheetCreator, start_late_finder
from typing import List
import os
import tempfile

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static", html=True), name="static")

@app.get("/")
def read_root():
  return {"Hello": "World!"}

@app.post("/process_csv/")
async def process_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV files are allowed.")
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file_path = temp_file.name
        temp_file.write(file.file.read())

    try:
        # Process the CSV file
        lf = pl.scan_csv(temp_file_path)
        lf_selected = lf.select([
            '日付',
            'カンパニー名称',
            '店舗名称',
            '締時間',
            '配送開始時間',
            '配送終了時間',
            '拠点名称',
            'CAP設定',
        ])

        date = lf.select('日付').unique().collect().to_series().sort().to_list()[0]
        df = set_base_data(lf_selected, date)
        df_flagged = set_flag(df)
        df_lag_flagged = set_lag_flag(df_flagged)

        flag_list: List[sheet_name_type] = ['締時間差', 'CAP設定', '配送便間隙間']
        sc = SheetCreator(sorter(df_flagged))

        # Create a temporary output file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as output_file:
          output_file_path = output_file.name

        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
          start_late_finder(sorter(df_lag_flagged)).to_pandas().to_excel(writer, sheet_name='開始終了時間', index=False)
          for flag in flag_list:
            sheet_data = sc.get_sheet(flag)
            sheet_data.to_pandas().to_excel(writer, sheet_name=flag, index=False)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up the temporary CSV file
        os.remove(temp_file_path)
    
    return FileResponse(output_file_path, filename="pick_up便設定状況.xlsx")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
