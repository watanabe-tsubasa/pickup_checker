from typing import Literal
import polars as pl

cap_output_cols = [
  'カンパニー名称',
  '店舗名称',
  '拠点名称',
  '締時間',
  '配送開始時間',
  '配送終了時間',
  'CAP設定',
]
gap_output_cols = [
  'カンパニー名称',
  '店舗名称',
  '拠点名称',
  '締時間',
  '配送開始時間',
  '配送終了時間',
  '次便開始時間',
]
dif_output_cols = [
  'カンパニー名称',
  '店舗名称',
  '拠点名称',
  '締時間',
  '配送開始時間',
  '配送終了時間',
  '締時間差',
]
lag_output_cols = [
  'カンパニー名称',
  '店舗名称',
  '拠点名称',
  '配送開始時間',
  '配送終了時間',
]

def start_late_finder(df: pl.DataFrame):
  df_filtered = (
    df
    .filter((pl.col('開始遅れフラグ') == 1) | (pl.col('早期終了フラグ') == 1))
    .select(lag_output_cols)
  )
  return df_filtered

# def create_sheet(df: pl.DataFrame, wb: xw.Book, sheet_name: str):
#   ws = wb.sheets.add(sheet_name)
#   ws.range('A1').value = df.to_pandas().set_index('カンパニー名称', drop=True)
  
sheet_name_type = Literal['締時間差', 'CAP設定', '配送便間隙間']
class SheetCreator:
  def __init__(self, df: pl.DataFrame):
    self.df = df
    
  def read_data(self, df: pl.DataFrame):
    self.df = df
    
  def get_data(self):
    return self.df
    
  def get_sheet(self, sheet_name: sheet_name_type):
    df_filtered = (
      self.df
      .filter(pl.col(f'{sheet_name}フラグ') == 1)
    )
    if sheet_name == '配送便間隙間':
      return df_filtered.select(gap_output_cols)
    elif sheet_name == '締時間差':
      return df_filtered.select(dif_output_cols)
    else:
      return df_filtered.select(cap_output_cols)
  
  # def create_sheet(self, wb: xw.Book, sheet_name: sheet_name_type):
  #   df = self.get_sheet(sheet_name)
  #   ws = wb.sheets.add(sheet_name)
  #   ws.range('A1').value = df.to_pandas().set_index('カンパニー名称', drop=True)