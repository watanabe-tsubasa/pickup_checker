import datetime
import polars as pl

def set_base_data(df: pl.DataFrame, date: str):
  df_result = (
    df
    .drop_nulls(subset='拠点名称')
    .with_columns(
      pl.col('カンパニー名称').str.replace('カンパニー', '').alias('カンパニー名称'),
      (pl.col('日付') + ' ' + pl.col('配送開始時間')).alias('配送開始時間'),
      (pl.col('日付') + ' ' + pl.col('配送終了時間')).alias('配送終了時間')
    )
    .with_columns(
      pl.col('配送開始時間').str.strptime(dtype=pl.Datetime),
      pl.col('配送終了時間').str.strptime(dtype=pl.Datetime),
      pl.duration(minutes=pl.col('締時間')).alias('締時間')
    )
    .with_columns(
      (pl.col('配送終了時間') - pl.col('締時間')).alias('締時間')
    )
    .with_columns(
      (pl.col('配送開始時間') - pl.col('締時間')).alias('締時間差')
    )
    .with_columns(
      pl.when(pl.col('締時間差') > pl.duration(hours=3))
      .then(1)
      .otherwise(0)
      .alias('締時間差フラグ'),
      pl.when(pl.col('CAP設定') < 5)
      .then(1)
      .otherwise(0)
      .alias('CAP設定フラグ'),
    )
    .sort(by=['カンパニー名称', '店舗名称','拠点名称',  '配送開始時間'])
    .filter(pl.col('日付') == date)
  )
  return df_result

def format_duration(duration):
  if isinstance(duration, datetime.timedelta):
      total_seconds = int(duration.total_seconds())
      hours, remainder = divmod(total_seconds, 3600)
      minutes, _ = divmod(remainder, 60)
      return f"{int(hours):02d}:{int(minutes):02d}"
  return None

def set_flag(df: pl.DataFrame):
  df_flagged = (
    pl.concat(
      [df, (df
        .select('配送開始時間', '拠点名称')[1:]
        .with_columns(
          pl.col('配送開始時間').alias('次便開始時間'),
          pl.col('拠点名称').alias('後拠点名称'),
        )
        .drop(['配送開始時間', '拠点名称'])
        )
      ],
      how='horizontal'
    )
    .with_columns(
      pl.when((pl.col('拠点名称') == pl.col('後拠点名称')) & ((pl.col('次便開始時間') - pl.col('配送終了時間')) > pl.duration(minutes=10)))
        .then(1)
        .otherwise(0)
        .alias('配送便間隙間フラグ')
    )
    .with_columns(
      pl.col('締時間差').map_elements(format_duration, return_dtype=pl.Utf8)
    )
  )
  return df_flagged

def set_lag_flag(df: pl.DataFrame):
  df_lag_flagged = (
    df
    .group_by([
      '日付',
      'カンパニー名称',
      '店舗名称',
      '拠点名称'
    ])
    .agg([
      pl.min('配送開始時間'),
      pl.max('配送終了時間')
    ])
    .with_columns(
      pl.lit('10:00:00').alias('予定開始時間'),
      pl.lit('20:00:00').alias('予定終了時間'),
    )
    .with_columns(
      (pl.col('日付') + ' ' + pl.col('予定開始時間')).alias('予定開始時間').str.strptime(dtype=pl.Datetime),
      (pl.col('日付') + ' ' + pl.col('予定終了時間')).alias('予定終了時間').str.strptime(dtype=pl.Datetime),
    )
    .with_columns(
      pl.when((pl.col('配送開始時間') - pl.col('予定開始時間')) > pl.duration(minutes=10))
        .then(1)
        .otherwise(0)
        .alias('開始遅れフラグ'),
      pl.when((pl.col('予定終了時間') - pl.col('配送終了時間')) > pl.duration(minutes=10))
        .then(1)
        .otherwise(0)
        .alias('早期終了フラグ'),
    )
  )
  return df_lag_flagged

def sorter(df: pl.DataFrame):
  df_temp = (
    df
    .with_columns(
      pl
      .when(pl.col('カンパニー名称') == '北関東').then(0)
      .when(pl.col('カンパニー名称') == '南関東').then(1)
      .when(pl.col('カンパニー名称') == '北陸信越').then(2)
      .when(pl.col('カンパニー名称') == '東海').then(3)
      .when(pl.col('カンパニー名称') == '近畿').then(4)
      .otherwise(5)
      .alias('company_sorter')
    )
    .sort(['company_sorter', '店舗名称', '拠点名称', '配送開始時間'])
    .drop('company_sorter')
  )
  return df_temp