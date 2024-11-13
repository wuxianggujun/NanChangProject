import logging
import polars as pl
import datetime as dt
from tool.file import FileManager
from tqdm import tqdm


def process_excel(df: pl.DataFrame) -> tuple:
    now_time = dt.datetime.now()
    start_time = (now_time - dt.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = now_time.replace(hour=16, minute=0, second=0, microsecond=0)

    # 过滤数据
    dataframe = df.with_columns(pl.col("系统接单时间").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
    dataframe = dataframe.filter(
        (pl.col("系统接单时间") >= start_time) & (pl.col("系统接单时间") <= end_time)
    )
    logging.info(f"开始处理：{start_time} 到 {end_time} 共三十天的数据...")

    # 删除序号和工单号
    dataframe = dataframe.drop(["序号", "工单号"])

    # 通过客服流水号进行去重
    dataframe = dataframe.unique(subset=["客服流水号"])

    dataframe = dataframe.with_columns(
        pl.col("系统接单时间").dt.strftime("%Y/%m/%d %H").alias("系统接单时间2")
    )

    if '口碑未达情况原因' in dataframe.columns:
        column_index = dataframe.columns.index('口碑未达情况原因')
        dataframe = dataframe.select(dataframe.columns[:column_index + 1])
        logging.info("删除“口碑未达情况原因”左侧数据")

    if all(col in dataframe.columns for col in ["区域", "受理号码"]):
        dataframe = dataframe.with_columns(
            (pl.col("区域") + "-" + pl.col("受理号码").cast(pl.Utf8)).alias("区域-受理号码"))

    # 添加筛选条件：今天下午四点到昨天下午四点的数据
    yesterday_end = (now_time - dt.timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    today_start = now_time.replace(hour=16, minute=0, second=0, microsecond=0)

    # 筛选出时间段内的重复工单
    filtered_df = dataframe.filter(
        (pl.col("系统接单时间") >= yesterday_end) & (pl.col("系统接单时间") <= today_start)
    )

    result_list = []

    for row in tqdm(filtered_df.iter_rows(named=True), desc="匹配区域-受理号码"):
        matches = dataframe.filter(pl.col("区域-受理号码") == row["区域-受理号码"])
        result_list.extend(matches.to_dicts())

    result_df = pl.DataFrame(result_list)

    # 计算重复投诉次数
    repeat_counts = result_df.group_by("区域-受理号码").agg(pl.len().alias("重复投诉次数"))

    result_df = result_df.join(repeat_counts, on="区域-受理号码", how="left")

    return dataframe, filtered_df, result_df


if __name__ == '__main__':
    start_time = dt.datetime.now()
    file_manager = FileManager("WorkDocument\\重复投诉日报")

    source_file = file_manager.get_latest_file("source")

    # 读取Excel文件
    try:
        df = pl.read_excel(source_file)
    except Exception as e:
        logging.error(f"无法读取Excel文件: {e}")
        exit(1)

    # 处理数据并获取结果
    processed_df, filtered_df, result_df = process_excel(df)


    processed_df.write_excel("processed_data.xlsx",autofilter=False)
   
    # 保存处理后的数据到文件
    processed_df.write_csv("processed_data.csv")
    filtered_df.write_csv("filtered_data.csv")
    result_df.write_csv("result_data.csv")
    
    
    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
