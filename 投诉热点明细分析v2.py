import datetime as dt
import logging
from collections import Counter

import polars as pl
from tool.data.DataUtils import DataUtils
from tool.file import FileManager
from tool.decorators.ErrorHandler import error_handler
from rapidfuzz import process, fuzz


@error_handler
def process_excel(df: pl.DataFrame):

    return df, result_df


if __name__ == "__main__":
    start_time = dt.datetime.now()
    file_manager = FileManager("WorkDocument\\投诉热点明细分析")

    source_file = file_manager.get_latest_file("source")

    # 读取Excel文件
    try:
        df = file_manager.read_excel(source_file, "明细")
    except Exception as e:
        logging.error(f"无法读取Excel文件: {e}")
        exit(1)

    # 处理数据并获取结果
    processed_df, result_df = process_excel(df)

    file_manager.save_to_sheet("投诉热点明细分析",
                               原始数据=processed_df, 热点明细分析结果=result_df)

    # 如果需要获取输出路径
    print(f"输出文件路径: {file_manager.output_path}")

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
