from idlelib.iomenu import errors

import polars as pl
import numpy as np
import os
import logging
import datetime as dt
import Utils


def process_excel(df: pl.DataFrame) -> pl.DataFrame:
    df = df.drop(['序号', '工单号']).unique(subset='客服流水号')
    return df

if __name__ == '__main__':
    
    start_time = dt.datetime.now()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    current_path = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.join(current_path, "WorkDocument", "重复投诉日报", "source")

    if not os.path.exists(source_dir):
        logging.error("未找到指定目录，请检查路径是否正确！")
        exit(1)

    file_path = Utils.get_first_excel_file_in_dir(source_dir)

    data_excel = pl.read_excel(file_path,sheet_name='sheet',engine='calamine') 
    
    data_excel = process_excel(data_excel)
    
    logging.info(f"正在处理文件：{file_path}")

    # 保存处理后的数据到文件
    data_str = start_time.strftime("%Y%m%d")
    output_filename = f'重复投诉日报{data_str}{os.path.splitext(file_path)[1]}'
    output_path = os.path.join(current_path, "WorkDocument", "重复投诉日报", output_filename)

    data_excel.write_excel(output_path,autofit=True)

    #效率最高
    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
    

    