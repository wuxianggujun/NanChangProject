import pandas as pd
import datetime as dt
import logging
import os
from pandas import DataFrame
import Utils

def process_excel(df: pd.DataFrame, csv_data: DataFrame):
    print("DataFrame Data Columns:", csv_data.columns)
    print("CSV Data Columns:", csv_data.columns)
    excel_data = df.copy()
    excel_data['客服流水号'] = excel_data['客服流水号'].astype(str)
    csv_data['客服流水号'] = csv_data['客服流水号'].astype(str)

    # 定义需要从 CSV 中导入的列
    update_columns = ['确认经度', '确认纬度', '唯一ID', 'ID', '地市', '区县', '场景名称', '一级场景',
                      '二级场景', '中心经度', '中心纬度']

    # 使用 merge 来将 csv_data 中相应的列加入到 excel_data 中，基于 '客服流水号'
    merged_data = pd.merge(excel_data, csv_data[['客服流水号'] + update_columns], on='客服流水号', how='left')

    print("Merged Data Columns:", merged_data.columns)

    # 更新 excel_data 中的相应列
    for col in update_columns:
        # 合并后的列名
        merged_col = col + '_y'  # 默认为 CSV 数据的列，带有后缀 '_y'
        print(f"合并后的列名 {merged_col}")
        # 填充 Excel 数据对应列
        excel_data[col] = merged_data[merged_col]

    print(excel_data[update_columns].head())  # 打印更新后的数据，检查是否正确填充
    return excel_data


if __name__ == '__main__':
    start_time = dt.datetime.now()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    current_path = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.join(current_path, "WorkDocument", "重复投诉解决跟踪", "source")

    if not os.path.exists(source_dir):
        logging.error("未找到指定目录，请检查路径是否正确！")
        exit(1)

    file_path = Utils.get_first_excel_file_in_dir(source_dir)

    logging.info(f"正在处理文件：{file_path}")

    df = Utils.read_excel_sheet(file_path, sheet_name="明细")

    # 检查df是否为None
    if df is None:
        logging.error(f"读取Excel文档失败: {file_path}")
        exit(1)

    csv_file_path = os.path.join(source_dir, "Query.csv")
    logging.info(f"正在处理文件：{csv_file_path}")
    csv_data = pd.read_csv(csv_file_path,encoding='gbk')

    # 处理数据并获取结果
    processed_df = process_excel(df, csv_data)

    # 保存处理后的数据到文件
    data_str = start_time.strftime("%Y%m%d")
    output_filename = f'重复投诉解决跟踪{data_str}{os.path.splitext(file_path)[1]}'
    output_path = os.path.join(current_path, "WorkDocument", "重复投诉解决跟踪", output_filename)

    # 保存原始数据
    Utils.save_to_excel(processed_df, output_path)
  
    # 文件存在，使用追加模式
    with pd.ExcelWriter(output_path, engine='openpyxl', mode='a',if_sheet_exists='replace') as writer:
            processed_df.to_excel(writer, sheet_name="处理结果", index=False)

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
