import datetime as dt
import pandas as pd
import logging
import os
import re


def clean_complaint_content(df, column_name):
    if column_name in df.columns:
        df[column_name] = df[column_name].apply(lambda x: str(x).replace('_x000d_', '') if isinstance(x, str) else x)
    else:
        logging.warning(f"列 '{column_name}' 不存在于DataFrame中。")


def get_first_excel_file_in_dir(directory: str):
    """检查目录是否存在并返回第一个Excel文件的路径"""
    if not os.path.exists(directory):
        logging.error(f"未找到指定目录: {directory}")
    excel_file = next((f for f in os.listdir(directory) if f.endswith((".xlsx", ".xls"))), None)
    if excel_file is None:
        logging.error("未找到 Excel 文件，请检查目录中是否存在 Excel 文件。")
        exit(1)
    return os.path.join(directory, excel_file)


def read_excel(file_path):
    if not os.path.exists(file_path):
        logging.error(f"文件路径不存在: {file_path}")
        return None
    try:
        df = pd.read_excel(file_path)
        logging.info("读取Excel文档成功")
        return df 
    except Exception as e:
        logging.error(f"读取Excel文档失败: {e}")
        return None

def read_excel_sheet(file_path, sheet_name=None):
    if not os.path.exists(file_path):
        logging.error(f"文件路径不存在: {file_path}")
        return None
    try:
        df = pd.read_excel(file_path, sheet_name)
        logging.info("读取Excel文档成功")
        return df 
    except Exception as e:
        logging.error(f"读取Excel文档失败: {e}")
        return None


def process_dataframe(df: pd.DataFrame, days: int = 1):
    df = df.copy()  # 确保使用深拷贝
    # df.dropna(how="all", axis=1, inplace=True)

    # 确保“系统接单时间”是日期格式
    df['系统接单时间'] = pd.to_datetime(df['系统接单时间'], errors='coerce')

    now = dt.datetime.now()
    start_time = (now - dt.timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)

    filtered_df = df[(df['系统接单时间'] >= start_time) & (df['系统接单时间'] <= end_time)]

    # 处理“系统接单时间”列
    filtered_df.loc[:, '系统接单时间2'] = filtered_df['系统接单时间'].dt.date
    filtered_df.insert(filtered_df.columns.get_loc('系统接单时间') + 1, '系统接单时间2',
                       filtered_df.pop('系统接单时间2'))

    # 删除无用列
    filtered_df.drop(columns='序号', inplace=True, errors='ignore')

    # 使用 .loc 处理去重
    filtered_df = filtered_df.loc[filtered_df.drop_duplicates(subset=['客服流水号'], keep='first').index]

    logging.info(f"受理路径唯一值: {filtered_df['受理路径'].unique()}")

    # 筛选特定的受理路径
    path_conditions = (
            filtered_df['受理路径'].str.contains("投诉工单（2021版）>>移网>>【网络使用】移网语音", na=False) |
            filtered_df['受理路径'].str.contains("投诉工单（2021版）>>融合>>【网络使用】移网语音", na=False)
    )
    filtered_df = filtered_df[path_conditions]

    # 清空“口碑未达情况原因”右侧的所有列
    if '区域' in filtered_df.columns:
        # 清除“区域”列中的“市”字
        filtered_df['区域'] = filtered_df['区域'].apply(lambda x: re.sub(r'市$', '', str(x)))

        # 先清空“口碑未达情况原因”右侧的数据
        temp_index = filtered_df.columns.get_loc('口碑未达情况原因') + 1
        right_columns = filtered_df.columns[temp_index:]
        filtered_df.drop(columns=right_columns, inplace=True)

        logging.info("删除右侧列")

        # 对“区域”列进行排序并添加为新列“区域_复制”
        filtered_df.sort_values(by=['区域'], ignore_index=True, inplace=True)
        filtered_df['区域2'] = filtered_df['区域']

        # 替换“口碑未达情况原因”为排序后的区域
        # filtered_df['口碑未达情况原因'] = filtered_df['区域']

        logging.info("处理口碑未达情况原因替换为区域并删除右侧列!")
    else:
        logging.warning("没有找到区域列，请检查输入文档的时间数据")

    if filtered_df.empty:
        logging.warning("筛选后的数据为空,请检查输入文档的时间数据")

    return filtered_df


def remove_duplicate_columns(df: pd.DataFrame):
    duplicated_columns = df.columns[df.columns.duplicated()].tolist()
    if duplicated_columns:
        logging.warning(f"存在重复的列名：{duplicated_columns}")

    # 去除.1 后缀的重复列
    # df = df.loc[:, ~df.columns.duplicated()]

    # 如果只想去除列名中的 .1 等后缀
    df.columns = [col.split('.')[0] for col in df.columns]
    return df


def save_to_excel(df: pd.DataFrame, output_path):
    try:
        if os.path.exists(output_path):
            logging.warning(f"输出路径已存在，将删除并重新创建文件: {output_path}")
            os.remove(output_path)

        # 清除_x000d_
        clean_complaint_content(df, '投诉内容')
        df = remove_duplicate_columns(df)
        df.to_excel(output_path, index=False)
        logging.info(f"保存处理后的数据成功 {output_path}")
    except Exception as e:
        logging.error(f"保存处理后的数据失败: {e}")
