import re
import os
from csv import excel
import datetime as dt
from os import remove

import pandas as pd


def clean_text(text: str):
    # 去除多余的空白字符和特殊字符
    text = re.sub(r'\s+', ' ', text)  # 替换多个空格为一个空格
    text = re.sub(r'_x000D_', '', text)  # 去除换行符编码
    text = text.strip()  # 去除首尾空白字符
    return text


def remove_duplicates(address: str):
    """去除地址中的重复部分"""
    # 这里的逻辑考虑了县、镇、区等重复的情况
    address = re.sub(r'(\w+)(\1)', r'\1', address)  # 去除连续重复的省、市、县部分
    return address


def parse_address(excel_data: pd.DataFrame, address: str):
    """解析地址并去除重复部分"""
    address_text = clean_text(address)

    # 正则表达式，提取省、市、县、镇、村等部分
    pattern = r'(\w+省)?(\w+市)?(\w+县)?(?:\w*区)?(?:\w*镇)?(?:\w*村)?'
    match = re.search(pattern, address_text)

    if match:
        # 提取匹配到的省、市、县（以及镇，如果有的话）
        address_text = "".join([match.group(1), match.group(2), match.group(3)])

        # 如果有镇（第四组），则加上镇
        if len(match.groups()) > 3 and match.group(4):  # 确保group(4)存在
            address_text += match.group(4)

        # 去重处理：避免相同部分出现多次
        address_text = remove_duplicates(address_text)

        print("地址预处理完成 : " + address_text)
        return address_text
    else:
        # 如果没有匹配到，返回原地址或空值
        print(f"未匹配到有效地址: {address}")
        return None


def process_excel(df: pd.DataFrame):
    excel_data = df.copy()

    for index, row in excel_data.iterrows():
        if pd.isnull(row['投诉地址']) or row['投诉地址'] == '无':
            excel_data.at[index, '处理后的地址'] = parse_address(excel_data, row['投诉内容'])
        else:
            excel_data.at[index, '处理后的地址'] = parse_address(excel_data, row['投诉地址'])
    return excel_data


if __name__ == '__main__':
    current_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_path, 'WorkDocument', '工单地址解析')
    if not os.path.exists(data_path):
        print('未找到指定目录，开始创建目录！')
        os.mkdir(data_path)

    file_list = os.listdir(data_path)

    # 获取当前时间并格式化
    now_time = dt.datetime.now().strftime('%Y%m%d')

    for file in file_list:
        if file.endswith('.xlsx'):
            df = pd.read_excel(os.path.join(data_path, file))
            df = process_excel(df)

            # 根据当前时间生成唯一的输出文件名
            output_file_name = f"工单地址_预处理{now_time}.xlsx"
            output_path = os.path.join(data_path, output_file_name)
            if os.path.exists(output_path):
                remove(output_path)
            # 保存处理后的文件
            df.to_excel(output_path, index=False)
            print(f"处理完成: {output_file_name}")
