import pandas as pd
import datetime as dt
import logging
import os
import re
import Utils

from pandas import DataFrame


# 定义提取地址和问题的函数
def extract_address_and_issue(complaint_content: str):
    # 定义正则表达式用于提取地址信息，假设地址以"县"、"区"、"校区"等为标识
    address_pattern = r'([\u4e00-\u9fa5]+(县|区|街道|路|大道|校区|楼|栋|单元|室))'  # 匹配中文地址
    address_matches = re.findall(address_pattern, complaint_content)

    # 如果找到多个地址，优先返回详细地址（最后一个匹配的地址）
    address = None
    if address_matches:
        address = address_matches[-1][0]  # 取最后一个匹配的地址，假设它更详细

    # 定义问题描述的关键字
    issue_keywords = ["上网慢", "卡顿", "无法上网", "信号不好", "网络问题", "无法连接"]

    # 搜索问题描述
    issue = None
    for keyword in issue_keywords:
        if keyword in complaint_content:
            issue = keyword
            break
    if not issue:
        issue = ""

    # 如果没有找到地址，返回一个空地址
    if not address:
        address = ""  # 如果没有地址，直接返回空

    return address, issue


def process_excel(df: DataFrame):
    # 对数据按“区域”列进行排序，假设“区域”列是地市
    #df = df.sort_values('区域')
    # 对数据按“区域”列和“重复投诉次数”列进行排序，确保地市排序后投诉次数高的记录在前
    df = df.sort_values(['区域', '重复投诉次数'], ascending=[True, False])
    result = {}

    # 遍历每一行数据
    for idx, row in df.iterrows():
        # complaint_number = row['区域-受理号码']
        complaint_number = row['LL']
        complaint_count = row['重复投诉次数']

        # 只处理重复投诉次数大于等于2的记录
        if complaint_count >= 2:
            # 先通过区域-受理号码进行分组，然后对每组按投诉日期排序
            complaint_group = df[df['LL'] == complaint_number]
            # complaint_group = df[df['区域-受理号码'] == complaint_number]
            complaint_group = complaint_group.sort_values('系统接单时间')  # 按时间排序

            # 初始化一个文本内容列表
            complaint_texts = [f"{complaint_number} ({complaint_count}次)"]

            # 生成投诉文本
            for _, complaint_row in complaint_group.iterrows():
                   # 确保 '系统接单时间' 是 datetime 类型，如果是字符串，则需要转为 datetime
                complaint_date = complaint_row['系统接单时间']
                if isinstance(complaint_date, str):
                    complaint_date = pd.to_datetime(complaint_date, errors='coerce')

                # 格式化日期
                if pd.notnull(complaint_date):
                    complaint_date_str = complaint_date.strftime("%m月%d日")
                else:
                    complaint_date_str = "日期无效"

                complaint_content = complaint_row['投诉内容'] if pd.notnull(complaint_row['投诉内容']) else "无内容"
                # 提取地址和问题
                address, issue = extract_address_and_issue(complaint_content)

                if address:
                    # 格式化投诉信息
                    complaint_texts.append(f"{complaint_date_str}投诉：用户来电反映在；")
                # complaint_texts.append(f"{complaint_date_str}投诉：用户来电表示在{address}{issue}；")
                else:
                    complaint_texts.append(f"{complaint_date_str}投诉：用户来电反映；")
                    # complaint_texts.append(f"{complaint_date_str}投诉：用户来电表示{issue}；")
            # 将投诉文本加入到结果字典
            result[complaint_number] = '\n'.join(complaint_texts)

    return result


if __name__ == '__main__':
    start_time = dt.datetime.now()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    current_path = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.join(current_path, "WorkDocument", "重复投诉日报")

    if not os.path.exists(source_dir):
        logging.error("未找到指定目录，请检查路径是否正确！")
        exit(1)

    file_path = Utils.get_first_excel_file_in_dir(source_dir)

    logging.info(f"正在处理文件：{file_path}")

    df = Utils.read_excel_sheet(file_path, sheet_name="Sheet2")

    # 检查df是否为None
    if df is None:
        logging.error(f"读取Excel文档失败: {file_path}")
        exit(1)

    # 处理数据并获取结果
    processed_df = process_excel(df)

    print(processed_df)
    # 保存处理后的数据到文件
    data_str = start_time.strftime("%Y%m%d")

    # 输出到文本文件
    output_file_path = os.path.join(source_dir, "投诉结果.txt")
    with open(output_file_path, 'w', encoding='utf-8') as f:
        for complaint_number, complaint_text in processed_df.items():
            f.write(f"{complaint_text}\n")  # 每个投诉信息之间添加空行

    logging.info(f"处理结果已保存到: {output_file_path}")

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    # logging.info(f'运行时间：{runtime}')
