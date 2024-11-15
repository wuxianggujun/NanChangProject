import os
import logging
import datetime as dt
import Utils
import pandas as pd


def count_occurrences_in_last_month(dataframe: pd.DataFrame, target_dataframe: pd.DataFrame):
    # 先去除客服流水号的重复项，保留首次出现的记录
    unique_dataframe = dataframe.drop_duplicates(subset=['客服流水号'])
    # 计算过去一个月内每个 '区域-受理号码' 的出现次数
    monthly_counts = unique_dataframe.groupby('区域-受理号码').size().reset_index(name='重复投诉次数')

    target_occurrences = target_dataframe.merge(monthly_counts, on='区域-受理号码', how='left')
    target_occurrences['重复投诉次数'] = target_occurrences['重复投诉次数'].fillna(0).astype(int)
    return target_occurrences


def count_repeats_by_city(de_result):
    repeat_counts = de_result.groupby('区域-受理号码').size().reset_index(name='重复投诉次数')
    return repeat_counts


def generate_summary_table(dataframe: pd.DataFrame):
    # 初始化 summary DataFrame，列为所需字段
    summary_table = pd.DataFrame(columns=[
        '区域', '重复2次', '重复3次', '重复4次及以上', '当日新增重复投诉总计'
    ])

    # 统计每个区域的重复投诉情况
    rows = []  # 使用一个列表来存储每个区域的汇总行
    for region, group in dataframe.groupby('区域'):
        # 计算重复投诉次数
        repeat_counts = group['重复投诉次数'].value_counts()

        # 统计重复2次、3次、4次及以上的数量
        repeat_2 = repeat_counts.get(2, 0)
        repeat_3 = repeat_counts.get(3, 0)
        repeat_4_plus = sum(repeat_counts.get(i, 0) for i in range(4, max(repeat_counts.index)+1))

        # 计算当日新增重复投诉总计
        total_complaints = repeat_2 + repeat_3 + repeat_4_plus

        # 将结果添加到 rows 列表
        rows.append({
            '区域': region,
            '重复2次': repeat_2,
            '重复3次': repeat_3,
            '重复4次及以上': repeat_4_plus,
            '当日新增重复投诉总计': total_complaints
        })

    # 创建汇总表格
    summary_table = pd.DataFrame(rows)

    # 添加汇总行
    summary_row = {
        '区域': '总计',
        '重复2次': summary_table['重复2次'].sum(),
        '重复3次': summary_table['重复3次'].sum(),
        '重复4次及以上': summary_table['重复4次及以上'].sum(),
        '当日新增重复投诉总计': summary_table['当日新增重复投诉总计'].sum()
    }
    summary_table = pd.concat([summary_table, pd.DataFrame([summary_row])], ignore_index=True)

    return summary_table


def process_excel(df: pd.DataFrame):
    global column_index
    dataframe = df.copy()

    # 先通过时间筛选过滤出30天，为期一个月的数据
    dataframe['系统接单时间'] = pd.to_datetime(dataframe['系统接单时间'], errors='coerce')
    now_time = dt.datetime.now()
    start_time = (now_time - dt.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = now_time.replace(hour=16, minute=0, second=0, microsecond=0)

    # 确保 '系统接单时间' 已被成功转换为 datetime
    logging.info(f"系统接单时间数据类型：{dataframe['系统接单时间'].dtype}")
    # 过滤数据
    dataframe = dataframe[(dataframe['系统接单时间'] >= start_time) & (dataframe['系统接单时间'] <= end_time)]

    logging.info(f"开始时间：{start_time}")
    logging.info(f"结束时间：{end_time}")

    # 删除序号和工单号
    dataframe.drop(columns=['序号', '工单号'], inplace=True, errors='ignore')

    # 使用 .loc 处理去重
    dataframe = dataframe.loc[dataframe.drop_duplicates(subset=['客服流水号'], keep='first').index]

    # 再次复制系统时间一列到右侧并保留天数
    dataframe.loc[:, '系统接单时间2'] = dataframe['系统接单时间'].dt.strftime('%Y/%m/%d %H')  # 格式化为 'YYYY/MM/DD HH'
    dataframe.insert(dataframe.columns.get_loc('系统接单时间') + 1, '系统接单时间2', dataframe.pop('系统接单时间2'))

    if '口碑未达情况原因' in dataframe.columns:
        column_index = dataframe.columns.get_loc('口碑未达情况原因')
        dataframe = dataframe.iloc[:, :column_index + 1]
        logging.info("删除“口碑未达情况原因”左侧一列")

    if '受理号码' in dataframe.columns and '区域' in dataframe.columns:
        dataframe['区域-受理号码'] = dataframe['区域'] + '-' + dataframe['受理号码'].astype(str)
        dataframe.insert(column_index + 1, '区域-受理号码', dataframe.pop('区域-受理号码'))

    # 添加筛选条件：今天下午四点到昨天下午四点的数据
    yesterday_end = (now_time - dt.timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    today_start = now_time.replace(hour=16, minute=0, second=0, microsecond=0)

    target_dataframe = dataframe[
        (dataframe['系统接单时间'] >= yesterday_end) & (dataframe['系统接单时间'] <= today_start)]

    target_dataframe = target_dataframe.drop_duplicates(subset=['区域-受理号码'])

    # complaint_counts = dataframe['区域-受理号码'].value_counts().to_dict()
    # target_dataframe['重复投诉次数'] = target_dataframe['区域-受理号码'].map(complaint_counts).fillna(0).astype(int)

    monthly_occurrences: pd.DataFrame = count_occurrences_in_last_month(dataframe,target_dataframe)
    print(monthly_occurrences )

    # 返回处理后的 DataFrame 和带有重复投诉次数的 DataFrame
    return dataframe, target_dataframe,monthly_occurrences 


if __name__ == '__main__':
    start_time = dt.datetime.now()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    current_path = os.path.dirname(os.path.abspath(__file__))
    source_dir = os.path.join(current_path, "WorkDocument", "重复投诉日报", "source")

    if not os.path.exists(source_dir):
        logging.error("未找到指定目录，请检查路径是否正确！")
        exit(1)

    file_path = Utils.get_first_excel_file_in_dir(source_dir)

    logging.info(f"正在处理文件：{file_path}")

    df = Utils.read_excel(file_path)

    # 处理数据并获取结果
    processed_df, result_df,monthly_occurrences  = process_excel(df)

    # 保存处理后的数据到文件
    data_str = start_time.strftime("%Y%m%d")
    output_filename = f'重复投诉日报{data_str}{os.path.splitext(file_path)[1]}'
    output_path = os.path.join(current_path, "WorkDocument", "重复投诉日报", output_filename)

    # 保存原始数据
    Utils.save_to_excel(processed_df, output_path)

    summary_table = generate_summary_table(monthly_occurrences)

    # 保存结果到新 sheet
    with pd.ExcelWriter(output_path, engine='openpyxl', mode='a') as writer:
        result_df.to_excel(writer, sheet_name='重复投诉统计', index=False)
        summary_table.to_excel(writer, sheet_name='区域重复投诉统计', index=False)
        monthly_occurrences.to_excel(writer, sheet_name='区域-受理号码出现次数', index=False)

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
