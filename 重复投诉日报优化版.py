import logging
import pandas as pd
import datetime as dt
from tool.file import FileManager
from tqdm import tqdm

def count_occurrences_in_last_month(dataframe: pd.DataFrame, target_dataframe: pd.DataFrame):
    # 先去除客服流水号的重复项，保留首次出现的记录
    unique_dataframe = dataframe.drop_duplicates(subset=['客服流水号'])
    # 计算过去一个月内每个 '区域-受理号码' 的出现次数
    monthly_counts = unique_dataframe.groupby('区域-受理号码').size().reset_index(name='重复投诉次数')

    target_occurrences = target_dataframe.merge(monthly_counts, on='区域-受理号码', how='left')
    target_occurrences['重复投诉次数'] = target_occurrences['重复投诉次数'].fillna(0).astype(int)
    return target_occurrences

def process_excel(df: pd.DataFrame):
    global column_index
    dataframe: pd.DataFrame = df.copy()

    # 先通过时间筛选过滤出30天，为期一个月的数据
    dataframe['系统接单时间'] = pd.to_datetime(dataframe['系统接单时间'], errors='coerce')
    now_time = dt.datetime.now()
    start_time = (now_time - dt.timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = now_time.replace(hour=16, minute=0, second=0, microsecond=0)

    # 过滤数据
    dataframe = dataframe[(dataframe['系统接单时间'] >= start_time) & (dataframe['系统接单时间'] <= end_time)]

    logging.info(f"开始处理：{start_time} 到 {end_time} 共三十天的数据...")

    # 删除序号和工单号
    dataframe.drop(columns=['序号', '工单号'], inplace=True, errors='ignore')

    # 使用 .loc 处理通过客服流水号进行去重
    dataframe = dataframe.loc[dataframe.drop_duplicates(subset=['客服流水号'], keep='first').index]

    # 再次复制系统时间一列到右侧并保留天数
    dataframe.loc[:, '系统接单时间2'] = dataframe['系统接单时间'].dt.strftime('%Y/%m/%d %H')  # 格式化为 'YYYY/MM/DD HH'
    dataframe.insert(dataframe.columns.get_loc('系统接单时间') + 1, '系统接单时间2', dataframe.pop('系统接单时间2'))

    if '口碑未达情况原因' in dataframe.columns:
        column_index = dataframe.columns.get_loc('口碑未达情况原因')
        dataframe = dataframe.iloc[:, :column_index + 1]
        logging.info("删除“口碑未达情况原因”左侧数据")

    # 生成‘区域-受理号码’列
    if '受理号码' in dataframe.columns and '区域' in dataframe.columns:
        dataframe['区域-受理号码'] = dataframe['区域'] + '-' + dataframe['受理号码'].astype(str)
        dataframe.insert(column_index + 1, '区域-受理号码', dataframe.pop('区域-受理号码'))


    # 添加筛选条件：今天下午四点到昨天下午四点的数据
    yesterday_end = (now_time - dt.timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    today_start = now_time.replace(hour=16, minute=0, second=0, microsecond=0)

    # 筛选出时间段内的重复工单
    filtered_df = dataframe[(dataframe['系统接单时间'] >= yesterday_end) & (dataframe['系统接单时间'] <= today_start)]
    
    result_list = []
    
    for _,row in tqdm(filtered_df.iterrows(),desc="遍历并找出重复数据",total=filtered_df.shape[0]):
        for _,column in dataframe.iterrows():
            if row['区域-受理号码'] == column['区域-受理号码']:
                result_list.append(column)
            
    
    result_df = pd.DataFrame(result_list)
    
    repeat_counts = result_df['区域-受理号码'].value_counts().to_dict()
    result_df['重复投诉次数'] = result_df['区域-受理号码'].map(repeat_counts)
    
    return dataframe, filtered_df, result_df


if __name__ == '__main__':
    start_time = dt.datetime.now()
    file_manager = FileManager("WorkDocument\\重复投诉日报")

    source_file = file_manager.get_latest_file("source")

    df: pd.DataFrame = file_manager.read_excel(source_file)

    # 处理数据并获取结果
    processed_df,filtered_df, result_df = process_excel(df)

    # 保存结果到新 sheet
    file_manager.save_to_sheet("重复投诉日报", 原始数据=processed_df, 时间分组数据=filtered_df, 处理后数据=result_df)

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
