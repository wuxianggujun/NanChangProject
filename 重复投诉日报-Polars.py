import datetime as dt
import logging
import polars as pl
from tool.data import DataUtils
from tool.file import FileManager


##我还需要排序一下，就是让重复投诉内容按照重复投诉次数大的排序。比如抚州两个号码出现投诉多次，一个3次一个2次，那先把三次排在前面
## 新增时间列
##

def extract_address_and_issue(complaint_content):
    return "", ""

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

    # 添加“系统接单时间2”列，只保留日期部分
    dataframe = dataframe.with_columns(
        pl.col("系统接单时间").dt.strftime("%Y/%m/%d %H").alias("系统接单时间2")
    )

    dataframe = DataUtils(dataframe).insert_colum("系统接单时间", "系统接单时间2")
  
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

    for row in filtered_df.iter_rows(named=True):
        matches = dataframe.filter(pl.col("区域-受理号码") == row["区域-受理号码"])
        result_list.extend(matches.to_dicts())

    result_df = pl.DataFrame(result_list)

    # 计算重复投诉次数
    repeat_counts = result_df.group_by("区域-受理号码").agg(pl.len().alias("重复投诉次数"))

    result_df = result_df.join(repeat_counts, on="区域-受理号码", how="left")

    filtered_repeat_df = result_df.filter(pl.col("重复投诉次数") >= 2)

    # 按照重复投诉次数降序排序
    filtered_repeat_df = filtered_repeat_df.sort("重复投诉次数")

    complaint_texts = []
    region_complaints = {}
    complaint_numbers = set()  # To track unique complaint numbers
    
    # 自定义区域排序
    region_order = ["南昌", "九江", "上饶", "抚州", "宜春", "吉安", "赣州", "景德镇", "萍乡", "新余", "鹰潭"]

    region_map = {region: idx for idx, region in enumerate(region_order)}

    region_complaints_data = {} # 用来存储每个区域对应的投诉数据
    
    for row in filtered_repeat_df.iter_rows(named=True):
        complaint_number = row["区域-受理号码"]
        
        if complaint_number in complaint_numbers:
            continue
        
        complaint_numbers.add(complaint_number)
        
        complaint_count = row["重复投诉次数"]
        complaint_group = result_df.filter(pl.col("区域-受理号码") == complaint_number).sort("系统接单时间")

        text_content = [f"{complaint_number} ({complaint_count}次)"]

        for complaint_row in complaint_group.iter_rows(named=True):
            complaint_date = complaint_row["系统接单时间"]
            complaint_date_str = complaint_date.strftime("%m月%d日") if complaint_date else "日期无效"
            text_content.append(f"{complaint_date_str}投诉：用户来电反映在；")

        complaint_texts.append("\n".join(text_content))
        
        # 收集每个区域的投诉数据
        region = row["区域"].replace("市", "")  # Remove "市" from the region name
        if region not in region_complaints_data:
            region_complaints_data[region] = []
        region_complaints_data[region].append({"complaint_number": complaint_number, "count": complaint_count, "text": "\n".join(text_content)})

        if region in region_map:
            region_complaints[region] = region_complaints.get(region, 0) + 1

    # 按照自定义顺序进行排序
    sorted_regions = sorted(region_complaints.items(), key=lambda x: region_map.get(x[0], float('inf')))

    region_summary = "、".join([f"{region}{count}单" for region, count in sorted_regions])

    # 排序后的文本内容
    complaint_texts_sorted = []
    for region, complaints in sorted_regions:
        # 每个区域内部按照重复投诉次数降序排序
        sorted_complaints = sorted(region_complaints_data[region], key=lambda x: x["count"], reverse=True)
        for complaint in sorted_complaints:
            complaint_texts_sorted.append(complaint["text"])

    # Concatenate all complaint texts into a single string with region summary at the top
    all_complaints_text = f"总共投诉：{region_summary}\n\n重复投诉内容如下:\n" + "\n".join(complaint_texts_sorted)

    # Create a new DataFrame for complaint text
    text_df = pl.DataFrame({"投诉信息": [all_complaints_text]})

    return dataframe, result_df, text_df


if __name__ == '__main__':
    start_time = dt.datetime.now()
    file_manager = FileManager("WorkDocument\\重复投诉日报")

    source_file = file_manager.get_latest_file("source")

    # 读取Excel文件
    try:
        df = file_manager.read_excel(source_file)
    except Exception as e:
        logging.error(f"无法读取Excel文件: {e}")
        exit(1)

    # 处理数据并获取结果
    processed_df, result_df, text_df = process_excel(df)

    file_manager.save_to_sheet("重复投诉日报", 原始数据=processed_df, 重复投诉结果=result_df,
                               重复投诉文本=text_df)

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
