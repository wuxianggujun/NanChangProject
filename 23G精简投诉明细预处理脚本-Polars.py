import logging
import datetime as dt
import os
import polars as pl
from tool.file import FileManager
from tool.data import DataUtils
import re

def process_excel(excel_data: pl.DataFrame, days: int) -> pl.DataFrame:
    data_utils = DataUtils(excel_data)

    filtered_df = data_utils.filter_data_range(date_column="系统接单时间",days=days)

    # 添加“系统接单时间2”列，只保留日期部分
    filtered_df = DataUtils(filtered_df).add_date_only_column(date_column="系统接单时间",new_column="系统接单时间2")

    filtered_df = DataUtils(filtered_df).insert_colum("系统接单时间", "系统接单时间2")

    filtered_df = DataUtils(filtered_df).clean_and_unique(
        unique_columns="客服流水号",
        drop_columns=["序号"]
    )

    logging.info(f"受理路径唯一值: {filtered_df['受理路径'].unique()}")

    # 筛选特定的受理路径
    path_conditions = (
            filtered_df["受理路径"].str.contains("投诉工单（2021版）>>移网>>【网络使用】移网语音", literal=True)
            | filtered_df["受理路径"].str.contains("投诉工单（2021版）>>融合>>【网络使用】移网语音", literal=True)
    )

    filtered_df = filtered_df.filter(path_conditions)

     # 添加投诉内容筛选条件
    content_include = (
         filtered_df["投诉内容"].str.contains("语音业务类型：无法主被叫", literal=True)
        |filtered_df["投诉内容"].str.contains("无法接打电话", literal=True)
        | filtered_df["投诉内容"].str.contains("无法通话", literal=True)
        | filtered_df["投诉内容"].str.contains("接不到电话", literal=True)
        | filtered_df["投诉内容"].str.contains("无法打电话", literal=True)
        | filtered_df["投诉内容"].str.contains("无法拨打电话", literal=True)
        | filtered_df["投诉内容"].str.contains("无法接通拨打电话", literal=True)
        | filtered_df["投诉内容"].str.contains("语音通话无法正常使用", literal=True)
        | filtered_df["投诉内容"].str.contains("无法正常通话", literal=True)
        | filtered_df["投诉内容"].str.contains("打不了电话", literal=True)
        | filtered_df["投诉内容"].str.contains("无法接通电话", literal=True)
        | filtered_df["投诉内容"].str.contains("打不出电话能接电话", literal=True)
        | filtered_df["投诉内容"].str.contains("手机不支持", literal=True)
        | filtered_df["投诉内容"].str.contains("系统不支持", literal=True)
        | filtered_df["投诉内容"].str.contains("拨打不出电话", literal=True)
        | filtered_df["投诉内容"].str.contains("无法拨打出去电话", literal=True)
        | filtered_df["投诉内容"].str.contains("没办法打电话", literal=True)
        | filtered_df["投诉内容"].str.contains("接不了电话", literal=True)
    )
    
    content_exclude = (
        ~filtered_df["投诉内容"].str.contains("无法上网", literal=True)
        & ~filtered_df["投诉内容"].str.contains("无信号", literal=True)
        & ~filtered_df["投诉内容"].str.contains("信号不好", literal=True)
        & ~filtered_df["投诉内容"].str.contains("故障告警", literal=True)
        & ~filtered_df["投诉内容"].str.contains("没有信号", literal=True)
        & ~filtered_df["投诉内容"].str.contains("上网速度慢", literal=True)
        & ~filtered_df["投诉内容"].str.contains("私密号码", literal=True)
        & ~filtered_df["投诉内容"].str.contains("网络不稳定", literal=True)
        & ~filtered_df["投诉内容"].str.contains("反映信号差", literal=True)
        & ~filtered_df["投诉内容"].str.contains("停机状态", literal=True)
    )
    
    # 应用投诉内容筛选条件
    filtered_df = filtered_df.filter(content_include & content_exclude)
    
    # 如果“区域”列存在，进行数据清理和排序
    if "区域" in filtered_df.columns:
        # 去掉“区域”列中的“市”字
        filtered_df = filtered_df.with_columns(
            pl.col("区域").str.replace(r"市", "", literal=True)
        )

        # 删除“口碑未达情况原因”右侧的所有列
        temp_index = filtered_df.columns.index("口碑未达情况原因") + 1
        right_columns = filtered_df.columns[temp_index:]
        filtered_df = filtered_df.drop(right_columns)

        logging.info("删除右侧列")

        # 对“区域”列进行排序，并复制为“区域2”列
        filtered_df = (
            filtered_df.sort("区域")
            .with_columns(pl.col("区域").alias("区域2"))
        )

        logging.info("处理口碑未达情况原因替换为区域并删除右侧列!")
    else:
        logging.warning("没有找到区域列，请检查输入文档的时间数据")

    if filtered_df.is_empty():
        logging.warning("筛选后的数据为空，请检查输入文档的时间数据")

    return filtered_df


if __name__ == '__main__':
    startTime = dt.datetime.now()
    logging.info("开始解析工单查询数据并生成23G精简投诉明细数据....")
    file_manager = FileManager("WorkDocument\\23G精简投诉明细预处理脚本")
    source_file = file_manager.get_latest_file("source")

    # 读取Excel文件
    try:
        excel_data = pl.read_excel(source_file)

        if excel_data is not None:
            logging.info("如果本周一，记得修改days参数为3天否则默认为1，表示前一天的数据。")
    
            print(excel_data.columns)
            processed_df = process_excel(excel_data, days=1)
            
            # processed_df =  remove_repeat_columns(processed_df)
            
            # 假设df1, df2 是你的Polars DataFrame
            file_manager.save_to_sheet('23G精简投诉明细', sheet1=processed_df)
        else:
            logging.error("解析工单查询数据失败")
    except Exception as e:
        logging.error(f"无法读取Excel文件: {e}")
        exit(1)

    end_time = dt.datetime.now()
    logging.info("解析工单查询数据耗时：%s" % (end_time - startTime))
