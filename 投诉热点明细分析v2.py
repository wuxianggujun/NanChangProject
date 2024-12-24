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
    def find_most_similar_and_frequent(locations_list):
        """找出相似度高且出现次数最多的位置"""
        # 将 Polars Series 转换为 Python 列表
        locations = locations_list.to_list()
        
        # 过滤掉"无"值
        valid_locations = [loc for loc in locations if loc and loc != "无"]
        
        print(f"\n当前分组的所有参考位置: {valid_locations}")  # 调试信息
        
        if not valid_locations:
            return {"location": "无数据", "count": 0}
            
        if len(valid_locations) == 1:
            return {"location": valid_locations[0], "count": 1}
            
        # 存储每个位置的相似组及其出现次数
        location_groups = {}
        
        # 对每个位置进行相似度比较
        for loc1 in valid_locations:
            if loc1 not in location_groups:
                similar_group = []
                # 与其他所有位置比较相似度
                for loc2 in valid_locations:
                    similarity = fuzz.ratio(str(loc1), str(loc2))
                    print(f"比较: \n位置1: {loc1} \n位置2: {loc2} \n相似度: {similarity}")  # 调试信息
                    
                    if similarity >= 80:  # 相似度阈值
                        similar_group.append(loc2)
                
                # 记录这个相似组的代表位置和出现次数
                location_groups[loc1] = {
                    "similar_locations": similar_group,
                    "count": len(similar_group)
                }
                print(f"位置 '{loc1}' 的相似组: {similar_group}")  # 调试信息
        
        # 找出相似位置最多的组
        best_location = max(location_groups.items(), 
                          key=lambda x: (x[1]["count"], len(str(x[0]))))
        
        print(f"最终选择的位置: {best_location[0]}, 出现次数: {best_location[1]['count']}")  # 调试信息
        
        return {
            "location": best_location[0],
            "count": best_location[1]["count"]
        }
    
    # 按投诉标识分组并应用分析
    location_stats = (df.group_by('投诉标识')
        .agg(pl.col('参考位置'))
        .with_columns([
            pl.col('参考位置').map_elements(
                find_most_similar_and_frequent,
                return_dtype=pl.Struct([
                    pl.Field('location', pl.Utf8),
                    pl.Field('count', pl.Int64)
                ])
            ).alias('位置分析')
        ]))
    
    # 将分析结果拆分为位置和次数
    location_stats = location_stats.with_columns([
        pl.col('位置分析').struct.field('location').alias('最相似位置'),
        pl.col('位置分析').struct.field('count').alias('出现次数')
    ]).drop('位置分析')
    
    # 将结果合并回原始数据框
    result_df = df.join(
        location_stats.select(['投诉标识', '最相似位置', '出现次数']), 
        on='投诉标识'
    ).with_columns([
        pl.col('最相似位置').alias('投诉位置')
    ]).drop('最相似位置')
    
    print("\n最终处理结果示例：")
    print(result_df.head())
    
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
