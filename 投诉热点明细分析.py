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
    def clean_duplicate_location(location):
        # 清理重复的行政区名称
        for admin in ['省', '市', '区', '县']:
            parts = location.split(admin)
            if len(parts) > 2:  # 如果同一个行政区出现多次
                # 保留第一次出现的位置
                cleaned = admin.join(parts[:2])
                if len(parts) > 2:
                    cleaned += admin.join(parts[2:])
                location = cleaned
        return location

    def find_common_substring(locations):
        if not locations:
            return None
            
        # 过滤无效数据并清理重复的行政区
        invalid_words = ['无', '未联系到用户']
        locations = [clean_duplicate_location(loc) for loc in locations 
                    if loc and isinstance(loc, str) and 
                    not any(word == loc for word in invalid_words)]
        if not locations:
            return None
            
        # 找出最长公共子串
        def get_common_substring(str1, str2):
            matrix = {}
            longest_length = 0
            longest_end_pos = 0
            
            for i, char1 in enumerate(str1):
                for j, char2 in enumerate(str2):
                    if char1 == char2:
                        current_length = matrix.get((i-1, j-1), 0) + 1
                        matrix[i, j] = current_length
                        if current_length > longest_length:
                            longest_length = current_length
                            longest_end_pos = i
            
            if longest_length < 4:  # 至少4个字符匹配
                return None
                
            return str1[longest_end_pos - longest_length + 1:longest_end_pos + 1]
            
        # 找出所有位置中的公共子串
        common = locations[0]
        for loc in locations[1:]:
            common_part = get_common_substring(common, loc)
            if not common_part:
                continue
            common = common_part
            
        if len(common) < 4:
            return None
            
        # 定义地址结束标识词（按优先级排序）
        location_markers = [
            '学校', '中学', '高中', '学院', '专科学校',  # 教育机构
            '工业园', '科技园', '厂房',                  # 工业区
            '村', '小区', '广场', '府', '城',           # 具体地点
            '镇'                                        # 行政区
        ]
        
        # 扩展公共子串以包含完整的地址
        base_locations = []
        for loc in locations:
            if common in loc:
                start_idx = loc.find(common)
                # 向后查找地址标识词
                for marker in location_markers:
                    marker_idx = loc.find(marker, start_idx)
                    if marker_idx >= 0:  # 找到标识词
                        # 保留完整的行政区信息
                        prefix = loc[:start_idx]
                        if any(admin in prefix for admin in ['省', '市', '区', '县']):
                            base_locations.append(loc[:marker_idx + len(marker)])
                        else:
                            base_locations.append(loc[start_idx:marker_idx + len(marker)])
        
        # 选择出现次数最多的基础地址
        if base_locations:
            base_count = Counter(base_locations)
            base_location = base_count.most_common(1)[0][0]
            
            # 如果是学校类地址，查找楼栋信息
            if any(marker in base_location for marker in ['学校', '中学', '学院', '专科']):
                building_patterns = []
                for loc in locations:
                    if base_location in loc:
                        # 提取数字+楼栋标识
                        remaining = loc[loc.find(base_location) + len(base_location):].strip()
                        for i, char in enumerate(remaining):
                            if char.isdigit():
                                j = i
                                while j < len(remaining) and (remaining[j].isdigit() or remaining[j] in ['栋', '号', '楼']):
                                    j += 1
                                if j > i:
                                    # 只保留到"栋"或"号楼"
                                    building = remaining[i:j]
                                    for marker in ['栋', '号楼']:
                                        if marker in building:
                                            end_idx = building.find(marker) + len(marker)
                                            building_patterns.append(building[:end_idx])
                                            break
                
                # 统计楼栋信息出现次数
                if building_patterns:
                    building_count = Counter(building_patterns)
                    most_common = building_count.most_common(1)[0][0]
                    return f"{base_location}{most_common}"
            
            return base_location

        return common

    # 按投诉标识分组
    grouped_data = {}
    for row in df.iter_rows(named=True):
        complaint_id = row['投诉标识']
        location = row['参考位置']
        if complaint_id not in grouped_data:
            grouped_data[complaint_id] = []
        grouped_data[complaint_id].append(location)

    # 处理每组数据
    results = {}
    for complaint_id, locations in grouped_data.items():
        common_location = find_common_substring(locations)
        if common_location:
            results[complaint_id] = common_location

    # 创建结果DataFrame
    result_df = df.clone()
    result_df = result_df.with_columns(
        pl.Series(name="投诉位置", 
                 values=[results.get(row['投诉标识']) for row in df.iter_rows(named=True)])
    )

    return df, result_df

if __name__ == "__main__":
    start_time = dt.datetime.now()
    file_manager = FileManager("WorkDocument\\投诉热点明细分析")

    source_file = file_manager.get_latest_file("source")

    # 读取Excel文件
    try:
        df = file_manager.read_excel(source_file,"明细")
    except Exception as e:
        logging.error(f"无法读取Excel文件: {e}")
        exit(1)

    # 处理数据并获取结果
    processed_df, result_df= process_excel(df)

    file_manager.save_to_sheet("投诉热点明细分析",
                               原始数据=processed_df, 热点明细分析结果=result_df)

    # 如果需要获取输出路径
    print(f"输出文件路径: {file_manager.output_path}")

    end_time = dt.datetime.now()
    runtime = end_time - start_time
    logging.info(f'运行时间：{runtime}')
    