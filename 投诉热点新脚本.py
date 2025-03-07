import polars as pl
import re
import openpyxl
import pandas as pd
import os

def extract_address(text):
    """
    从文本中提取地址信息，优先提取用户实际位置而不是基站位置
    """
    if text is None or pd.isna(text) or text == "":
        return None
    
    # 预处理文本，替换标点
    text = text.replace('，', ',').replace('。', '.').replace('；', ';').replace('：', ':')
    
    # 第一步：识别用户实际位置的关键短语
    user_location_patterns = [
        # 直接描述用户位置的短语
        r'(?:联系用户得知|用户反映|据用户反映|用户表示|了解到用户|用户称)(?:在|位于|到达|来到|处于)([^,.;:!?，。；：！？JL\[\]]*?(?:宿舍|公寓|楼|栋|院|校区|小区|大学|学院|财大)[^,.;:!?，。；：！？JL\[\]]*?)(?:内|中|里|上网|信号|5G|4G|卡顿)',
        
        # "反映在XX" 模式
        r'反映在([^,.;:!?，。；：！？JL\[\]]*?(?:宿舍|公寓|楼|栋|院|校区|小区|大学|学院|财大)[^,.;:!?，。；：！？JL\[\]]*?)(?:内|中|里|上网|信号|5G|4G|卡顿)',
        
        # "在XX使用" 模式
        r'在([^,.;:!?，。；：！？JL\[\]]*?(?:宿舍|公寓|楼|栋|院|校区|小区|大学|学院|财大)[^,.;:!?，。；：！？JL\[\]]*?)(?:使用|上网|信号|卡顿)',
        
        # 新增: "XX上网卡顿"模式，针对"地址在共青财大上网卡顿"
        r'地址在([^,.;:!?，。；：！？JL\[\]]*?(?:宿舍|公寓|楼|栋|院|校区|小区|大学|学院|财大)[^,.;:!?，。；：！？JL\[\]]*?)(?:上网|信号|5G|4G|卡顿)',
        
        # 新增: "现场核实发现XX"模式
        r'现场核实发现([^,.;:!?，。；：！？JL\[\]]*?(?:宿舍|公寓|楼|栋|院|校区|小区|大学|学院|财大)[^,.;:!?，。；：！？JL\[\]]*?)(?:内|中|里|上网|信号|5G|4G|弱覆盖|覆盖)',
        
        # 新增: "您投XX"和"您投诉XX"模式
        r'您投(?:诉)?([^,.;:!?，。；：！？JL\[\]]*?(?:\d+)?[号栋楼][^,.;:!?，。；：！？JL\[\]]*?)(?:上网|信号|5G|4G|内|中|里|卡顿)',
        
        # 新增: "您投诉XX"的更宽泛模式，针对商务技师学院
        r'您投诉([^,.;:!?，。；：！？JL\[\]]*?(?:学院|大学|校区|财大)[^,.;:!?，。；：！？JL\[\]]*?(?:\d+)?[号栋楼]?)(?:上网|信号|5G|4G|内|中|里|卡顿)',
        
        # 新增: "尊敬的用户，您投XX"模式
        r'尊敬的用户[^,.;:!?，。；：！？]*您投(?:诉)?([^,.;:!?，。；：！？JL\[\]]*?(?:\d+)?[号栋楼][^,.;:!?，。；：！？JL\[\]]*?)(?:上网|信号|5G|4G|内|中|里|卡顿)'
    ]
    
    # 尝试提取用户实际位置
    user_locations = []
    for pattern in user_location_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if match and len(match) > 3:  # 避免过短的匹配
                # 清理匹配结果
                cleaned_match = match.strip()
                if cleaned_match and not any(word in cleaned_match for word in ["JJG", "检测", "测试", "覆盖", "负荷", "占用"]):
                    user_locations.append(cleaned_match)
    
    # 第二步：如果没有找到明确的用户位置描述，尝试从句子开头识别位置
    if not user_locations:
        # 在句子开始位置搜索地址模式
        sentence_patterns = [
            # 在联系用户得知后面直接跟地址的模式
            r'联系用户得知((?:共青农大|南昌工学院|江西农业大学|南昌师范学院|共青现代职业学院|应用师范学院|商务技师学院|中大青山湖|共青财大)[^,.;:!?，。；：！？]*?(?:\d+)?[号栋楼]?[^,.;:!?，。；：！？]*?(?:宿舍|公寓)?)',
            
            # 在句子开始处查找直接的地址表述
            r'^[^,.;:!?，。；：！？]*?在((?:共青农大|南昌工学院|江西农业大学|南昌师范学院|共青现代职业学院|应用师范学院|商务技师学院|中大青山湖|共青财大)[^,.;:!?，。；：！？]*?(?:\d+)?[号栋楼]?[^,.;:!?，。；：！？]*?(?:宿舍|公寓)?)',
            
            # 新增: 处理"您投"和"您投诉"开头的模式
            r'您投(?:诉)?([^,.;:!?，。；：！？JL\[\]]*?(?:\d+)?[号栋楼]?[^,.;:!?，。；：！？JL\[\]]*?)(?:上网|信号|5G|4G|内|中|里|卡顿)',
            
            # 新增: 处理包含区和园的地址，如"青山湖区中大青山湖东园4栋20楼"
            r'([^,.;:!?，。；：！？]*?区[^,.;:!?，。；：！？]*?园[^,.;:!?，。；：！？]*?\d+栋[^,.;:!?，。；：！？]*?\d+楼)'
        ]
        
        for pattern in sentence_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if match and len(match) > 3:
                    cleaned_match = match.strip()
                    if cleaned_match and not any(word in cleaned_match for word in ["JJG", "检测", "测试", "覆盖", "负荷", "占用"]):
                        user_locations.append(cleaned_match)
    
    # 第三步：如果还未找到，搜索特定学校或机构的名称，并排除基站名称中的匹配
    if not user_locations:
        # 创建一个函数来确定文本是否是基站描述的一部分
        def is_part_of_base_station(text_portion, full_text):
            base_station_indicators = ["JJG_", "JJG", "[0", "小区", "基站", "室分"]
            for indicator in base_station_indicators:
                # 检查这个文本片段是否在包含基站指示器的更大片段中
                for i in range(max(0, full_text.find(text_portion) - 30), min(len(full_text), full_text.find(text_portion) + len(text_portion) + 30)):
                    if i >= 0 and i + len(indicator) <= len(full_text):
                        if full_text[i:i+len(indicator)] == indicator:
                            return True
            return False
        
        location_keywords = [
            ("南昌应用师范学院", ["宿舍", "公寓", "楼", "栋"]),
            ("共青农大", ["宿舍", "公寓", "楼", "栋", "商学院"]),
            ("南昌工学院", ["宿舍", "公寓", "楼", "栋"]),
            ("现代职业学院", ["宿舍", "公寓", "楼", "栋"]),
            ("共青现代职业学院", ["宿舍", "公寓", "楼", "栋"]),
            ("江西农业大学", ["宿舍", "公寓", "楼", "栋", "商学院"]),
            ("商务技师学院", ["宿舍", "公寓", "楼", "栋"]),
            ("中大青山湖", ["园", "栋", "楼"]),
            ("青山湖区", ["园", "栋", "楼"]),
            ("共青财大", ["宿舍", "公寓", "楼", "栋"])
        ]
        
        for location, suffixes in location_keywords:
            if location in text:
                # 查找这个位置是否在基站描述中
                if not is_part_of_base_station(location, text):
                    # 尝试找到更完整的地址（带楼栋号等）
                    for suffix in suffixes:
                        pattern = f"{location}[^,.;:!?，。；：！？]*?(?:\\d+)?[号栋楼]?[^,.;:!?，。；：！？]*?{suffix}"
                        matches = re.findall(pattern, text)
                        if matches:
                            for match in matches:
                                if match and len(match) > 3 and not is_part_of_base_station(match, text):
                                    user_locations.append(match.strip())
                        else:
                            # 如果没有找到带后缀的匹配，就用位置名本身
                            user_locations.append(location)
    
    # 四步：添加针对特定结构地址的匹配（如"青山湖区中大青山湖东园4栋20楼"）
    if not user_locations:
        specific_patterns = [
            r'([^,.;:!?，。；：！？]*?区[^,.;:!?，。；：！？]*?(?:东|西|南|北)?园[^,.;:!?，。；：！？]*?\d+栋(?:\d+楼)?)',
            r'([^,.;:!?，。；：！？]*?学院[^,.;:!?，。；：！？]*?\d+栋)',
            r'([^,.;:!?，。；：！？]*?大学[^,.;:!?，。；：！？]*?\d+栋)',
            r'([^,.;:!?，。；：！？]*?财大[^,.;:!?，。；：！？]*?)'
        ]
        
        for pattern in specific_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if match and len(match) > 3 and not is_part_of_base_station(match, text):
                    user_locations.append(match.strip())
    
    # 清理和优化提取到的地址
    cleaned_locations = []
    for location in user_locations:
        # 移除非地址相关前缀
        prefixes_to_remove = ["联系用户得知", "在", "位于", "反映在", "用户反映在", "来到", "处于", "您投", "您投诉", "尊敬的用户"]
        for prefix in prefixes_to_remove:
            if location.startswith(prefix):
                location = location[len(prefix):].strip()
        
        # 移除特定短语和网络类型标识
        location = re.sub(r'内上网.*$', '', location)
        location = re.sub(r'使用.*$', '', location)
        location = re.sub(r'信号.*$', '', location)
        location = re.sub(r'上网.*$', '', location)
        location = re.sub(r'卡顿.*$', '', location)
        location = re.sub(r'5G|4G|3G|2G', '', location)  # 移除网络类型标识
        
        # 移除地址末尾的"内"、"中"、"里"等词
        location = re.sub(r'内$', '', location)
        location = re.sub(r'中$', '', location)
        location = re.sub(r'里$', '', location)
        
        # 清理标点和多余空格
        location = re.sub(r'[^\w\s\d#]', '', location).strip()
        location = re.sub(r'\s+', ' ', location)
        
        if location and len(location) > 3:
            # 排除纯数字和过长数字
            if not location.isdigit() and not re.search(r'\d{6,}', location):
                cleaned_locations.append(location)
    
    # 去重并选择最佳地址
    if cleaned_locations:
        # 移除重复项
        unique_locations = list(set(cleaned_locations))
        
        # 按长度排序，通常更长的地址包含更详细的信息
        sorted_locations = sorted(unique_locations, key=len, reverse=True)
        
        # 优先返回包含数字（楼栋号）的位置
        locations_with_numbers = [loc for loc in sorted_locations if re.search(r'\d+', loc)]
        if locations_with_numbers:
            # 找出最具体的地址（包含最多数字标识的）
            max_digits = 0
            best_location = locations_with_numbers[0]
            for loc in locations_with_numbers:
                digits_count = sum(c.isdigit() for c in loc)
                if digits_count > max_digits:
                    max_digits = digits_count
                    best_location = loc
            return best_location
        
        # 如果没有包含数字的位置，返回最长的位置
        return sorted_locations[0]
    
    return None

def main():
    # 文件路径
    file_path = "WorkDocument/投诉热点明细分析/source/202403-202502投诉热点明细表-终稿.xlsx"
    output_path = "WorkDocument/投诉热点明细分析/output/202403-202502投诉热点明细表-更新后.xlsx"
    
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # 读取Excel文件中的"明细"工作表
    print("正在读取Excel文件的明细工作表...")
    df = pl.read_excel(file_path, sheet_name="明细")
    print(f"已读取明细工作表，共 {len(df)} 行")
    
    # 将polars DataFrame转换为pandas DataFrame以便更容易处理
    pandas_df = df.to_pandas()
    
    # 对每一行应用地址提取函数
    print("开始精确提取地址，优先提取用户实际位置...")
    address_list = []
    for index, row in pandas_df.iterrows():
        # 首先从答复口径提取地址
        address = None
        if not pd.isna(row["答复口径"]):
            address = extract_address(row["答复口径"])
        
        # 如果答复口径中没有找到地址，则从投诉内容中提取
        if (address is None or address == "") and not pd.isna(row["投诉内容"]):
            address = extract_address(row["投诉内容"])
        
        address_list.append(address)
        
        # 显示进度
        if (index + 1) % 100 == 0 or index == len(pandas_df) - 1:
            print(f"已处理 {index + 1}/{len(pandas_df)} 行")
    
    # 更新投诉位置列
    print("更新投诉位置列...")
    pandas_df["投诉位置"] = address_list
    
    # 打印一些样本，验证地址提取效果
    print("\n样本预览（提取的地址）:")
    sample_df = pandas_df[pandas_df["投诉位置"].notna()].head(10)
    for idx, row in sample_df.iterrows():
        print(f"行 {idx}:")
        print(f"  答复口径: {str(row['答复口径'])[:150]}...")
        print(f"  提取地址: {row['投诉位置']}")
        print("-" * 80)
    
    # 额外打印特殊例子，检查是否正确提取
    print("\n检查特殊例子:")
    
    special_cases = [
        "联系用户得知在南昌应用师范学院宿舍内上网慢，核实发现该处JJG_SLRN_共青城市南昌工学院F宿3#3F弱电间-8[02060778]忙时负荷80%左右，负荷偏高导致上网慢，需扩容，预计2025年2月之前完成，请用户耐心等待",
        "联系用户得知共青农大商学院5G上网慢，核实发现共青农大商学院5G主占的JJG_SLRN_共青城市江西农业大学南昌商学院1#学生公寓宿舍南面3F-2[02060156]利用率90%左右，负荷偏高导致上网慢，已推电信扩容，预计2025年2月，建议用户尝试使用4G网络，用户表示满意并认可",
        "尊敬的用户，您投青山湖区中大青山湖东园4栋20楼上网慢，核实用户投诉位置距离NCH_SLQN_青山湖区中大青山湖东园3栋1单元电梯机房-2[01049237]基站300米，无MR，为提高用户感知，12月8-10号用户有时间，将进行优化。联系用户，用户认可。",
        "您投诉江西省商务技师学院2栋上网慢，我专业已派人员上门联系用户，在指定位置已经测试，RSRP-95dBm，SINR13dB，用户感知良好，现场已针对问题进行优化，联系用户，用户认可，表示感谢。",
        "地址在共青财大上网卡顿，用户反馈在晚上网络容易断",
        "现场核实发现共青现代职业学院宿舍内信号弱覆盖，已启动优化方案"
    ]
    
    for i, case in enumerate(special_cases):
        print(f"特殊例子{i+1}:")
        print(f"  原文: {case[:150]}...")
        print(f"  提取地址: {extract_address(case)}")
        print("-" * 80)
    
    # 使用pandas直接保存到Excel，保留其他工作表
    print("正在保存结果到Excel文件...")
    
    # 首先读取原始Excel的所有工作表
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        original_excel = pd.ExcelFile(file_path)
        for sheet_name in original_excel.sheet_names:
            if sheet_name != "明细":  # 跳过明细工作表，使用更新后的数据
                sheet_df = pd.read_excel(file_path, sheet_name=sheet_name)
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # 写入更新后的明细工作表
        pandas_df.to_excel(writer, sheet_name="明细", index=False)
    
    print(f"已更新投诉位置并保存到: {output_path}")
    
    # 显示处理的统计信息
    total_rows = len(pandas_df)
    filled_rows = pandas_df["投诉位置"].notna().sum()
    print(f"总行数: {total_rows}")
    print(f"成功填充地址的行数: {filled_rows}")
    print(f"填充率: {filled_rows / total_rows * 100:.2f}%")
    
    # 验证保存是否成功
    try:
        test_df = pd.read_excel(output_path, sheet_name="明细")
        test_filled = test_df["投诉位置"].notna().sum()
        print(f"验证: 输出文件中成功填充地址的行数: {test_filled}")
        if test_filled == filled_rows:
            print("验证通过: 文件保存成功!")
        else:
            print("警告: 保存的文件中填充的地址行数与处理结果不一致!")
    except Exception as e:
        print(f"验证文件时出错: {e}")

if __name__ == "__main__":
    main()
