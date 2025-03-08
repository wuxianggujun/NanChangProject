# coding=utf-8
import polars as pl
import re
import os
from openai import AsyncOpenAI
import asyncio
import time
from datetime import datetime
import platform
from tqdm import tqdm
import sys
from collections import Counter

class AliyunLLM:
    """阿里云大语言模型API接口"""
    
    def __init__(self, api_key=None):
        """初始化API客户端"""
        # 优先使用传入的API密钥，否则使用默认值
        self.api_key = api_key or "sk-c8464e16fdc844fd8ca1399062d3c1d7"
        self.base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
        
        # 初始化客户端
        self.client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
    
    def normalize_address(self, address):
        """规范化地址，去除不必要的后缀和突兀的表达"""
        if not address:
            return address
            
        # 去除结尾的方位词
        unwanted_suffixes = ["内", "中", "处", "边", "旁", "附近", "左右", "上", "里", "下"]
        for suffix in unwanted_suffixes:
            if address.endswith(suffix):
                address = address[:-len(suffix)]
                
        # 清理额外空格
        address = re.sub(r'\s+', '', address)
        
        return address
    
    async def analyze_top_addresses(self, address_info, timeout=30):
        """分析地址列表，找出最多3个最常见/最具代表性的地址
        
        参数:
            address_info: 一个列表，包含所有地址（可能有重复的地址）
        """
        if not address_info:
            return ["无有效地址"]
            
        if isinstance(address_info, dict):
            # 如果是字典格式，转换为列表（每个地址重复出现的次数等于其值）
            address_list = []
            for addr, count in address_info.items():
                address_list.extend([addr] * count)
        else:
            address_list = address_info
            
        # 统计地址出现次数
        counter = Counter(address_list)
        
        # 如果只有1-3个不同的地址，按频率排序返回
        unique_addresses = set(address_list)
        if len(unique_addresses) <= 3:
            return [self.normalize_address(addr) for addr, _ in counter.most_common()]
            
        # 预处理：如果有出现次数明显高于其他地址的地址（超过1次），只选择这些高频地址
        multi_addrs = [addr for addr, count in counter.most_common() if count > 1]
        
        # 如果有2个或以上的多次出现地址，就不再考虑只出现一次的地址
        if len(multi_addrs) >= 2:
            # 只使用AI分析多次出现的地址
            counter_multi = {addr: count for addr, count in counter.items() if count > 1}
            sorted_addrs = sorted(counter_multi.items(), key=lambda x: x[1], reverse=True)
            
            # 如果只有2-3个多次出现的地址，直接返回这些地址（按频率排序）
            if len(sorted_addrs) <= 3:
                return [self.normalize_address(addr) for addr, _ in sorted_addrs]
            
            # 仅讨论多次出现的地址
            address_list_for_ai = []
            for addr, count in sorted_addrs:
                address_list_for_ai.extend([addr] * count)
                
            # 更新计数器
            counter = Counter(address_list_for_ai)
        
        # 使用AI分析
        prompt = f"""
请分析投诉地址数据，严格按以下规则输出最合适的地址：

# 核心处理规则
1. **去行政区划**：
   - 只保留具体点位名称（如"农大商学院16栋"，而非"江西省南昌市农大商学院16栋"）
   - 自动剥离省/市/区等上级地名（即使原始地址包含）
   - 此步骤非常重要且复杂，需要理解上下文，判断哪些是行政区划，哪些是具体地点名称

2. **相似地址合并**：
   - 层级递进合并：当同时存在父级和子级地址时，仅保留最详细版本并叠加次数
     例："南昌大学(5次)+南昌大学前湖校区(3次)" → 保留后者，总次数8次
   - 严禁同时输出父级和子级地址

3. **精简标准**：
   - 必须删除结尾方位词（内/中/里等）
   - 保留最小完整单元：需能独立标识位置
     有效："工业园3号楼"、"职业学院宿舍"
     无效："工业园"（过于宽泛）

# 过滤机制
1. **次数过滤**：
   - 当存在≥2个地址出现≥2次时，完全剔除单次地址
   - 所有地址均为单次时，才允许输出单次地址

2. **冗余过滤**：
   - 若某地址是其他地址的父级，则剔除该父级地址
     当存在"农大商学院16栋"时，自动过滤"农大商学院"

# 示例处理
1. 示例一：
   输入："江西省九江市共青城市农大商学院16栋(8次)、农大商学院(5次)"
   输出："农大商学院16栋"（合并次数13次，剥离行政区划，过滤父级地址）

2. 示例二：
   输入："共青现代职业学院宿舍内(30次)、江西省九江市共青城市江西现代职业技术学院共青产教融合基地(1次)、共青工业职业学院宿舍内(4次)"
   输出："现代职业学院宿舍"、"工业职业学院宿舍"（剥离行政区划和"内"字，过滤低频地址）

3. 示例三：
   输入："科技园内(3次)、朝阳科技园B栋(2次)"
   输出："朝阳科技园B栋"（剥离"内"字，父级"科技园"被过滤，因为B栋更具体）

原始地址列表（按出现次数排序）：
{', '.join([f"{addr} (出现{count}次)" for addr, count in counter.most_common()])}

请理解每个地址的上下文，按合并后的次数从高到低输出最多3个标准化地址（每行一个，无需包含任何解释或次数信息）。
输出要点：
- 删除行政区划和结尾方位词
- 合并相似地址并累加次数
- 当有≥2个地址出现≥2次时，完全不输出单次地址
- 避免输出冗余地址（父级地址）
"""
        try:
            # 使用超时机制确保请求不会卡住
            stream_response = await asyncio.wait_for(
                self.client.chat.completions.create(
                    model="qwq-32b",
                    messages=[{"role": "user", "content": prompt}],
                    stream=True
                ),
                timeout=timeout
            )
            
            # 收集内容
            result = ""
            async for chunk in stream_response:
                if chunk.choices and len(chunk.choices) > 0 and chunk.choices[0].delta.content:
                    result += chunk.choices[0].delta.content
            
            # 处理结果，返回最多3个地址
            addresses = []
            for line in result.strip().split('\n'):
                line = line.strip()
                # 去除可能的频率信息
                line = re.sub(r'\s*\(出现\d+次\)\s*', '', line)
                line = re.sub(r'^\d+[\.\s、]+', '', line)  # 移除可能的序号
                if line and len(addresses) < 3:
                    # 检查是否AI已经使用了顿号分隔
                    if "、" in line and len(addresses) == 0:
                        addresses = [re.sub(r'\s*\(出现\d+次\)\s*', '', addr.strip()) for addr in line.split("、")][:3]
                        break
                    addresses.append(line)
            
            # 只进行方位词的规范化，行政区划处理完全交给AI
            addresses = [self.normalize_address(addr) for addr in addresses]
            
            # 验证地址是否有冗余（简略版本和详细版本同时存在）
            if len(addresses) > 1:
                # 去除冗余地址
                non_redundant = []
                for i, addr1 in enumerate(addresses):
                    is_redundant = False
                    for j, addr2 in enumerate(addresses):
                        if i != j and addr1 in addr2:  # addr1是addr2的子串，表示addr1是简略版本
                            is_redundant = True
                            break
                    if not is_redundant:
                        non_redundant.append(addr1)
                
                # 如果发现并移除了冗余地址，更新结果
                if len(non_redundant) < len(addresses):
                    addresses = non_redundant
            
            # 验证AI输出的地址是否符合要求
            if addresses:
                # 如果有多次出现的地址，检查AI是否输出了只出现一次的地址
                if len(multi_addrs) >= 2:
                    # 先规范化multi_addrs列表
                    normalized_multi_addrs = [self.normalize_address(addr) for addr in multi_addrs]
                    # 检查AI输出的地址是否在规范化后的多次出现地址列表中
                    filtered_addresses = []
                    for addr in addresses:
                        # 检查规范化后的地址是否匹配
                        is_high_freq = addr in normalized_multi_addrs
                        # 如果不匹配，检查原始地址是否匹配
                        if not is_high_freq:
                            for m_addr in multi_addrs:
                                if self.normalize_address(m_addr) == addr:
                                    is_high_freq = True
                                    break
                        if is_high_freq:
                            filtered_addresses.append(addr)
                    
                    # 如果过滤后还有地址，使用过滤后的结果
                    if filtered_addresses:
                        return filtered_addresses[:3]
                    
                # 检查是否所有地址都是低频地址
                all_low_freq = True
                for addr in addresses:
                    # 检查规范化前后的地址是否为高频地址
                    is_high_freq = False
                    for orig_addr, count in counter.items():
                        if count > 1 and (addr == orig_addr or addr == self.normalize_address(orig_addr)):
                            is_high_freq = True
                            break
                    if is_high_freq:
                        all_low_freq = False
                        break
                
                if all_low_freq and multi_addrs:
                    # 如果AI返回的全是低频地址，但实际有高频地址，则使用高频地址
                    return [self.normalize_address(addr) for addr in multi_addrs[:3]]
                    
                return addresses[:3]
            
            # 如果AI没有返回任何有效地址，则使用频率排序
            # 优先选择出现次数大于1的地址
            if multi_addrs:
                return [self.normalize_address(addr) for addr in multi_addrs[:3]]
            else:
                return [self.normalize_address(addr) for addr, _ in counter.most_common(3)]
                
        except Exception as e:
            # 出错时使用频率排序，优先选择出现次数大于1的地址
            if multi_addrs:
                return [self.normalize_address(addr) for addr in multi_addrs[:3]]
            else:
                return [self.normalize_address(addr) for addr, _ in counter.most_common(3)]

class ComplaintAddressProcessor:
    """投诉地址处理工具"""
    
    def __init__(self):
        """初始化工具"""
        self.results_folder = "处理结果"
        self.ai = AliyunLLM()
        
        # 创建结果文件夹（如果不存在）
        if not os.path.exists(self.results_folder):
            os.makedirs(self.results_folder)
    
    def load_data(self, file_path, sheet_name="明细"):
        """加载Excel数据"""
        try:
            print(f"正在加载文件: {file_path}, 工作表: {sheet_name}")
            df = pl.read_excel(file_path, sheet_name=sheet_name)
            print(f"成功加载数据，共 {df.height} 条记录")
            return df
        except Exception as e:
            print(f"加载数据时出错: {e}")
            return None
    
    def extract_identifier(self, identifier):
        """从投诉标识中提取数字部分，如从'JJ-397'提取'397'"""
        if not identifier or not isinstance(identifier, str):
            return ""
        
        # 使用正则表达式提取数字部分
        match = re.search(r'(\d+)', identifier)
        if match:
            return match.group(1)
        return identifier  # 如果没有数字，则返回原标识
    
    async def process_address_batch(self, id_address_map, progress=None):
        """批量处理地址分析任务"""
        tasks = []
        results = {}
        task_map = {}  # 跟踪每个任务对应的ID
        
        # 创建所有任务
        for complaint_id, addresses in id_address_map.items():
            if addresses:
                task = self._analyze_address_task(complaint_id, addresses)
                tasks.append(task)
                task_map[id(task)] = complaint_id
            else:
                results[complaint_id] = ["无有效地址"]
                if progress:
                    progress.update(1)
        
        # 如果没有任务，直接返回
        if not tasks:
            return results
            
        # 使用as_completed并行执行任务，可以立即处理完成的任务
        for task in asyncio.as_completed(tasks):
            try:
                complaint_id, result = await task
                results[complaint_id] = result
            except Exception as e:
                # 如果任务失败，找出是哪个ID
                task_id = id(task)
                complaint_id = task_map.get(task_id, "未知ID")
                
                # 为失败的任务设置默认值
                if complaint_id in id_address_map and id_address_map[complaint_id]:
                    # 使用频率统计
                    counter = Counter(id_address_map[complaint_id])
                    # 优先选择多次出现的地址
                    multi_addrs = [addr for addr, count in counter.most_common() if count > 1]
                    if len(multi_addrs) >= 2:
                        results[complaint_id] = multi_addrs[:3]
                    elif multi_addrs:
                        # 如果只有一个多次出现的地址，也只返回这一个
                        results[complaint_id] = multi_addrs
                    else:
                        # 如果没有多次出现的地址，返回频率最高的最多3个
                        results[complaint_id] = [addr for addr, _ in counter.most_common(3)]
                else:
                    results[complaint_id] = ["处理失败"]
            
            # 每完成一个任务就更新进度
            if progress:
                progress.update(1)
                
        return results
    
    async def _analyze_address_task(self, complaint_id, address_list):
        """单个地址分析任务"""
        try:
            top_addresses = await self.ai.analyze_top_addresses(address_list)
            return complaint_id, top_addresses
        except Exception as e:
            # 使用频率统计
            counter = Counter(address_list)
            # 优先选择多次出现的地址
            multi_addrs = [addr for addr, count in counter.most_common() if count > 1]
            if len(multi_addrs) >= 2:
                return complaint_id, multi_addrs[:3]
            elif multi_addrs:
                # 如果只有一个多次出现的地址，也只返回这一个
                return complaint_id, multi_addrs
            else:
                # 如果没有多次出现的地址，返回频率最高的最多3个
                return complaint_id, [addr for addr, _ in counter.most_common(3)]
    
    async def process_by_identifier(self, df, id_column="投诉标识", address_column="投诉位置", batch_size=20):
        """按投诉标识分组，处理地址并添加最终投诉位置"""
        print(f"开始按{id_column}分组处理地址...")
        
        # 检查必要的列是否存在
        columns = df.columns
        if id_column not in columns:
            print(f"错误: 数据中不包含'{id_column}'列")
            return None, None
        if address_column not in columns:
            print(f"错误: 数据中不包含'{address_column}'列")
            return None, None
        
        # 创建编号列 - 提取标识中的数字部分
        print("正在提取标识编号...")
        df = df.with_columns(
            pl.col(id_column).map_elements(
                lambda x: self.extract_identifier(x), 
                return_dtype=pl.Utf8
            ).alias("complaint_id")
        )
        
        # 筛选有效地址记录
        print("正在筛选有效地址...")
        valid_records = df.filter(
            (pl.col(address_column).is_not_null()) & 
            (pl.col(address_column) != "") & 
            (pl.col(address_column) != "解析地址失败")
        )
        
        print(f"有效地址记录数: {valid_records.height}")
        
        # 获取所有不同的标识编号
        id_list = df.get_column("complaint_id").unique().to_list()
        print(f"共有 {len(id_list)} 个不同的投诉标识编号")
        
        # 构建ID-地址映射，保留全部地址（不去重）
        print("构建地址映射...")
        id_address_map = {}
        address_count_map = {}
        
        for complaint_id in id_list:
            # 获取该编号的所有地址（不去重）
            addresses = valid_records.filter(pl.col("complaint_id") == complaint_id).get_column(address_column).to_list()
            # 保存完整地址列表（不去重）
            id_address_map[complaint_id] = addresses
            address_count_map[complaint_id] = len(addresses)
        
        # 批量处理地址
        print("\n开始AI分析地址...")
        top_addresses_dict = {}
        
        # 使用简洁的进度条设置
        progress_bar = tqdm(total=len(id_list), desc="处理进度")
        
        # 使用更大的批处理大小
        for i in range(0, len(id_list), batch_size):
            batch_ids = id_list[i:i+batch_size]
            batch_map = {id: id_address_map[id] for id in batch_ids}
            
            batch_results = await self.process_address_batch(batch_map, progress_bar)
            top_addresses_dict.update(batch_results)
            
            # 只在每5个批次后保存一次中间结果，减少输出干扰
            if (i // batch_size) % 5 == 4:
                self._save_interim_results(top_addresses_dict, id_list[:i+len(batch_ids)], 
                                        df, id_column, id_address_map, address_count_map, silent=True)
        
        progress_bar.close()
        
        # 打印每个投诉标识的高频地址
        print("\n各投诉标识的代表性地址:")
        for complaint_id, addresses in top_addresses_dict.items():
            orig_id = df.filter(pl.col("complaint_id") == complaint_id).get_column(id_column)[0]
            addr_str = "、".join(addresses)
            print(f"{orig_id} (编号{complaint_id}): {addr_str}")
        
        # 根据标识编号获取最终投诉位置（使用"、"连接多个地址）
        def get_final_addresses(row):
            complaint_id = row["complaint_id"]
            addresses = top_addresses_dict.get(complaint_id, ["未处理"])
            return "、".join(addresses)
        
        # 添加最终投诉位置列到结果
        print("\n填充最终投诉位置到所有记录...")
        
        # 首先获取原始列
        original_cols = df.columns
        
        # 创建最终投诉位置列
        final_position_col = pl.struct(["complaint_id"]).map_elements(
            lambda x: get_final_addresses(x), 
            return_dtype=pl.Utf8
        ).alias("最终投诉位置")
        
        # 重新排列列，将最终投诉位置放在投诉标识列后面
        id_index = original_cols.index(id_column)
        new_cols = original_cols.copy()
        
        # 插入最终投诉位置列到投诉标识后面
        results = df.with_columns(final_position_col)
        
        # 如果需要重排列顺序
        if "最终投诉位置" not in original_cols:
            new_cols.insert(id_index + 1, "最终投诉位置")
            # 移除可能存在的重复列
            if "最终投诉位置" in original_cols:
                new_cols.remove("最终投诉位置")
            results = results.select(new_cols)
        
        # 创建汇总表
        summary_data = []
        for complaint_id in id_list:
            if complaint_id in top_addresses_dict:
                # 获取原始的投诉标识（使用第一条记录的标识）
                original_id = df.filter(pl.col("complaint_id") == complaint_id).get_column(id_column)[0]
                
                # 获取该编号的所有地址（原始列表）
                addresses = id_address_map.get(complaint_id, [])
                
                # 统计重复出现的地址
                address_counter = Counter(addresses)
                
                # 获取AI分析的代表性地址
                top_addresses = top_addresses_dict[complaint_id]
                
                # 添加频率信息
                freq_info = []
                for addr in top_addresses:
                    count = address_counter.get(addr, 0)
                    freq_info.append(f"{addr}(出现{count}次)")
                
                summary_data.append({
                    "投诉标识": original_id,
                    "最终投诉位置": "、".join(top_addresses),
                    "标识编号": complaint_id,
                    "地址数量": len(addresses),
                    "地址频率分析": "、".join(freq_info),
                    "原始汇总地址": "、".join(set(addresses)) if addresses else "无有效地址"
                })
        
        return results, pl.DataFrame(summary_data)
    
    def _save_interim_results(self, top_addresses_dict, processed_ids, df, id_column, id_address_map, address_count_map, silent=False):
        """保存中间结果，防止长时间处理中断导致全部丢失"""
        try:
            # 创建中间汇总表
            interim_summary = []
            for complaint_id in processed_ids:
                if complaint_id in top_addresses_dict:
                    # 获取原始的投诉标识
                    original_id_series = df.filter(pl.col("complaint_id") == complaint_id).get_column(id_column)
                    original_id = original_id_series[0] if len(original_id_series) > 0 else complaint_id
                    
                    # 获取该编号的所有地址（原始列表）
                    addresses = id_address_map.get(complaint_id, [])
                    
                    # 统计重复出现的地址
                    address_counter = Counter(addresses)
                    
                    # 获取AI分析的代表性地址
                    top_addresses = top_addresses_dict[complaint_id]
                    
                    interim_summary.append({
                        "投诉标识": original_id,
                        "最终投诉位置": "、".join(top_addresses),
                        "标识编号": complaint_id,
                        "地址数量": len(addresses)
                    })
            
            # 保存临时结果
            if interim_summary:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                temp_file = f"{self.results_folder}/临时结果_{timestamp}.xlsx"
                pl.DataFrame(interim_summary).write_excel(temp_file)
                if not silent:
                    print(f"已保存中间结果: {temp_file} ({len(interim_summary)}条记录)")
        except Exception as e:
            if not silent:
                print(f"保存中间结果时出错: {e}")
    
    def save_results(self, df, output_name=None):
        """保存处理结果"""
        if df is None or df.height == 0:
            print("没有数据可保存")
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_name:
            file_name = f"{self.results_folder}/{output_name}_{timestamp}"
        else:
            file_name = f"{self.results_folder}/处理结果_{timestamp}"
        
        try:
            # 保存为Excel
            df.write_excel(f"{file_name}.xlsx")
            print(f"结果已保存为Excel: {file_name}.xlsx")
            print(f"  - {output_name if output_name else '处理结果'}: 包含所有记录，并标记了最终投诉位置")
        except Exception as e:
            print(f"保存结果时出错: {e}")

async def main_process():
    """主程序流程"""
    start_time = time.time()
    print("=" * 50)
    print("投诉地址汇总分析工具")
    print("=" * 50)
    
    # 创建工具实例
    processor = ComplaintAddressProcessor()
    
    # 固定参数 - 直接使用硬编码的文件路径和列名，无需用户输入
    file_path = "D:\\NanChangWork\\202403-202502投诉热点明细表-修改后.xlsx"
    sheet_name = "明细"
    id_column = "投诉标识"
    address_column = "投诉位置"
    
    print(f"处理文件: {file_path}")
    print(f"工作表: {sheet_name}")
    
    # 加载数据
    data = processor.load_data(file_path, sheet_name)
    
    if data is None:
        print("无法加载数据，程序退出。")
        return
    
    # 处理并生成结果
    print(f"\n开始批量处理地址数据...")
    # 使用较大批处理大小
    results, summary = await processor.process_by_identifier(data, id_column, address_column, batch_size=20)
    
    # 保存结果
    processor.save_results(results, "更新后明细表")
    processor.save_results(summary, "地址汇总分析表")
    print("\n说明:")
    print("1. 更新后明细表: 包含所有原始记录，并在投诉标识列旁添加了'最终投诉位置'列")
    print("2. 地址汇总分析表: 汇总了每个投诉标识的地址信息，包含AI分析出的代表性地址")
    print("这些文件保存在'处理结果'文件夹中，可以直接打开查看和使用。")
    
    # 计算执行时间
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    print(f"\n处理完成！总耗时: {minutes}分{seconds}秒")

def run():
    """启动程序的入口函数"""
    # 设置事件循环策略（在Windows上需要）
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 运行异步主函数
    asyncio.run(main_process())

if __name__ == "__main__":
    run() 