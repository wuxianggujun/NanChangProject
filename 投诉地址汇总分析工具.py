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
                
        # 去除开头的行政区划（省、市、区、县等）
        address = re.sub(r'^(江西省|江西|南昌市|南昌|九江市|九江|赣州市|赣州|上饶市|上饶|吉安市|吉安|抚州市|抚州|宜春市|宜春|景德镇市|景德镇|萍乡市|萍乡|新余市|新余|鹰潭市|鹰潭)?\s*', '', address)
        address = re.sub(r'^([\w]+(?:省|市|区|县|镇|乡))\s*', '', address)
        
        # 清理额外空格
        address = re.sub(r'\s+', '', address)
        
        return address
    
    async def analyze_addresses(self, address_list, timeout=30):
        """分析地址列表，找出最多3个最常见/最具代表性的地址"""
        if not address_list:
            return ["无有效地址"]
        
        # 准备原始地址列表和频率统计
        counter = Counter(address_list)
        
        # 打印原始地址列表
        print("\n==================== 原始地址列表 ====================")
        for addr, count in counter.most_common():
            print(f"  {addr} (出现{count}次)")
        
        # 使用AI进行分析
        prompt = f"""
请严格按照以下规则分析投诉地址列表：

## 基本任务
1. 识别相似地址并合并统计频率
2. 选择最详细的地址作为代表
3. 剔除行政区划前缀和结尾方位词
4. 按真实频率排序选择最具代表性的地址

## 重要规则
1. **频率统计必须准确**：
   - 严格按照原始出现次数统计，不要人为增加频率
   - 相似地址合并时，准确累加原始频率

2. **地址选择规则**：
   - 当所有地址频率均为1次时，只选择1个最具体的地址
   - 当有地址频率>1次时，按频率排序选择最多3个地址
   - 绝不输出冗余地址（如父子关系地址）

3. **相似地址判断标准**：
   - 父子关系：如"南昌大学"与"南昌大学前湖校区"属于父子关系
   - 别名关系：如"江西师大"与"江西师范大学"属于别名关系
   - 行政区划变化：如"湾里区冯翊小区"与"新建区冯翊小区"指同一地点

## 输出要求
- 每行输出一个地址，最多3个地址
- 不包含频率信息或解释
- 已去除行政区划前缀和方位词后缀

原始投诉地址列表（标注原始出现次数）：
{', '.join([f"{addr} (出现{count}次)" for addr, count in counter.most_common()])}

请记住：当所有地址都只出现1次时，只输出1个最具体的地址。
"""
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
        
        # 打印AI回复的完整内容
        print("\n==================== AI回复内容 ====================")
        print(result.strip())
        
        # 处理结果，返回最多3个地址
        addresses = []
        for line in result.strip().split('\n'):
            line = line.strip()
            # 去除可能的频率信息和序号
            line = re.sub(r'\s*\(出现\d+次\)\s*', '', line)
            line = re.sub(r'^\d+[\.\s、]+', '', line)  # 移除可能的序号
            if line and len(addresses) < 3:
                # 检查是否AI已经使用了顿号分隔
                if "、" in line and len(addresses) == 0:
                    addresses = [re.sub(r'\s*\(出现\d+次\)\s*', '', addr.strip()) for addr in line.split("、")][:3]
                    break
                addresses.append(line)
        
        # 打印最终选择的结果
        print("\n==================== 最终选择结果 ====================")
        for addr in addresses[:3]:
            print(f"  {addr}")
        print("============================================================\n")
        
        return addresses[:3]

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
            complaint_id, result = await task
            results[complaint_id] = result
            
            # 每完成一个任务就更新进度
            if progress:
                progress.update(1)
                
        return results
    
    async def _analyze_address_task(self, complaint_id, address_list):
        """单个地址分析任务"""
        print(f"\n处理投诉标识编号: {complaint_id} ====================")
        top_addresses = await self.ai.analyze_addresses(address_list)
        return complaint_id, top_addresses
    
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
        
        for complaint_id in id_list:
            # 获取该编号的所有地址（不去重）
            addresses = valid_records.filter(pl.col("complaint_id") == complaint_id).get_column(address_column).to_list()
            # 保存完整地址列表（不去重）
            id_address_map[complaint_id] = addresses
        
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
                                        df, id_column, id_address_map, silent=True)
        
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
                
                # 获取AI分析的代表性地址
                top_addresses = top_addresses_dict[complaint_id]
                
                # 统计原始频率（仅用于显示）
                raw_counter = Counter(addresses)
                
                # 添加频率信息
                freq_info = []
                for addr in top_addresses:
                    # 尝试找到相似的原始地址及其频率
                    norm_addr = self.ai.normalize_address(addr)
                    total_count = 0
                    for orig_addr, count in raw_counter.items():
                        # 规范化原始地址
                        norm_orig = self.ai.normalize_address(orig_addr)
                        # 如果规范化后地址相似，累加次数
                        if norm_addr == norm_orig or norm_addr in norm_orig or norm_orig in norm_addr:
                            total_count += count
                    
                    if total_count > 0:
                        freq_info.append(f"{addr}(出现{total_count}次)")
                    else:
                        # 尝试直接匹配
                        direct_count = sum(1 for orig_addr in addresses if addr in orig_addr or orig_addr in addr)
                        if direct_count > 0:
                            freq_info.append(f"{addr}(出现{direct_count}次)")
                        else:
                            freq_info.append(f"{addr}")  # 不显示次数
                
                summary_data.append({
                    "投诉标识": original_id,
                    "最终投诉位置": "、".join(top_addresses),
                    "标识编号": complaint_id,
                    "地址数量": len(addresses),
                    "地址频率分析": "、".join(freq_info),
                    "原始汇总地址": "、".join(set(addresses)) if addresses else "无有效地址"
                })
        
        return results, pl.DataFrame(summary_data)
    
    def _save_interim_results(self, top_addresses_dict, processed_ids, df, id_column, id_address_map, silent=False):
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
    print("   - AI识别并合并了相似地址，计算总频率")
    print("   - 按合并后的频率从高到低排序，并去除冗余地址")
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