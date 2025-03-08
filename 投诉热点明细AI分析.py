# coding=utf-8
import polars as pl
import os
from openai import AsyncOpenAI
import asyncio
import time
from datetime import datetime
import platform
import json

class AsyncAliyunLLM:
    """阿里云大语言模型异步API封装类"""
    
    def __init__(self, api_key=None):
        """初始化异步API客户端"""
        # 优先使用传入的API密钥，否则尝试从环境变量获取
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY", "sk-c8464e16fdc844fd8ca1399062d3c1d7")
        self.base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
        
        # 初始化异步客户端
        self.client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
    
    async def chat_async(self, prompt, model="qwq-32b", system_prompt=None):
        """异步调用模型，使用流式处理"""
        # 构建消息列表
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        try:
            # 使用流式模式调用API
            stream_response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                stream=True  # 启用流式模式
            )
            
            # 收集流式响应
            collected_content = ""
            reasoning_content = ""
            
            async for chunk in stream_response:
                # 处理流式响应的每个块
                if chunk.choices and len(chunk.choices) > 0:
                    delta = chunk.choices[0].delta
                    
                    # 处理思考过程（如果有）
                    if hasattr(delta, 'reasoning_content') and delta.reasoning_content:
                        reasoning_content += delta.reasoning_content
                    
                    # 处理实际内容
                    if delta.content:
                        collected_content += delta.content
            
            # 返回收集到的完整内容
            return collected_content.strip()
            
        except Exception as e:
            print(f"API调用错误: {str(e)}")
            raise e

class ComplaintAnalyzer:
    """投诉热点明细AI分析工具"""
    
    def __init__(self, api_key=None):
        """初始化分析器"""
        self.llm = AsyncAliyunLLM(api_key=api_key)
        self.results_folder = "分析结果"
        
        # 创建结果文件夹（如果不存在）
        if not os.path.exists(self.results_folder):
            os.makedirs(self.results_folder)
    
    def load_data(self, file_path, sheet_name="明细"):
        """加载投诉数据文件"""
        try:
            # 尝试自动检测文件类型并加载
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.csv':
                return pl.read_csv(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                return pl.read_excel(file_path, sheet_name=sheet_name)
            else:
                print(f"不支持的文件格式: {file_ext}")
                return None
        except Exception as e:
            print(f"加载数据时出错: {e}")
            return None
    
    async def extract_address_async(self, text):
        """从文本中异步提取地址"""
        if not text or (isinstance(text, str) and text.strip() == "") or pl.Series([text]).is_null().all():
            return "解析地址失败"
            
        prompt = f"""
只提取文本中明确提及的用户实际位置，忽略基站信息。仅返回地址，无说明。
注意：如果只提到基站而没有明确用户实际位置，返回"解析地址失败"。

文本: {text}

规则:
1. 必须明确提及用户在哪里，如"用户在XX地点"、"用户所在XX区域"、"客户反映在XX地点"
2. 如果有冒号，如"用户反映在青山湖区:南池路"，去除冒号并连接，输出为"青山湖区南池路"
3. 如果有中括号【】，请忽略中括号内的内容，只保留中括号前的地址
4. 如果只有基站名称而没有用户位置，返回"解析地址失败"
5. 下列情况必须返回"解析地址失败"：
   - 文本只描述基站位置和负荷情况
   - 找不到"用户得知"、"用户反映"、"用户所在"等用户位置指示词
6. 地点必须是用户实际所在地点，而非基站所在地点
7. 对于路口位置格式，保持"路名/路名(路口)"的格式

例1: "已联系用户，经核查该处5G基站JJG_SLRN_共青城市南昌工学院F宿12#3F弱电间-17[02060787]忙时负荷90%，负荷偏高导致上网慢，建议用户优先..."
输出: "解析地址失败"

例2: "联系用户得知在共青农大4栋宿舍使用，测试发现用户主占该处JJG_SLRN_共青城市江西农业大学南昌商学院4#学生公寓B栋1F2F-4[020..."
输出: "共青农大4栋宿舍"

例3: "多次联系用户无人接听，根据用户投诉内容得知在共青农大商学院5G上网较慢，核实发现该处主占的JJG_SLRN_共青城市江西农业大学南昌商学院1#学生公寓宿舍南面3..."
输出: "共青农大商学院"

例4: "用户反映在青山湖区:南池路/顺外路(路口)上网卡顿，核查用户主占5GNCH_WLRN_青云谱区华福制衣搬迁-3[01038154]忙时PRB利用率80%"
输出: "青山湖区南池路/顺外路(路口)"

例5: "客户反映在向塘镇仁胜新村5栋408室移动网络无法正常使用，用户于1月15日投诉称室内无信号BU6245VL-2[2606064]"
输出: "向塘镇仁胜新村5栋408室"

例6: "用户反应:江西理工大学莲花五栋，测试发现用户主使用NCH_SLRN_南昌市江西理工莲花五栋楼-1[02060771]小区"
输出: "江西理工大学莲花五栋"

例7: "投诉内容:江西航空职业技术学院(经开校区)-南门【江西省南昌市新建区建业大街与车塘湖路交汇处】，测试结果正常"
输出: "江西航空职业技术学院(经开校区)-南门"

例8: "用户投诉青山湖区南京东路【江西财经大学对面】经常无信号"
输出: "青山湖区南京东路"

如果无法确定用户实际位置，必须返回"解析地址失败"。
"""
        # 异步调用LLM模型
        try:
            result = await self.llm.chat_async(prompt)
            # 如果结果不是"解析地址失败"但内容很长，可能是AI添加了额外说明
            if result and result != "解析地址失败" and len(result) > 100:
                # 尝试只保留第一行内容
                first_line = result.strip().split('\n')[0]
                if len(first_line) < 100:
                    return first_line
            return result.strip()
        except Exception as e:
            print(f"地址解析出错: {e}")
            return "解析地址失败"
    
    async def process_record_async(self, row, has_reply_column, has_complaint_column, index, total):
        """异步处理单条记录"""
        print(f"[{index+1}/{total}] 处理记录...")
        
        # 获取答复口径和投诉内容
        reply_text = row.get("答复口径", "") if has_reply_column else ""
        complaint_text = row.get("投诉内容", "") if has_complaint_column else ""
        
        # 首先尝试从答复口径中提取地址
        address = "解析地址失败"
        source = "无"
        
        if reply_text and str(reply_text).strip():
            print(f"尝试从答复口径提取地址: {str(reply_text)[:80]}..." if len(str(reply_text)) > 80 else str(reply_text))
            address = await self.extract_address_async(reply_text)
            source = "答复口径"
        
        # 如果答复口径提取失败，再尝试从投诉内容提取
        if address == "解析地址失败" and complaint_text and str(complaint_text).strip():
            print(f"答复口径提取失败，尝试从投诉内容提取: {str(complaint_text)[:80]}..." if len(str(complaint_text)) > 80 else str(complaint_text))
            address = await self.extract_address_async(complaint_text)
            source = "投诉内容"
        
        if address != "解析地址失败":
            print(f"✓ 从{source}中提取到地址: {address}")
        else:
            print(f"✗ 地址提取失败")
        
        # 创建结果记录，复制所有原始列
        result_row = {}
        for key, value in row.items():
            result_row[key] = value
        
        # 添加新的列
        result_row["提取的地址"] = address
        result_row["地址来源"] = source
        
        return result_row, (address != "解析地址失败")
    
    async def process_and_extract_addresses_async(self, data, batch_size=20):
        """异步处理数据并提取地址，优先从答复口径列提取，失败则从投诉内容列提取"""
        print(f"开始处理数据并提取地址，共 {data.height} 条记录")
        
        # 检查必要的列是否存在
        columns = data.columns
        has_reply_column = "答复口径" in columns
        has_complaint_column = "投诉内容" in columns
        has_location_column = "投诉位置" in columns
        
        if not (has_reply_column or has_complaint_column):
            print("错误: 数据中既没有'答复口径'列也没有'投诉内容'列，无法提取地址。")
            return None
            
        # 创建结果列表
        results = []
        total = data.height
        processed = 0
        success_count = 0
        
        print(f"\n开始提取地址...")
        
        # 按批次处理数据
        all_rows = list(data.iter_rows(named=True))
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i+batch_size]
            batch_tasks = []
            
            # 创建批处理任务
            for j, row in enumerate(batch):
                task = self.process_record_async(
                    row, has_reply_column, has_complaint_column, i+j, total
                )
                batch_tasks.append(task)
            
            # 异步执行批处理任务
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 处理结果
            for j, result in enumerate(batch_results):
                # 处理异常情况
                if isinstance(result, Exception):
                    print(f"处理第 {i+j+1} 条记录时出错: {result}")
                    result_row = {key: value for key, value in batch[j].items()}
                    result_row["提取的地址"] = "解析地址失败"
                    result_row["地址来源"] = "处理出错"
                    results.append(result_row)
                    processed += 1
                    continue
                    
                # 正常处理
                result_row, is_success = result
                results.append(result_row)
                processed += 1
                if is_success:
                    success_count += 1
                
                # 更新投诉位置列
                if has_location_column and result_row["提取的地址"] != "解析地址失败":
                    result_row["投诉位置"] = result_row["提取的地址"]
            
            # 每批次保存一次中间结果
            self._save_interim_results(results, "addresses")
            success_rate = (success_count/processed*100) if processed > 0 else 0
            print(f"已完成 {processed}/{total} 条记录处理，成功率: {success_rate:.2f}%")
            
            # 短暂暂停，避免过快请求
            await asyncio.sleep(1)
        
        # 保存最终结果
        if results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.results_folder}/地址解析结果_{timestamp}.csv"
            updated_file = f"{self.results_folder}/更新后投诉明细_{timestamp}.xlsx"
            
            try:
                # 将结果转换为polars DataFrame
                df = pl.DataFrame(results)
                # 保存为CSV，不使用encoding参数
                df.write_csv(output_file)
                print(f"\n完整结果已保存至: {output_file}")
                
                # 保存为Excel，方便导入原系统
                df.write_excel(updated_file)
                print(f"更新后的完整数据已保存至: {updated_file}")
                
                # 输出处理统计信息
                if processed > 0:
                    success_rate = (success_count / processed) * 100
                    print(f"\n处理统计:")
                    print(f"总记录数: {total}")
                    print(f"处理记录数: {processed}")
                    print(f"成功提取地址数: {success_count}")
                    print(f"地址提取成功率: {success_rate:.2f}%")
            except Exception as e:
                print(f"保存结果时出错: {e}")
        
        return results
    
    def _save_interim_results(self, results, result_type="analysis"):
        """保存中间结果"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if result_type == "addresses":
            interim_file = f"{self.results_folder}/地址解析_中间结果_{timestamp}.csv"
        else:
            interim_file = f"{self.results_folder}/投诉分析_中间结果_{timestamp}.csv"
        
        try:
            # 将结果转换为polars DataFrame
            df = pl.DataFrame(results)
            # 保存为CSV，不使用encoding参数
            df.write_csv(interim_file)
            print(f"已保存中间结果至: {interim_file}")
        except Exception as e:
            print(f"保存中间结果时出错: {e}")


async def main_async():
    """异步主函数"""
    # 默认参数和文件路径
    file_path = "WorkDocument/投诉热点明细分析/source/202403-202502投诉热点明细表-终稿.xlsx"
    sheet_name = "明细"
    
    print("=" * 50)
    print("投诉热点明细地址自动提取工具 (异步版)")
    print("=" * 50)
    print(f"默认文件路径: {file_path}")
    print(f"默认工作表: {sheet_name}")
    
    # 创建分析器实例
    analyzer = ComplaintAnalyzer()
    
    # 加载数据
    print(f"\n正在读取Excel文件...")
    data = analyzer.load_data(file_path, sheet_name)
    
    if data is None:
        print("无法加载数据，程序退出。")
        return
    
    print(f"\n成功加载数据，共 {data.height} 条记录")
    
    # 批量大小设置
    batch_size = 20  # 增加到20条/批次
    print(f"设置批量处理大小: {batch_size}条记录/批次")
    
    # 直接执行地址提取
    print("\n开始执行地址提取...")
    await analyzer.process_and_extract_addresses_async(data, batch_size)
    
    print("\n处理完成！")


def main():
    """同步入口函数"""
    # 设置事件循环策略（在Windows上需要）
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 运行异步主函数
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
