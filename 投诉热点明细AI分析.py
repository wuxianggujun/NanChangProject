# coding=utf-8
import polars as pl
import os
from aliyun_llm import AliyunLLM
import time
from datetime import datetime

class ComplaintAnalyzer:
    """投诉热点明细AI分析工具"""
    
    def __init__(self, api_key=None):
        """初始化分析器"""
        self.llm = AliyunLLM(api_key=api_key)
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
    
    def extract_address(self, text):
        """从文本中提取地址"""
        if not text or (isinstance(text, str) and text.strip() == "") or pl.Series([text]).is_null().all():
            return "解析地址失败"
            
        prompt = f"""
只提取文本中明确提及的用户实际位置，忽略基站信息。仅返回地址，无说明。
注意：如果只提到基站而没有明确用户实际位置，返回"解析地址失败"。

文本: {text}

规则:
1. 必须明确提及用户在哪里，如"用户在XX宿舍"、"用户所在XX小区"、"客户反映在XX地点"
2. 如果只有基站名称（如"该处5G基站JJG_SLRN_共青城市南昌工学院F宿"）而没有用户位置，返回"解析地址失败"
3. 下列情况必须返回"解析地址失败"：
   - 文本只描述基站位置和负荷情况
   - 找不到"用户得知"、"用户反映"、"用户所在"等用户位置指示词
4. 地点必须是用户实际所在地点，而非基站所在地点

例1: "已联系用户，经核查该处5G基站JJG_SLRN_共青城市南昌工学院F宿12#3F弱电间-17[02060787]忙时负荷90%，负荷偏高导致上网慢，建议用户优先..."
输出: "解析地址失败"

例2: "联系用户得知在共青农大4栋宿舍使用，测试发现用户主占该处JJG_SLRN_共青城市江西农业大学南昌商学院4#学生公寓B栋1F2F-4[020..."
输出: "共青农大4栋宿舍"

例3: "多次联系用户无人接听，根据用户投诉内容得知在共青农大商学院5G上网较慢，核实发现该处主占的JJG_SLRN_共青城市江西农业大学南昌商学院1#学生公寓宿舍南面3..."
输出: "共青农大商学院"

例4: "已联系用户，经核查无法明确用户位置，检测发现该处5G基站JJG_SLRN_共青城市南昌工学院F宿12#3F-17信号正常，负荷低，容量充足..."
输出: "解析地址失败"

例5: "客户反映在向塘镇仁胜新村5栋408室移动网络无法正常使用，用户于1月15日投诉称室内无信号BU6245VL-2[2606064]"
输出: "向塘镇仁胜新村5栋408室"

如果无法确定用户实际位置，必须返回"解析地址失败"。
"""
        # 使用不显示流式输出的方式获取地址
        try:
            result = self.llm.chat_without_streaming_display(prompt)
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
    
    def process_and_extract_addresses(self, data):
        """处理数据并提取地址，优先从答复口径列提取，失败则从投诉内容列提取"""
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
        
        for i, row in enumerate(data.iter_rows(named=True)):
            print(f"[{i+1}/{total}] 处理记录...")
            
            # 获取答复口径和投诉内容
            reply_text = row.get("答复口径", "") if has_reply_column else ""
            complaint_text = row.get("投诉内容", "") if has_complaint_column else ""
            
            # 首先尝试从答复口径中提取地址
            address = "解析地址失败"
            source = "无"
            
            if reply_text and str(reply_text).strip():
                print(f"尝试从答复口径提取地址: {str(reply_text)[:80]}..." if len(str(reply_text)) > 80 else str(reply_text))
                address = self.extract_address(reply_text)
                source = "答复口径"
            
            # 如果答复口径提取失败，再尝试从投诉内容提取
            if address == "解析地址失败" and complaint_text and str(complaint_text).strip():
                print(f"答复口径提取失败，尝试从投诉内容提取: {str(complaint_text)[:80]}..." if len(str(complaint_text)) > 80 else str(complaint_text))
                address = self.extract_address(complaint_text)
                source = "投诉内容"
            
            processed += 1
            if address != "解析地址失败":
                success_count += 1
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
            
            # 更新投诉位置列
            if has_location_column and address != "解析地址失败":
                result_row["投诉位置"] = address
            
            results.append(result_row)
            
            # 每10条保存一次中间结果
            if (i+1) % 10 == 0:
                self._save_interim_results(results, "addresses")
                
            # 避免API请求过快
            time.sleep(0.5)
        
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

    def analyze_complaint_text(self, text):
        """分析单条投诉文本"""
        prompt = f"""
请作为专业的客户投诉分析师，对以下投诉内容进行详细分析：

投诉内容: {text}

请提供以下分析结果:
1. 投诉类型和主要问题点
2. 投诉原因分析
3. 情感倾向判断（正面/负面/中性）
4. 投诉严重程度评估（低/中/高）
5. 建议的处理方案

请确保分析客观、专业且有实用价值。
"""
        # 使用不显示流式输出的方式获取分析结果
        return self.llm.chat_without_streaming_display(prompt)
    
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


def main():
    # 默认参数和文件路径
    file_path = "WorkDocument/投诉热点明细分析/source/202403-202502投诉热点明细表-终稿.xlsx"
    sheet_name = "明细"
    
    print("=" * 50)
    print("投诉热点明细地址自动提取工具")
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
    
    # 直接执行地址提取
    print("\n开始执行地址提取...")
    analyzer.process_and_extract_addresses(data)
    
    print("\n处理完成！")


if __name__ == "__main__":
    main()
