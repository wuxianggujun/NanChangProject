# coding=utf-8
from aliyun_llm import AliyunLLM
import pandas as pd
import os
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
    
    def load_data(self, file_path):
        """加载投诉数据文件"""
        try:
            # 尝试自动检测文件类型并加载
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.csv':
                return pd.read_csv(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                return pd.read_excel(file_path)
            else:
                print(f"不支持的文件格式: {file_ext}")
                return None
        except Exception as e:
            print(f"加载数据时出错: {e}")
            return None
    
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
    
    def analyze_complaint_batch(self, complaints_data, text_column, 
                               limit=None, output_format='csv'):
        """批量分析投诉文本"""
        if complaints_data is None or len(complaints_data) == 0:
            print("没有数据可供分析")
            return
        
        print(f"开始批量分析投诉数据，共 {len(complaints_data)} 条记录")
        if limit:
            print(f"已设置分析上限: {limit} 条")
            complaints_data = complaints_data.head(limit)
        
        # 创建结果数据框
        results = []
        
        # 开始分析
        total = len(complaints_data)
        for i, row in enumerate(complaints_data.itertuples(), 1):
            complaint_text = getattr(row, text_column) if hasattr(row, text_column) else "无内容"
            
            print(f"\n[{i}/{total}] 正在分析投诉...")
            
            # 跳过空内容
            if pd.isna(complaint_text) or str(complaint_text).strip() == "":
                print("跳过空内容")
                continue
                
            try:
                # 分析投诉内容
                analysis = self.llm.chat_without_streaming_display(
                    f"分析这条客户投诉并提取关键信息:\n\n{complaint_text}\n\n" +
                    "请提供JSON格式的分析结果，包含以下字段：" +
                    "投诉类型，主要问题，情感倾向，严重程度，建议处理方案"
                )
                
                # 创建结果记录
                result_row = {col: getattr(row, col) for col in complaints_data.columns}
                result_row["AI分析结果"] = analysis
                results.append(result_row)
                
                # 每5条保存一次中间结果
                if i % 5 == 0:
                    self._save_interim_results(results, output_format)
                    
                # 避免API请求过快
                time.sleep(1)
                
            except Exception as e:
                print(f"分析第 {i} 条记录时出错: {e}")
        
        # 保存最终结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.results_folder}/投诉分析结果_{timestamp}"
        
        if output_format == 'csv':
            pd.DataFrame(results).to_csv(f"{filename}.csv", index=False, encoding='utf-8-sig')
        else:
            pd.DataFrame(results).to_excel(f"{filename}.xlsx", index=False)
            
        print(f"\n分析完成! 结果已保存至: {filename}.{output_format}")
        return results
    
    def _save_interim_results(self, results, output_format):
        """保存中间结果"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        interim_file = f"{self.results_folder}/中间结果_{timestamp}"
        
        if output_format == 'csv':
            pd.DataFrame(results).to_csv(f"{interim_file}.csv", index=False, encoding='utf-8-sig')
        else:
            pd.DataFrame(results).to_excel(f"{interim_file}.xlsx", index=False)
    
    def generate_summary_report(self, analyzed_data):
        """生成总结报告"""
        if not analyzed_data or len(analyzed_data) == 0:
            print("没有数据可供生成报告")
            return ""
            
        all_analysis = "\n".join([row.get("AI分析结果", "") for row in analyzed_data if "AI分析结果" in row])
        
        prompt = f"""
我有一批客户投诉数据的AI分析结果，请基于这些分析生成一份总结报告。

以下是部分分析结果样本:
{all_analysis[:3000]}  # 限制提示长度

请生成一份包含以下内容的总结报告:
1. 投诉热点分析 - 最常见的投诉类型和问题
2. 问题趋势 - 是否有明显的投诉模式或趋势
3. 严重程度分析 - 各级别投诉的分布情况
4. 改进建议 - 基于分析结果提出的具体改进措施
5. 执行摘要 - 对所有发现的简明总结

请尽可能具体和有操作性，以便管理层可以制定相应的改进计划。
"""
        print("\n正在生成总结报告...")
        report = self.llm.chat_without_streaming_display(prompt)
        
        # 保存报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"{self.results_folder}/投诉总结报告_{timestamp}.txt"
        
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)
            
        print(f"报告已生成并保存至: {report_file}")
        return report


def main():
    print("=" * 50)
    print("投诉热点明细AI分析工具")
    print("=" * 50)
    print("\n本工具使用阿里云大模型API分析客户投诉数据，提取关键信息并生成分析报告。")
    
    # 创建分析器实例
    analyzer = ComplaintAnalyzer()
    
    # 询问用户输入
    data_path = input("\n请输入投诉数据文件路径 (Excel或CSV格式): ")
    
    # 尝试加载数据
    data = analyzer.load_data(data_path)
    
    if data is None:
        print("无法加载数据，程序退出。")
        return
        
    # 显示数据概览
    print(f"\n成功加载数据，共 {len(data)} 条记录")
    print("\n数据前5行预览:")
    print(data.head())
    
    # 让用户选择文本列
    print("\n可用列:")
    for i, col in enumerate(data.columns):
        print(f"{i+1}. {col}")
        
    col_idx = int(input("\n请选择包含投诉内容的列编号: ")) - 1
    text_column = data.columns[col_idx]
    
    # 询问分析数量
    limit_input = input("\n要分析的记录数量 (直接回车表示全部): ")
    limit = int(limit_input) if limit_input.strip() else None
    
    # 询问输出格式
    output_format = input("\n输出格式 (csv/xlsx, 默认csv): ").lower() or 'csv'
    if output_format not in ['csv', 'xlsx']:
        output_format = 'csv'
    
    # 执行批量分析
    results = analyzer.analyze_complaint_batch(data, text_column, limit, output_format)
    
    # 询问是否生成总结报告
    if results:
        generate_report = input("\n是否生成总结报告? (y/n): ").lower() == 'y'
        if generate_report:
            analyzer.generate_summary_report(results)
    
    print("\n分析完成！")


if __name__ == "__main__":
    main()
