# coding=utf-8
import polars as pl
import os
from aliyun_llm import AliyunLLM
import time
from datetime import datetime

class AddressExtractor:
    """投诉热点明细地址提取工具"""
    
    def __init__(self, api_key=None):
        """初始化地址提取器"""
        self.llm = AliyunLLM(api_key=api_key)
        self.results_folder = "地址解析结果"
        
        # 创建结果文件夹（如果不存在）
        if not os.path.exists(self.results_folder):
            os.makedirs(self.results_folder)
    
    def load_data(self, file_path):
        """使用polars加载数据文件"""
        try:
            # 尝试自动检测文件类型并加载
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.csv':
                return pl.read_csv(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                return pl.read_excel(file_path)
            else:
                print(f"不支持的文件格式: {file_ext}")
                return None
        except Exception as e:
            print(f"加载数据时出错: {e}")
            return None
    
    def extract_address(self, text):
        """使用AI从文本中提取地址"""
        if not text or text.strip() == "" or pl.Series([text]).is_null().any():
            return "解析地址失败"
            
        prompt = f"""
请从以下文本中提取地址信息。只需返回地址，不要包含任何其他内容。
如果无法识别出地址，请只回复"解析地址失败"，不要有任何其他解释。

文本: {text}

例如：
1. 如果文本是"答复口径：用户反映江西省南昌市西湖区丰和北大道与长春路交界处移动信号差"，则只返回"江西省南昌市西湖区丰和北大道与长春路交界处"
2. 如果文本是"答复口径：中国移动南昌红谷滩10086营业厅用户反馈网络问题"，则只返回"南昌红谷滩"
3. 如果文本是"投诉内容：南昌青山湖区政务中心无法办理业务"，则只返回"南昌青山湖区政务中心"

请注意，我只需要提取出准确的地址信息，不需要额外的说明或解释。如果文本中包含多个地址，请提取最主要的一个地址。
如果无法识别出任何地址，只返回"解析地址失败"四个字。
"""
        # 使用大模型解析地址
        try:
            result = self.llm.chat_without_streaming_display(prompt)
            return result.strip()
        except Exception as e:
            print(f"地址解析出错: {e}")
            return "解析地址失败"
    
    def batch_extract_addresses(self, data, text_column, limit=None):
        """批量提取地址"""
        if data is None or len(data) == 0:
            print("没有数据可供分析")
            return
        
        # 获取总行数
        total_rows = data.height
        print(f"开始批量解析地址，共 {total_rows} 条记录")
        
        if limit:
            print(f"已设置解析上限: {limit} 条")
            data = data.slice(0, limit)
        
        # 创建结果列表
        results = []
        
        # 开始解析
        total = data.height
        for i, row in enumerate(data.iter_rows(named=True)):
            text = row.get(text_column, "")
            
            print(f"\n[{i+1}/{total}] 正在解析地址...")
            print(f"原始文本: {text[:100]}..." if len(str(text)) > 100 else text)
            
            # 跳过空内容
            if not text or str(text).strip() == "":
                print("跳过空内容")
                result_row = {**row}
                result_row["提取的地址"] = "解析地址失败"
                results.append(result_row)
                continue
                
            try:
                # 解析地址
                address = self.extract_address(text)
                print(f"提取结果: {address}")
                
                # 添加到结果
                result_row = {**row}
                result_row["提取的地址"] = address
                results.append(result_row)
                
                # 每5条保存一次中间结果
                if (i+1) % 5 == 0:
                    self._save_interim_results(results)
                    
                # 避免API请求过快
                time.sleep(1)
                
            except Exception as e:
                print(f"解析第 {i+1} 条记录时出错: {e}")
                result_row = {**row}
                result_row["提取的地址"] = "解析地址失败"
                results.append(result_row)
        
        # 保存最终结果
        self._save_final_results(results)
        return results
    
    def _save_interim_results(self, results):
        """保存中间结果"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        interim_file = f"{self.results_folder}/地址解析_中间结果_{timestamp}.csv"
        
        try:
            # 将结果转换为polars DataFrame
            df = pl.DataFrame(results)
            # 保存为CSV
            df.write_csv(interim_file)
            print(f"已保存中间结果至: {interim_file}")
        except Exception as e:
            print(f"保存中间结果时出错: {e}")
    
    def _save_final_results(self, results):
        """保存最终结果"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_file = f"{self.results_folder}/地址解析_最终结果_{timestamp}.csv"
        
        try:
            # 将结果转换为polars DataFrame
            df = pl.DataFrame(results)
            # 保存为CSV
            df.write_csv(final_file)
            print(f"\n解析完成! 结果已保存至: {final_file}")
            
            # 创建一个只包含地址的简化版本
            simple_file = f"{self.results_folder}/地址解析_简化结果_{timestamp}.csv"
            df.select(["提取的地址"]).write_csv(simple_file)
            print(f"简化结果已保存至: {simple_file}")
        except Exception as e:
            print(f"保存最终结果时出错: {e}")


def main():
    print("=" * 50)
    print("投诉热点明细地址解析工具")
    print("=" * 50)
    print("\n本工具使用阿里云大模型API解析投诉热点明细中的地址信息。")
    
    # 创建解析器实例
    extractor = AddressExtractor()
    
    # 直接指定文件路径
    file_path = "WorkDocument/投诉热点明细分析/source/202403-202502投诉热点明细表-终稿.xlsx"
    print(f"\n将读取文件: {file_path}")
    
    # 尝试加载数据
    data = extractor.load_data(file_path)
    
    if data is None:
        print("无法加载数据，程序退出。")
        return
        
    # 显示数据概览
    print(f"\n成功加载数据，共 {data.height} 条记录")
    print("\n数据前5行预览:")
    print(data.head())
    
    # 获取列名
    columns = data.columns
    print("\n可用列:")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    
    # 直接使用"答复口径"列，如果存在的话
    text_column = "答复口径"
    if text_column not in columns:
        text_column = input("\n'答复口径'列不存在，请输入包含地址信息的列名: ")
        while text_column not in columns:
            print(f"列名 '{text_column}' 不存在，请重新输入。")
            text_column = input("请输入包含地址信息的列名: ")
    
    # 询问解析数量
    limit_input = input("\n要解析的记录数量 (直接回车表示全部): ")
    limit = int(limit_input) if limit_input.strip() else None
    
    # 执行批量解析
    results = extractor.batch_extract_addresses(data, text_column, limit)
    
    print("\n解析完成！")


if __name__ == "__main__":
    main() 