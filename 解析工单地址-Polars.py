from typing import Dict
from tool.data import AddressParser
from tool.file import FileManager
import polars as pl


def process_complaints(df: pl.DataFrame) -> pl.DataFrame:
    """处理投诉数据并提取地址信息"""
    parser = AddressParser()
    
    def get_address(row: Dict) -> str:
        """按优先级获取地址"""
        # 优先级：投诉地址 > 回复客服内容 > 区域 > 投诉内容
        for field in ['投诉地址', '回复客服内容', '区域', '投诉内容']:
            if field in row and row[field] and str(row[field]) != '无':
                address = parser.extract_address(str(row[field]))
                if address:
                    return address
        return ''
    
    # 处理数据
    rows = df.to_dicts()
    addresses = [get_address(row) for row in rows]
    
    # 添加新列
    return df.with_columns([
        pl.Series('完整地址', addresses)
    ])

if __name__ == '__main__':
    file_manager = FileManager("WorkDocument/工单地址解析")
    source_file = file_manager.get_latest_file("source")
    
    print(f"正在读取文件：{source_file}")
    
    # 读取数据
    df = file_manager.read_excel(source_file)
    
    # 处理数据
    result_df = process_complaints(df)
    
    # 保存结果
    file_manager.save_to_excel(result_df, "解析地址数据")
    
    # 显示部分结果
    print("\n地址解析结果示例：")
    print(result_df.select(['投诉地址', '区域', '完整地址']).head(10))