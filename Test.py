import pandas as pd
from tool.file import FileManager

if __name__ == '__main__':
    base_dir = "WorkDocument\\23G精简投诉明细预处理脚本"
    file_manager = FileManager(base_dir)
    latest_file_path = file_manager.get_latest_file("source")
    df = pd.read_excel(latest_file_path)
    print(f"最新文件路径: {latest_file_path}")
    
    # output_file = file_manager.save_to_excel(df, "测试")
    # file_manager.save_to_sheet("测试",原始数据=df, 处理后数据=df)
    file_manager.save_to_sheet("测试")
