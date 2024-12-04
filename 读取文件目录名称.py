import os
import polars as pl

def get_folder_names(directory_path):
    # 获取目录下所有文件夹名称
    folders = []
    for item in os.listdir(directory_path):
        full_path = os.path.join(directory_path, item)
        if os.path.isdir(full_path):
            folders.append({"文件夹名称": item})
    return folders

if __name__ == '__main__':
    # 设置要扫描的目录路径
    directory_path = input("请输入要扫描的目录路径: ")
    # 设置输出的Excel文件路径
    output_file = input("请输入保存的Excel文件路径(例如: folders.xlsx): ")
    
    # 获取所有文件夹名称
    folders = get_folder_names(directory_path)
    
    # 创建 polars DataFrame
    df = pl.DataFrame(folders)
    
    # 保存到Excel文件
    df.write_excel(output_file)
    print(f"已成功将{len(folders)}个目录名称保存到 {output_file}")