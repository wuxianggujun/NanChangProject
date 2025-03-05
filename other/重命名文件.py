import os
from datetime import datetime
import time


def rename_files_by_modified_date(directory):
    # 确保目录存在
    if not os.path.exists(directory):
        print(f"目录 {directory} 不存在!")
        return

    # 获取目录中的所有文件
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

    for file in files:
        file_path = os.path.join(directory, file)

        # 获取文件修改时间
        modified_time = os.path.getmtime(file_path)
        date_modified = datetime.fromtimestamp(modified_time)

        # 获取文件扩展名
        file_extension = os.path.splitext(file)[1]

        # 新文件名：修改日期
        new_filename = date_modified.strftime("%Y%m%d") + file_extension
        new_file_path = os.path.join(directory, new_filename)

        # 如果新文件名已存在，添加序号
        counter = 1
        while os.path.exists(new_file_path):
            new_filename = f"{date_modified.strftime('%Y%m%d')}_{counter}{file_extension}"
            new_file_path = os.path.join(directory, new_filename)
            counter += 1

        try:
            os.rename(file_path, new_file_path)
            print(f"已将文件 {file} 重命名为 {new_filename}")
        except Exception as e:
            print(f"重命名文件 {file} 时出错: {str(e)}")


# 使用示例
if __name__ == "__main__":
    # 在这里指定要处理的目录路径
    target_directory = r"C:\Users\wuxianggujun\CodeSpace\PycharmProjects\NanChangProject\WorkDocument\重复投诉日报\source"
    rename_files_by_modified_date(target_directory)