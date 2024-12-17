import os

def find_file(filename, search_path):
    # 遍历指定路径及子路径
    for root, dirs, files in os.walk(search_path):
        if filename in files:
            return os.path.join(root, filename)
    return None

if __name__ == '__main__':
    filename = 'EntBase.dat'
    search_paths = [r"D:\\",r"C:\\"]
    file_path = None
    
    for path in search_paths:
        file_path = find_file(filename,path)
        if file_path:
            break

    if file_path:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        for i in range(len(lines)):
            if 'uienable=' in lines[i]:
                lines[i] = 'uienable=0\n'
            elif 'qtenable=' in lines[i]:
                lines[i] = 'qtenable=0\n'
            elif 'uipass=' in lines[i]:
                lines[i] = 'uipass=\n'
            elif 'qtpass=' in lines[i]:
                lines[i] = 'qtpass=\n'
     
        with open(file_path, 'w', encoding='utf-8') as file:
            file.writelines(lines)

        print(f'文件 {file_path} 已经修改成功，现在可以在托盘右键进行退出奇安信了')
    else:
        print(f'未找到文件 {filename}')