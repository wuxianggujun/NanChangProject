import base64
import datetime
import json
import os
import subprocess
import psutil
import pyperclip
from pywinauto.application import Application
from pywinauto import Desktop, keyboard
import time
import requests
import re
from pywinauto.keyboard import send_keys
from selenium import webdriver
from selenium.common import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options  # 使用 Edge 的 Options 类
from selenium.webdriver.edge.service import Service  # 导入 Service 类
from selenium.webdriver.support.wait import WebDriverWait

def get_gotify_message(gotify_url, client_token, timeout=180):
    """
    从 Gotify 服务器获取最新消息，并验证消息时间。

    Args:
        gotify_url: Gotify 服务器的 URL
        client_token: Gotify 客户端的 token
        timeout: 等待消息的超时时间 (秒)

    Returns:
        如果获取到有效验证码，返回验证码 (字符串)。
        如果在超时时间内未获取到有效验证码，返回 None。
    """
    start_time = time.time()
    headers = {
        'Authorization': f'Bearer {client_token}',
    }

    params = {
        'limit': 10,
        'since': int(start_time - timeout),  # 获取 180 秒内的消息
    }

    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{gotify_url}/message", headers=headers, params=params, timeout=5)
            response.raise_for_status()

            messages = response.json().get("messages", [])
            if messages:
                messages.sort(key=lambda x: x.get("date"), reverse=True)

                for message in messages:
                    message_content = message.get("message")
                    message_date_str = message.get("date")

                    # 提取接收时间和验证码
                    receive_time_str = None
                    verification_code = None

                    lines = message_content.splitlines()  # 将消息内容按行分割

                    for line in lines:
                        if line.startswith("接收时间:"):
                            receive_time_str = line.split("接收时间:")[1].strip()
                        elif line.startswith("短信内容:"):
                            # 修改正则表达式以匹配新的短信内容格式
                            match = re.search(r"登录动态验证码：(\d{6})", line)
                            if match:
                                verification_code = match.group(1)

                    # 验证接收时间
                    if receive_time_str and verification_code:
                        try:
                            # 解析接收时间字符串
                            receive_time = datetime.datetime.strptime(receive_time_str, "%Y-%m-%d %H:%M:%S")
                            # 将解析后的时间转换为时间戳
                            receive_time_timestamp = int(time.mktime(receive_time.timetuple()))
                            # 检查消息时间是否在有效范围内 (例如，2 分钟内)
                            time_diff = time.time() - receive_time_timestamp
                            if 0 <= time_diff <= 600:
                                print(f"获取到验证码: {verification_code}, 接收时间: {receive_time_str}")
                                return verification_code
                            else:
                                print(f"接收时间不在有效范围内: {receive_time_str}")

                        except ValueError:
                            print(f"解析接收时间失败: {receive_time_str}")

                print("当前没有有效范围内的验证码消息")
            else:
                print("当前没有消息")

        except requests.exceptions.RequestException as e:
            print(f"获取 Gotify 消息失败: {e}")
        time.sleep(2)  # 等待 2 秒再重试

    return None


def is_remote_debugging_port_open(port):
    """检查远程调试端口是否已打开且可以连接。

    Args:
        port: 远程调试端口号。

    Returns:
        如果端口已打开且可以连接，返回 True；否则返回 False。
    """
    options = Options()
    options.debugger_address = f"localhost:{port}"
    driver = None  # 初始化 driver 变量
    try:
        # 尝试连接到远程调试端口
        driver = webdriver.Edge(options=options)
        return True
    except WebDriverException:
        return False
    finally:
        if driver:  # 只有 driver 成功初始化才执行 quit()
            driver.quit()


def find_edge_process(user_data_dir=None):
    """查找 Edge 进程，可以根据 user-data-dir 参数来查找特定的 Edge 实例。

    Args:
        user_data_dir: 要查找的 Edge 实例使用的 user-data-dir 路径。

    Returns:
        如果找到 Edge 进程，返回该进程的 psutil.Process 对象；否则返回 None。
    """
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if "msedge" in process.info['name'].lower():
                if user_data_dir:
                    if any(user_data_dir in arg for arg in process.info['cmdline']):
                        return process
                else:
                    return process
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return None


def start_edge_with_remote_debugging(port=9222, user_data_dir=r"D:\python\seleniumEdge", timeout=10):
    """启动 Edge 浏览器并开启远程调试。

    Args:
        port: 远程调试端口号。
        user_data_dir: 用户数据目录。
        timeout: 启动超时时间（秒）。

    Returns:
        如果成功启动 Edge 并开启远程调试，返回 True；否则返回 False。
    """
    edge_path = find_edge_path()
    if not edge_path:
        raise Exception("未找到 Microsoft Edge浏览器，请手动指定路径")

    # 尝试连接到已存在的实例
    if is_remote_debugging_port_open(port):
        print(f"远程调试端口 {port} 已打开,尝试连接")
        try:
            options = Options()
            options.debugger_address = f"localhost:{port}"
            driver = webdriver.Edge(options=options)
            driver.quit()
            print(f"成功连接到已存在的 Edge 实例，端口：{port}")
            return True
        except WebDriverException:
            print(f"连接到已存在的 Edge 实例失败，端口：{port}")

    # 查找目标实例
    edge_process = find_edge_process(user_data_dir)
    if edge_process:
        print("检测到已存在的目标 Edge 进程")
        if is_remote_debugging_port_open(port):
            print(f"远程调试端口 {port} 已打开,尝试连接")
            return True
        else:
            print("远程调试端口未打开或无法连接，将尝试重启 Edge")
            # 杀死已存在的目标 Edge 进程
            edge_process.terminate()
            edge_process.wait()  # 等待进程完全终止
            print("已关闭已存在的目标 Edge 进程")

    command = [edge_path, f"--remote-debugging-port={port}", f"--user-data-dir={user_data_dir}"]
    print(command)

    try:
        process = subprocess.Popen(command)
        print(f"已启动 Edge，远程调试端口: {port}")

        # 等待并验证远程调试端口是否打开
        start_time = time.time()
        while time.time() - start_time < timeout:
            if is_remote_debugging_port_open(port):
                print(f"远程调试端口 {port} 已成功打开")
                return True
            time.sleep(1)  # 短暂休眠，避免 CPU 占用过高

        print(f"在 {timeout} 秒内，远程调试端口 {port} 未能成功打开")
        return False

    except Exception as e:
        print(f"启动 Edge 失败: {e}")
        return False


def find_edge_path():
    """自动查找 Edge 浏览器的安装路径。"""
    # 常见的 Edge 安装路径
    edge_paths = [
        "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
        "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
        # 添加其他可能的路径...
    ]

    for path in edge_paths:
        if os.path.exists(path):
            return path

    return None


if __name__ == '__main__':
    # 启动带有远程调试端口的Edge浏览器
    if start_edge_with_remote_debugging(port=9222):
        print("Edge 启动成功！")
        options = Options()
        options.add_experimental_option("debuggerAddress", "localhost:9222")

        # 创建 WebDriver 对象，连接到已打开的 Edge 浏览器
        driver = webdriver.Edge(options=options)

        # 设置 JavaScript 异步脚本的超时时间为 30 秒 (根据需要调整)
        driver.set_script_timeout(30)

        # 访问指定 URL
        target_url = "http://10.186.254.225:10010/mcsnr/#/orderManage/complaints/orderQueryList"
        
        if driver.current_url != target_url:
            driver.get(target_url)
            print(f"已打开页面：{target_url}")

        wait = WebDriverWait(driver, 10)
        element = wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # 获取账号和密码输入框以及登录按钮
        username_input = driver.find_element(By.ID, "normal_login_username")
        password_input = driver.find_element(By.ID, "normal_login_password")
        login_button = driver.find_element(By.CSS_SELECTOR, ".login-form-button")

        # 清空账号和密码输入框
        username_input.clear()
        password_input.clear()

        # 填写账号和密码
        username_input.send_keys("17507042051") 
        password_input.send_keys("KF79189090dsp")

        # 点击获取验证码按钮
        get_code_button = driver.find_element(By.CSS_SELECTOR, ".codeLineBth--hjFKm")
        get_code_button.click()
        print("已点击获取验证码按钮")

        # 获取 Gotify 消息
        gotify_url = "http://wuxianggujun.com:40266"  # 替换为你的 Gotify 服务器地址
        client_token = "C5rHlCcPLWwLT3_"  # 替换为你的 Gotify 客户端的 token (需要创建客户端并获取 token)
        verification_code = get_gotify_message(gotify_url, client_token, timeout=180)

        if verification_code:
            # 输入验证码
            verification_code_input = driver.find_element(By.CSS_SELECTOR, ".codeLine--OTnML input.ant-input")
            verification_code_input.send_keys(verification_code)
            print(f"已输入验证码: {verification_code}")

            # 点击登录按钮
            login_button.click()
            print("已点击登录按钮")
        else:
            print("未获取到验证码或验证码已过期")

    else:
        print("Edge 启动失败！")

# if __name__ == '__main__':
# 
#     # 获取 Gotify 消息
#     gotify_url = "http://wuxianggujun.com:40266"  # 替换为你的 Gotify 服务器地址
#     # app_token  = "A7LY0EtgIdoNFTt"  # 替换为你的 Gotify 应用程序 token
#     client_token = "Cm0Z974k0.9ScEB"  # 替换为你的 Gotify 客户端的 token (需要创建客户端并获取 token)
# 
#     # 启动应用程序
#     app = Application(backend='uia').start(r"D:\Program Files\EnUES\EnUES.exe")
# 
#     # 连接到应用程序窗口
#     dlg = Desktop(backend="uia").window(title="江西零信任")  # 可以选择添加found_index=0来指定第一个找到的窗口
#     dlg.wait('ready', timeout=20, retry_interval=0.5)
# 
#     try:
#         # 使用 title="登 录" (注意中间的空格)
#         login_button = dlg.child_window(title="登 录", control_type="Button")
#         login_button.wait('ready', timeout=10, retry_interval=0.5)  # 等待按钮可点击
#         login_button.click_input()
#         print("已点击登录按钮")  # 加一个打印方便你判断是否执行
# 
#         phone_verify_static = dlg.child_window(title="手机验证", control_type="Text")
#         phone_verify_static.wait('ready', timeout=20, retry_interval=0.5)
#         phone_verify_static.click_input()
#         print("已点击手机验证")
#     except Exception as e:
#         print(f"未找到登录按钮: {e}")
#         
#     # 点击发送验证码
#     try:
#         send_code_static = dlg.child_window(title="发送验证码", control_type="Text")
#         send_code_static.wait('ready', timeout=10, retry_interval=0.5)
#         send_code_static.click_input()
#         print("已点击发送验证码")
#     except Exception as e:
#         print(f"未找到发送验证码: {e}")
#         exit()  # 发生错误时退出程序
# 
#     dlg.print_control_identifiers()
#     # 获取 Gotify 消息
#     verification_code = get_gotify_message(gotify_url, client_token, timeout=60)
#     # 确保获取到验证码后再执行后续操作
#     if verification_code:
#         print(f"获取到的验证码: {verification_code}")
# 
#         # 填入验证码输入框
#         try:
#             code_input = dlg.child_window(title="请输入验证码", control_type="Edit")
#             code_input.wait('visible', timeout=10, retry_interval=0.5)
# 
#             # 确保验证码输入框可见并点击它以获取焦点
#             code_input.click_input()
#             print("已点击验证码输入框")
# 
#             # 使用 keyboard 模块逐个输入验证码数字
#             for digit in verification_code:
#                 keyboard.send_keys(digit)
#                 time.sleep(0.1)  # 每个数字之间稍作延迟，确保输入被识别
#             print("已输入验证码")
#         except Exception as e:
#             print(f"输入验证码出错: {e}")
#             exit()
# 
#         # 点击二次认证
#         try:
#             confirm_button = dlg.child_window(title="二次认证", control_type="Button")
#             confirm_button.wait('ready', timeout=10, retry_interval=0.5)
#             confirm_button.click_input()
#             print("已点击二次认证")
#         except Exception as e:
#             print(f"点击二次认证出错: {e}")
#             exit()
# 
#     else:
#         print("未获取到验证码")
