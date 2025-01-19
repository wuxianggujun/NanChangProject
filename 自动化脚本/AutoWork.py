import base64
import datetime
import json
import os
import subprocess
from tkinter import Image

import psutil
import pyperclip
from aip import AipOcr
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

APP_ID = '117118052'
API_KEY = 'aQg3FvvkMECsBXd0QOqxkix9'
SECRET_KEY = '9MF110N1etGSiwwTFgWv88ZLSkuJblO9'

client = AipOcr(APP_ID, API_KEY, SECRET_KEY)


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
                            match = re.search(r"验证码(\d{6})", line)
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
                            if 0 <= time_diff <= 120:
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


def select_option_from_custom_dropdown(driver, dropdown_input_id, target_option_text, timeout=30, retries=3):
    """
      在自定义下拉框中选择指定的选项。

      Args:
          driver: WebDriver 对象。
          dropdown_input_id: 下拉框输入框的 ID (例如 "selectTree3_input")。
          target_option_text: 要选择的选项的文本内容 (例如 "江西分公司")。
          timeout: 等待超时时间 (秒)。
          retries: 重试次数。
      """
    attempts = 0
    while attempts < retries:
        attempts += 1
        try:
            # 等待下拉框输入框加载完成
            dropdown_input = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.ID, dropdown_input_id)))

            # 点击下拉框输入框以打开下拉框
            dropdown_input.click()
            print("已点击下拉输入框")

            # 使用 JavaScript 来获取下拉框的容器
            js_script = f"return document.getElementById('{dropdown_input_id}').parentNode.nextElementSibling;"
            dropdown_container = driver.execute_script(js_script)

            # 等待下拉框展开 (根据实际情况调整等待条件)
            WebDriverWait(driver, timeout).until(
                EC.visibility_of(dropdown_container)
            )
            print("下拉框已展开")

            # 构建目标选项的 XPath
            option_xpath = f"//ul/li/a/span[contains(text(), '{target_option_text}')]"

            # 等待目标选项出现并可点击
            target_option = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, option_xpath))
            )

            # 点击目标选项
            target_option.click()
            print(f"已选择: {target_option_text}")
            return  # 成功选择后直接返回

        except TimeoutException:
            print(f"在下拉框中选择 '{target_option_text}' 超时 (第 {attempts} 次尝试)")
            if attempts < retries:
                print("尝试重启 Edge 并重试...")
                driver.quit()  # 关闭当前的 driver

                # 重启 Edge 并获取新的 driver 对象
                if start_edge_with_remote_debugging(port=9222):
                    print("Edge 重启成功！")
                    options = Options()
                    options.add_experimental_option("debuggerAddress", "localhost:9222")
                    driver = webdriver.Edge(options=options)  # 更新 driver 对象
                    driver.get(
                        "http://10.188.34.1/cs/login.html?type=kickout&loginFlag=loginFlag&ip=")  # 重新加载页面
                    print("已重新加载页面")
                else:
                    print("Edge 重启失败！")
                    break  # 如果重启失败，退出循环

        except Exception as e:
            print(f"在下拉框中选择 '{target_option_text}' 出错: {e}")
            break  # 其他错误直接退出循环

    print(f"在 {retries} 次尝试后仍然无法选择 '{target_option_text}'")


def get_verification_code_by_baidu_ocr(image_data):
    # 在此处添加调用百度OCR API的代码，并返回验证码结果
    try:
        # 调用百度 OCR API 进行识别
        result = client.basicGeneral(image_data)  # 使用通用文字识别

        # 提取识别结果
        if result and 'words_result' in result:
            words = [word_info['words'] for word_info in result['words_result']]
            verification_code = ''.join(words)
            print(f"百度 OCR 识别结果: {verification_code}")
            return verification_code
        else:
            print(f"百度 OCR 识别失败: {result}")
            return None

    except Exception as e:
        print(f"百度 OCR 识别出错: {e}")
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
        target_url = "http://10.188.34.1/cs/login.html?type=kickout&loginFlag=loginFlag&ip="
        if driver.current_url != target_url:
            driver.get(target_url)
            print(f"已打开页面：{target_url}")

        wait = WebDriverWait(driver, 10)
        element = wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        select_option_from_custom_dropdown(driver, "selectTree3_input", "江西分公司")

        # 获取账号和密码输入框
        username_input = driver.find_element(By.NAME, "username")
        password_input = driver.find_element(By.NAME, "password")

        # 清空账号和密码输入框
        username_input.clear()
        password_input.clear()

        # 填写账号和密码
        username_input.send_keys("17507042051")  # 替换为你的实际账号
        password_input.send_keys("KF79189090dsp")  # 替换为你的实际密码

        # 获取验证码图片并识别
        verification_img = driver.find_element(By.XPATH, "//span[@class='verification']/img")
        img_src = verification_img.get_attribute("src")
        print(f"验证码图片地址: {img_src}")

        # JavaScript 代码，用于注入和拦截 blob
        blob_hook_script = """
               (function() {
                   console.log("注入脚本开始执行");
                   const originalCreateObjectURL = URL.createObjectURL;

                   URL.createObjectURL = function(blob) {
                       console.log("createObjectURL 被调用");
                       const blobUrl = originalCreateObjectURL.call(URL, blob);
                       const xhr = new XMLHttpRequest();
                       xhr.open('GET', blobUrl, true);
                       xhr.responseType = 'blob';

                       xhr.onload = function() {
                           console.log("XHR onload 触发, status:", this.status);
                           if (this.status === 200) {
                               const reader = new FileReader();
                               reader.readAsDataURL(this.response);
                               reader.onloadend = function() {
                                   const base64data = reader.result;
                                   console.log('Blob URL:', blobUrl, 'Base64 Data:', base64data.substring(0, 100) + "..."); // 打印部分 base64 数据
                                   // 直接将 base64 数据赋值给 window.b64Data
                                   window.b64Data = base64data;
                               };
                           } else {
                               console.error("XHR 请求失败, status:", this.status);
                           }
                       };
                       xhr.onerror = function() {
                           console.error("XHR 请求错误");
                       };
                       xhr.send();
                       return blobUrl;
                   };
               })();
               """

        # 等待页面加载完成后注入 JavaScript 代码
        time.sleep(2)  # 确保页面已加载完成
        driver.execute_script(blob_hook_script)
        print("已注入 JavaScript 代码")

        # 刷新验证码图片以触发 blob URL 的创建
        refresh_button = driver.find_element(By.XPATH, "//div[@id='verficationbox']/div/a[@class='changnexbtn']")
        refresh_button.click()
        print("已点击刷新验证码")

        # 等待 JavaScript 代码执行并将 base64 数据存储到 window.b64Data
        try:
            WebDriverWait(driver, 10).until(
                lambda driver: driver.execute_script("return window.b64Data !== undefined;")
            )
            print("已成功获取到 window.b64Data")
        except TimeoutException:
            print("获取 window.b64Data 超时")

        # 从 window.b64Data 获取 base64 数据
        img_base64 = driver.execute_script("return window.b64Data;")

        # 检查浏览器控制台错误
        for entry in driver.get_log('browser'):
            print(f"注入JavaScript后浏览器控制台日志: {entry}")

        # 检查是否成功获取到 base64 数据
        if img_base64:
            print(f"获取到base64编码的图片: {img_base64[:100]}...")  # 打印部分 base64 数据
            # 去除 Base64 编码前缀 (data:image/png;base64,)
            img_base64 = img_base64.split(",")[1]
            # 解码 Base64 数据
            img_data = base64.b64decode(img_base64)
            # 将图片数据保存到本地
            image_path = "verification_code.png"  # 保存路径
            with open(image_path, "wb") as f:
                f.write(img_data)
            print(f"验证码图片已保存至: {image_path}")

            # 使用百度 OCR API 识别验证码
            code = get_verification_code_by_baidu_ocr(img_data)

            if code:
                verification_code_input = driver.find_element(By.NAME, "verificationCode")
                verification_code_input.send_keys(code)

                # 提交表单
                login_form = driver.find_element(By.ID, "tenantLoginForm")
                login_form.submit()
            else:
                print("验证码识别失败，请手动处理或重试。")
        else:
            print("获取验证码图片数据失败！")

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
