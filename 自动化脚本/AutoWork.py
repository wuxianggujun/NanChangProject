import datetime

import pyperclip
from pywinauto.application import Application
from pywinauto import Desktop, keyboard
import time
import requests
import re
import dateutil.parser
from pywinauto.keyboard import send_keys


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

if __name__ == '__main__':

    # 获取 Gotify 消息
    gotify_url = "http://wuxianggujun.com:40266"  # 替换为你的 Gotify 服务器地址
    # app_token  = "A7LY0EtgIdoNFTt"  # 替换为你的 Gotify 应用程序 token
    client_token = "Cm0Z974k0.9ScEB"  # 替换为你的 Gotify 客户端的 token (需要创建客户端并获取 token)

    # 启动应用程序
    app = Application(backend='uia').start(r"D:\Program Files\EnUES\EnUES.exe")

    # 连接到应用程序窗口
    dlg = Desktop(backend="uia").window(title="江西零信任")  # 可以选择添加found_index=0来指定第一个找到的窗口
    dlg.wait('ready', timeout=20, retry_interval=0.5)

    try:
        # 使用 title="登 录" (注意中间的空格)
        login_button = dlg.child_window(title="登 录", control_type="Button")
        login_button.wait('ready', timeout=10, retry_interval=0.5)  # 等待按钮可点击
        login_button.click_input()
        print("已点击登录按钮")  # 加一个打印方便你判断是否执行

        phone_verify_static = dlg.child_window(title="手机验证", control_type="Text")
        phone_verify_static.wait('ready', timeout=20, retry_interval=0.5)
        phone_verify_static.click_input()
        print("已点击手机验证")
    except Exception as e:
        print(f"未找到登录按钮: {e}")
        
    # 点击发送验证码
    try:
        send_code_static = dlg.child_window(title="发送验证码", control_type="Text")
        send_code_static.wait('ready', timeout=10, retry_interval=0.5)
        send_code_static.click_input()
        print("已点击发送验证码")
    except Exception as e:
        print(f"未找到发送验证码: {e}")
        exit()  # 发生错误时退出程序

    dlg.print_control_identifiers()
    # 获取 Gotify 消息
    verification_code = get_gotify_message(gotify_url, client_token, timeout=60)
    # 确保获取到验证码后再执行后续操作
    if verification_code:
        print(f"获取到的验证码: {verification_code}")

        # 填入验证码输入框
        try:
            code_input = dlg.child_window(title="请输入验证码", control_type="Edit")
            code_input.wait('visible', timeout=10, retry_interval=0.5)

            # 确保验证码输入框可见并点击它以获取焦点
            code_input.click_input()
            print("已点击验证码输入框")

            # 使用 keyboard 模块逐个输入验证码数字
            for digit in verification_code:
                keyboard.send_keys(digit)
                time.sleep(0.1)  # 每个数字之间稍作延迟，确保输入被识别
            print("已输入验证码")
        except Exception as e:
            print(f"输入验证码出错: {e}")
            exit()

        # 点击二次认证
        try:
            confirm_button = dlg.child_window(title="二次认证", control_type="Button")
            confirm_button.wait('ready', timeout=10, retry_interval=0.5)
            confirm_button.click_input()
            print("已点击二次认证")
        except Exception as e:
            print(f"点击二次认证出错: {e}")
            exit()

    else:
        print("未获取到验证码")
