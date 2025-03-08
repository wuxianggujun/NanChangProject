# coding=utf-8

from openai import OpenAI
import http.client
import json
import time

# 方法一：使用OpenAI SDK
def test_with_openai_sdk():
    # 华为云Deepseek API配置
    base_url = "https://maas-cn-southwest-2.modelarts-maas.com/deepseek-v3/v1"  # API地址
    api_key = "kloIQolGnFCB5lH53fOnHkSgF8ixHCAgjCcGzaRPgquOH94c0OJTU5RTIvtbEVGBMw5rxs1MwAiiDs4yxOMFWg"  # API Key

    try:
        # 创建OpenAI客户端，连接到华为云Deepseek
        client = OpenAI(api_key=api_key, base_url=base_url)

        # 发送请求测试API连接
        response = client.chat.completions.create(
            model="DeepSeek-V3",  # 模型名称
            messages=[
                {"role": "system", "content": "你是一个专业的AI助手"},
                {"role": "user", "content": "你好，请简单介绍一下你自己"},
            ],
            max_tokens=1024,
            temperature=0.7,
            stream=False
        )

        # 打印模型返回的结果
        print("使用OpenAI SDK的响应:")
        print(response.choices[0].message.content)
    except Exception as e:
        print(f"OpenAI SDK方式出错: {e}")

# 方法二：使用直接HTTP请求
def test_with_direct_request():
    try:
        # API配置
        api_host = "maas-cn-southwest-2.modelarts-maas.com"
        api_path = "/deepseek-v3/v1/chat/completions"
        api_key = "kloIQolGnFCB5lH53fOnHkSgF8ixHCAgjCcGzaRPgquOH94c0OJTU5RTIvtbEVGBMw5rxs1MwAiiDs4yxOMFWg"

        # 请求参数
        payload = {
            "model": "DeepSeek-V3",
            "messages": [
                {"role": "system", "content": "你是一个专业的AI助手"},
                {"role": "user", "content": "你好，请简单介绍一下你自己"}
            ],
            "max_tokens": 1024,
            "temperature": 0.7
        }

        # 设置HTTP连接
        conn = http.client.HTTPSConnection(api_host)
        
        # 设置请求头
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        # 发送请求
        conn.request("POST", api_path, json.dumps(payload), headers)
        
        # 获取响应
        response = conn.getresponse()
        response_data = response.read().decode("utf-8")
        
        # 打印响应
        print(f"\n使用直接HTTP请求的响应 (状态码: {response.status}):")
        if response.status == 200:
            response_json = json.loads(response_data)
            if "choices" in response_json and len(response_json["choices"]) > 0:
                print(response_json["choices"][0]["message"]["content"])
            else:
                print(f"完整响应: {response_data}")
        else:
            print(f"错误响应: {response_data}")
            
        conn.close()
    except Exception as e:
        print(f"HTTP请求方式出错: {e}")

# 运行两种测试方法
print("测试华为云Deepseek API连接...\n")
test_with_openai_sdk()
time.sleep(1)  # 两次请求之间稍作暂停
test_with_direct_request() 