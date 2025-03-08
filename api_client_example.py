# coding=utf-8
import requests
import json
import time

API_BASE_URL = "http://localhost:5000"  # API服务的基础URL

def call_chat_api(prompt, system_prompt=None, with_reasoning=False):
    """
    调用聊天API
    
    Args:
        prompt: 用户提问
        system_prompt: 系统提示，可选
        with_reasoning: 是否返回思考过程
        
    Returns:
        API响应内容
    """
    # 构建请求数据
    data = {
        "prompt": prompt
    }
    
    if system_prompt:
        data["system_prompt"] = system_prompt
        
    # 选择API端点
    endpoint = "/api/chat_with_reasoning" if with_reasoning else "/api/chat"
    
    # 发送请求
    response = requests.post(
        f"{API_BASE_URL}{endpoint}",
        json=data,
        headers={"Content-Type": "application/json"}
    )
    
    # 检查响应状态码
    if response.status_code != 200:
        print(f"API请求失败，状态码: {response.status_code}")
        print(f"错误信息: {response.text}")
        return None
        
    # 解析JSON响应
    try:
        result = response.json()
        return result
    except Exception as e:
        print(f"解析响应失败: {e}")
        print(f"原始响应: {response.text}")
        return None

def check_health():
    """检查API服务健康状态"""
    try:
        response = requests.get(f"{API_BASE_URL}/api/health")
        if response.status_code == 200:
            result = response.json()
            print(f"API服务状态: {result.get('status')}")
            print(f"消息: {result.get('message')}")
            print(f"版本: {result.get('version')}")
            return True
        else:
            print(f"健康检查失败，状态码: {response.status_code}")
            return False
    except Exception as e:
        print(f"连接API服务失败: {e}")
        return False

def pretty_print_json(data):
    """美化打印JSON数据"""
    print(json.dumps(data, ensure_ascii=False, indent=2))

def main():
    """主函数"""
    print("=" * 50)
    print("阿里云大模型API客户端示例")
    print("=" * 50)
    
    # 检查API服务健康状态
    print("\n检查API服务状态...")
    if not check_health():
        print("API服务不可用，请确保服务已启动")
        return
        
    print("\n" + "=" * 50)
    
    # 示例1: 基本对话
    print("\n示例1: 基本对话")
    print("-" * 50)
    
    prompt = "你好，请简单介绍一下自己"
    print(f"用户提问: {prompt}")
    
    result = call_chat_api(prompt)
    if result and result.get("status") == "success":
        print("\n模型回答:")
        print(result["data"]["answer"])
    else:
        print("请求失败")
    
    # 暂停一下，避免请求太快
    time.sleep(1)
    
    # 示例2: 带系统提示的对话
    print("\n\n示例2: 带系统提示的对话")
    print("-" * 50)
    
    prompt = "写一首关于春天的短诗"
    system_prompt = "你是一位著名诗人，善于用优美的语言创作简短而有深度的诗歌。"
    
    print(f"系统提示: {system_prompt}")
    print(f"用户提问: {prompt}")
    
    result = call_chat_api(prompt, system_prompt)
    if result and result.get("status") == "success":
        print("\n模型回答:")
        print(result["data"]["answer"])
    else:
        print("请求失败")
    
    # 暂停一下，避免请求太快
    time.sleep(1)
    
    # 示例3: 带思考过程的对话
    print("\n\n示例3: 带思考过程的对话")
    print("-" * 50)
    
    prompt = "9.9和9.11谁大？请解释理由。"
    print(f"用户提问: {prompt}")
    
    result = call_chat_api(prompt, with_reasoning=True)
    if result and result.get("status") == "success":
        print("\n思考过程:")
        print(result["data"]["reasoning"])
        
        print("\n最终回答:")
        print(result["data"]["answer"])
    else:
        print("请求失败")

if __name__ == "__main__":
    main() 