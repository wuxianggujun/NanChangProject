# coding=utf-8
from aliyun_llm import AliyunLLM

def main():
    """测试阿里云大模型API调用"""
    print("=" * 50)
    print("阿里云大模型API测试")
    print("=" * 50)
    
    # 创建大模型客户端实例
    # 可以直接使用默认API密钥
    llm = AliyunLLM()
    
    # 或者提供自己的API密钥
    # llm = AliyunLLM(api_key="sk-c8464e16fdc844fd8ca1399062d3c1d7")
    
    # 测试1: 基本流式对话，含思考过程
    print("\n测试1: 基本流式对话（含思考过程）")
    print("-" * 50)
    reasoning, answer = llm.chat("海王星和木星哪个更大？请解释理由。")
    print("\n获取到的思考过程长度:", len(reasoning), "字符")
    print("获取到的回答长度:", len(answer), "字符")
    
    # 测试2: 流式对话但隐藏思考过程
    print("\n\n测试2: 流式对话（隐藏思考过程）")
    print("-" * 50)
    reasoning, answer = llm.chat(
        "列出三种常见的机器学习算法及其应用场景",
        include_reasoning=False
    )
    print("\n获取到的回答:", answer[:50] + "..." if len(answer) > 50 else answer)
    
    # 测试3: 不显示流式输出，直接获取结果
    print("\n\n测试3: 不显示流式输出，直接获取结果")
    print("-" * 50)
    response = llm.chat_without_streaming_display("简要介绍量子计算的基本原理")
    print("获取到的回答:", response[:100] + "..." if len(response) > 100 else response)
    
    # 测试4: 带系统提示的对话
    print("\n\n测试4: 带系统提示的对话")
    print("-" * 50)
    reasoning, answer = llm.chat(
        "写一首关于人工智能的短诗",
        system_prompt="你是一位著名诗人，善于用优美的语言创作简短而有深度的诗歌。"
    )

if __name__ == "__main__":
    main() 