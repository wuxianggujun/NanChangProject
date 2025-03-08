# coding=utf-8
from openai import OpenAI
import os

class AliyunLLM:
    """
    阿里云大语言模型API封装类
    """
    
    def __init__(self, api_key=None):
        """
        初始化阿里云大语言模型客户端
        
        Args:
            api_key: 阿里云API密钥，如果不提供则尝试从环境变量获取
        """
        # 优先使用传入的API密钥，否则尝试从环境变量获取
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY", "sk-c8464e16fdc844fd8ca1399062d3c1d7")
        self.base_url = "https://dashscope.aliyuncs.com/compatible-mode/v1"
        
        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
    
    def chat(self, prompt, model="qwq-32b", system_prompt=None, include_reasoning=True, return_raw_text=False):
        """
        发送聊天请求到阿里云大语言模型
        
        Args:
            prompt: 用户输入的问题或提示
            model: 模型名称，默认为qwq-32b
            system_prompt: 系统提示信息，可选
            include_reasoning: 是否显示思考过程，默认为True
            return_raw_text: 是否仅返回纯文本结果，默认为False
            
        Returns:
            如果return_raw_text=True，返回完整回复内容的字符串
            如果return_raw_text=False，返回(思考过程,回复内容)的元组
        """
        # 构建消息列表
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        # 阿里云模型只支持流式请求
        completion = self.client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True  # 阿里云要求必须使用流式模式
        )
        
        reasoning_content = ""  # 定义完整思考过程
        answer_content = ""     # 定义完整回复
        is_answering = False    # 判断是否结束思考过程并开始回复
        
        if include_reasoning and not return_raw_text:
            print("\n" + "=" * 20 + "思考过程" + "=" * 20 + "\n")
        
        for chunk in completion:
            # 如果chunk.choices为空，则是usage信息
            if not chunk.choices:
                if hasattr(chunk, 'usage') and include_reasoning and not return_raw_text:
                    print("\nToken使用情况:")
                    print(chunk.usage)
            else:
                delta = chunk.choices[0].delta
                # 处理思考过程
                if hasattr(delta, 'reasoning_content') and delta.reasoning_content is not None:
                    if include_reasoning and not return_raw_text:
                        print(delta.reasoning_content, end='', flush=True)
                    reasoning_content += delta.reasoning_content
                else:
                    # 开始回复部分
                    if delta.content != "" and is_answering is False:
                        if include_reasoning and not return_raw_text:
                            print("\n" + "=" * 20 + "完整回复" + "=" * 20 + "\n")
                        is_answering = True
                    # 输出回复内容
                    if include_reasoning and not return_raw_text:
                        print(delta.content, end='', flush=True)
                    answer_content += delta.content
        
        # 结束后打印一个换行
        if include_reasoning and not return_raw_text:
            print()
            
        if return_raw_text:
            return answer_content
        else:
            return (reasoning_content, answer_content)
    
    def chat_without_streaming_display(self, prompt, model="qwq-32b", system_prompt=None):
        """
        发送请求到阿里云大语言模型，但不在控制台实时显示，直接返回结果
        
        Args:
            prompt: 用户输入的问题或提示
            model: 模型名称，默认为qwq-32b
            system_prompt: 系统提示信息，可选
            
        Returns:
            返回完整回复内容字符串
        """
        return self.chat(prompt, model, system_prompt=system_prompt, include_reasoning=False, return_raw_text=True)

# 使用示例
if __name__ == "__main__":
    # 创建客户端实例
    llm = AliyunLLM()
    
    # 调用示例1：流式输出（默认方式）
    print("示例1: 流式输出")
    reasoning, answer = llm.chat("9.9和9.11谁大")
    
    # 调用示例2：流式请求但不显示思考过程
    print("\n\n示例2: 不显示思考过程")
    reasoning, answer = llm.chat("列出五个世界上最高的山峰", include_reasoning=False)
    print(f"获取到的回答: {answer[:100]}..." if len(answer) > 100 else answer)
    
    # 调用示例3：不显示流式输出，直接获取结果
    print("\n\n示例3: 不显示流式输出，直接获取结果")
    response = llm.chat_without_streaming_display("简要介绍量子计算的基本原理")
    print(f"获取到的回答: {response[:100]}..." if len(response) > 100 else response) 