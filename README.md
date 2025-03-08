# 阿里云大语言模型API调用工具

这个项目提供了阿里云大语言模型（通过DashScope兼容模式）的API调用封装，同时包含一个投诉热点分析应用示例。

## 项目结构

- `aliyun_llm.py`: 阿里云大语言模型API的Python封装类
- `test_aliyun_llm.py`: API调用测试脚本
- `投诉热点明细AI分析.py`: 使用大语言模型分析客户投诉数据的应用程序

## 环境要求

- Python 3.6+
- 依赖库: openai, pandas, os, time, datetime

安装依赖:
```bash
pip install openai pandas
```

## 快速开始

### 1. 使用AliyunLLM类

```python
from aliyun_llm import AliyunLLM

# 创建客户端实例
llm = AliyunLLM(api_key="sk-c8464e16fdc844fd8ca1399062d3c1d7")

# 简单流式输出调用
reasoning, answer = llm.chat("你好，请介绍一下自己")

# 不显示思考过程的流式输出
reasoning, answer = llm.chat("什么是人工智能?", include_reasoning=False)

# 不输出流式内容但直接获取结果
response = llm.chat_without_streaming_display("简要介绍量子计算")
print(response)
```

> **注意**: 阿里云大语言模型只支持流式（stream）输出模式。AliyunLLM类中的`chat_without_streaming_display`方法也是使用了流式请求，但不显示实时输出。

### 2. 运行测试脚本

```bash
python test_aliyun_llm.py
```

### 3. 使用投诉热点明细分析工具

```bash
python 投诉热点明细AI分析.py
```

按照提示输入Excel/CSV格式的投诉数据文件路径，选择包含投诉内容的列，程序将自动分析投诉内容并生成分析报告。

## 主要功能

### AliyunLLM类

- 支持流式响应并实时显示
- 可以显示阿里云大语言模型的思考过程(reasoning_content)
- 提供选项隐藏思考过程，仅显示或返回答案
- 提供简洁的API接口，易于集成到其他应用

### 投诉热点明细AI分析应用

- 支持Excel和CSV格式的投诉数据
- 批量分析投诉内容，提取关键信息
- 自动保存中间结果，防止分析中断导致数据丢失
- 生成总结报告，帮助识别投诉热点和问题趋势

## 阿里云大模型特点

- 阿里云大模型通过DashScope兼容模式提供OpenAI兼容的API
- `qwq-32b`模型支持思考过程输出，可通过`reasoning_content`字段获取
- 只支持流式输出模式(stream=True)，不支持非流式模式
- 支持system prompt设置模型人格

## 注意事项

1. 请确保您的API Key有效且有足够的使用额度
2. 批量处理大量数据时，程序会控制请求速率以避免触发API限制
3. 分析结果会保存在"分析结果"文件夹中
4. 如果出现错误`This model only support stream mode`，请确保使用流式模式调用API

## 示例用法

### 带系统提示的对话

```python
from aliyun_llm import AliyunLLM

llm = AliyunLLM()
reasoning, answer = llm.chat(
    "写一首关于秋天的诗",
    system_prompt="你是一位优秀的诗人，擅长创作优美的诗歌"
)
```

### 在现有项目中集成

```python
from aliyun_llm import AliyunLLM

class MyApplication:
    def __init__(self):
        self.llm = AliyunLLM()
    
    def process_user_input(self, user_input):
        # 使用LLM处理用户输入
        response = self.llm.chat_without_streaming_display(user_input)
        return response
```

## 贡献

欢迎提交问题和改进建议! 