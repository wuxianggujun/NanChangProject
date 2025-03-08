# coding=utf-8
from flask import Flask, request, jsonify
from aliyun_llm import AliyunLLM
import os
import traceback

app = Flask(__name__)

# 创建LLM实例
llm = AliyunLLM()

@app.route('/api/chat', methods=['POST'])
def chat():
    """
    聊天API接口
    
    请求体JSON格式:
    {
        "prompt": "用户提问内容",
        "system_prompt": "可选的系统提示"
    }
    
    响应JSON格式:
    {
        "status": "success" 或 "error",
        "data": {
            "answer": "模型回答内容"
        },
        "error": "如果status为error，返回错误消息"
    }
    """
    try:
        # 获取请求参数
        data = request.get_json()
        
        if not data or 'prompt' not in data:
            return jsonify({
                'status': 'error',
                'error': '请求参数不完整，需要提供prompt字段'
            }), 400
            
        prompt = data['prompt']
        system_prompt = data.get('system_prompt')  # 可选参数
        
        # 调用大模型API
        response = llm.chat_without_streaming_display(
            prompt=prompt,
            system_prompt=system_prompt
        )
        
        # 返回结果
        return jsonify({
            'status': 'success',
            'data': {
                'answer': response
            }
        })
        
    except Exception as e:
        # 捕获并返回错误
        error_message = str(e)
        traceback_info = traceback.format_exc()
        print(f"API调用出错: {error_message}\n{traceback_info}")
        
        return jsonify({
            'status': 'error',
            'error': error_message
        }), 500

@app.route('/api/chat_with_reasoning', methods=['POST'])
def chat_with_reasoning():
    """
    带思考过程的聊天API接口
    
    请求体JSON格式:
    {
        "prompt": "用户提问内容",
        "system_prompt": "可选的系统提示"
    }
    
    响应JSON格式:
    {
        "status": "success" 或 "error",
        "data": {
            "reasoning": "思考过程内容",
            "answer": "模型回答内容"
        },
        "error": "如果status为error，返回错误消息"
    }
    """
    try:
        # 获取请求参数
        data = request.get_json()
        
        if not data or 'prompt' not in data:
            return jsonify({
                'status': 'error',
                'error': '请求参数不完整，需要提供prompt字段'
            }), 400
            
        prompt = data['prompt']
        system_prompt = data.get('system_prompt')  # 可选参数
        
        # 调用大模型API，获取思考过程和回答
        reasoning, answer = llm.chat(
            prompt=prompt,
            system_prompt=system_prompt,
            include_reasoning=False,  # API调用时不打印思考过程
            return_raw_text=False     # 返回元组
        )
        
        # 返回结果
        return jsonify({
            'status': 'success',
            'data': {
                'reasoning': reasoning,
                'answer': answer
            }
        })
        
    except Exception as e:
        # 捕获并返回错误
        error_message = str(e)
        traceback_info = traceback.format_exc()
        print(f"API调用出错: {error_message}\n{traceback_info}")
        
        return jsonify({
            'status': 'error',
            'error': error_message
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    return jsonify({
        'status': 'success',
        'message': 'API服务正常运行',
        'version': '1.0.0'
    })

@app.route('/', methods=['GET'])
def home():
    """首页，返回简单的使用说明"""
    return """
    <html>
    <head>
        <title>阿里云大模型API服务</title>
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }
            h1 { color: #333; }
            pre { background: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; }
            .endpoint { background: #e0f7fa; padding: 10px; border-left: 4px solid #00bcd4; margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <h1>阿里云大模型API服务</h1>
        <p>这是一个封装了阿里云大语言模型的API服务。你可以通过以下端点与服务交互：</p>
        
        <div class="endpoint">
            <h3>1. 基本对话接口</h3>
            <code>POST /api/chat</code>
            <p>发送用户提问并获取模型回答。</p>
            <h4>请求格式:</h4>
            <pre>
{
    "prompt": "你的问题或提示",
    "system_prompt": "可选的系统提示"
}
            </pre>
        </div>
        
        <div class="endpoint">
            <h3>2. 带思考过程的对话接口</h3>
            <code>POST /api/chat_with_reasoning</code>
            <p>发送用户提问并获取模型思考过程和回答。</p>
            <h4>请求格式:</h4>
            <pre>
{
    "prompt": "你的问题或提示",
    "system_prompt": "可选的系统提示"
}
            </pre>
        </div>
        
        <div class="endpoint">
            <h3>3. 健康检查接口</h3>
            <code>GET /api/health</code>
            <p>检查API服务是否正常运行。</p>
        </div>
        
        <h3>使用示例 (使用curl):</h3>
        <pre>
curl -X POST http://localhost:5000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"prompt": "你好，请介绍一下自己", "system_prompt": "你是一位专业的AI助手"}'
        </pre>
    </body>
    </html>
    """

if __name__ == '__main__':
    # 获取端口，默认为5000
    port = int(os.environ.get("PORT", 5000))
    
    # 在开发环境中使用debug模式
    debug_mode = os.environ.get("FLASK_ENV") == "development"
    
    # 启动Flask应用
    print(f"启动API服务，监听端口: {port}, 调试模式: {'开启' if debug_mode else '关闭'}")
    app.run(host='0.0.0.0', port=port, debug=debug_mode) 