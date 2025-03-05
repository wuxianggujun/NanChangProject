import requests
import json

url = "http://127.0.0.1:11434/api/generate"

payload = {
    "model": "deepseek-r1:8b",  #  请替换成你下载的模型名称
    "prompt": "Translate the following English text to Chinese: Hello, world!",
    "stream": False,
    "options": {
        "temperature": 0.7,
        "top_p": 0.9,
        "num_predict": 128
    }
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    result = response.json()
    print(result["response"])
else:
    print(f"Error: {response.status_code} - {response.text}")