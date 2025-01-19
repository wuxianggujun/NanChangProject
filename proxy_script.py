from mitmproxy import http

def request(flow: http.HTTPFlow) -> None:
    if flow.request.pretty_url == "http://10.186.254.225:10010/mcsnr/static/js/7547.bb67afff.chunk.js":
        with open(r"C:\Users\wuxianggujun\CodeSpace\PycharmProjects\NanChangProject\客户服务支撑系统\7547.bb67afff.chunk.js", "rb") as f:  # 替换为你的本地 JS 文件路径
            content = f.read()
        flow.response = http.Response.make(
            200,  # HTTP status code
            content,
            {"Content-Type": "application/javascript"}
        )
