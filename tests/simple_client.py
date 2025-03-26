#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
简单的MCP客户端
"""

import json
import requests

class SimpleMCPClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.sse_url = f"{base_url}/sse"
        self.session_id = None
    
    def request(self, method, params=None):
        """发送请求到MCP服务器"""
        if params is None:
            params = {}
        
        headers = {"Content-Type": "application/json"}
        
        # 构建请求体
        request_body = {
            "method": method
        }
        if params:
            request_body["params"] = params
        
        # 针对本地MCP服务器的直接HTTP请求
        try:
            # 尝试通用的MCP消息格式
            request_url = f"{self.base_url}/message"
            response = requests.post(request_url, headers=headers, json=request_body)
            
            if response.status_code == 200:
                return response.json()
            
            # 如果失败，尝试直接发送到SSE端点
            request_url = self.sse_url
            response = requests.post(request_url, headers=headers, json=request_body)
            
            if response.status_code == 200:
                return response.json()
            
            # 所有尝试都失败
            return {
                "error": f"请求失败，状态码: {response.status_code}",
                "message": response.text
            }
        except Exception as e:
            return {
                "error": f"请求异常: {str(e)}"
            }

# 使用简单客户端测试
client = SimpleMCPClient("http://localhost:3000")

# 测试ping方法
print("测试ping...")
result = client.request("ping")
print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")

# 测试提示模板列表
print("\n测试prompts/list...")
result = client.request("prompts/list")
print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")

# 测试资源模板列表
print("\n测试resources/templates/list...")
result = client.request("resources/templates/list")
print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")

print("\n测试完成!") 