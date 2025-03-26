#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MCP客户端测试脚本
"""

import json
from unity_mcp_client import UnityMCP

# 创建MCP客户端实例
client = UnityMCP("http://localhost:3000")

# 连接到服务器
client.connect()

# 测试ping方法
try:
    print("测试ping...")
    result = client.request("ping")
    print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
except Exception as e:
    print(f"Ping失败: {e}")

# 测试提示模板列表
try:
    print("\n测试prompts/list...")
    result = client.request("prompts/list")
    print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
except Exception as e:
    print(f"获取提示模板列表失败: {e}")

# 测试资源模板列表
try:
    print("\n测试resources/templates/list...")
    result = client.request("resources/templates/list")
    print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
except Exception as e:
    print(f"获取资源模板列表失败: {e}")

# 断开连接
client.disconnect()
print("\n测试完成!") 