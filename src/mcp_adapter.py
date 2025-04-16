#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MCP协议适配器

基于JSON-RPC 2.0标准实现MCP协议
支持流式响应，提升用户体验
"""

import json
import logging
import os
import inspect
import traceback
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, StreamingResponse
from typing import Dict, Any, List, Optional, Callable, AsyncGenerator

# 获取日志记录器
logger = logging.getLogger("mcp.adapter")

# 流式响应格式类型
class ResponseType:
    THINKING = "thinking"    # 思考过程
    PROGRESS = "progress"    # 进度更新
    PARTIAL = "partial"      # 部分结果
    FINAL = "final"          # 最终结果
    ERROR = "error"          # 错误信息

class MCPAdapterMiddleware(BaseHTTPMiddleware):
    """
    MCP协议适配中间件
    基于标准JSON-RPC 2.0格式实现
    支持流式响应
    """
    
    def __init__(self, app=None):
        """初始化中间件
        
        Args:
            app: FastAPI应用实例，可选
        """
        if app is not None:
            super().__init__(app)
        # 不需要在这里调用父类初始化，因为可能没有提供app
    
    async def dispatch(self, request: Request, call_next):
        # 处理OPTIONS请求（预检请求）
        if request.method == "OPTIONS":
            return JSONResponse(
                {},
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*",
                    "Access-Control-Max-Age": "3600"
                }
            )
        
        # 只处理发送到/messages端点的POST请求
        if request.method == "POST" and "/messages" in request.url.path:
            logger.debug(f"检测到发往 {request.url.path} 的POST请求")
            
            # 检查是否要求流式响应
            use_stream = "stream" in request.query_params and request.query_params["stream"].lower() in ("true", "1", "yes")
            
            try:
                # 读取请求体
                body = await request.body()
                if not body:
                    logger.warning("请求体为空，跳过处理")
                    return await call_next(request)
                
                # 尝试解析JSON
                try:
                    data = json.loads(body)
                    logger.debug(f"接收到请求: {json.dumps(data)}")
                    
                    # 如果开启了调试模式，打印完整请求
                    if os.environ.get("MCP_DEBUG_ADAPTER", "").lower() == "true":
                        logger.debug(f"完整请求内容: {json.dumps(data, ensure_ascii=False)}")
                    
                except json.JSONDecodeError:
                    logger.warning("无法解析JSON请求，跳过处理")
                    return await call_next(request)
                
                # 处理JSON-RPC 2.0格式的请求
                if "jsonrpc" in data and "method" in data:
                    logger.info(f"处理JSON-RPC 2.0格式请求: method={data.get('method')}")
                    
                    # 如果是工具调用请求，记录一些调试信息
                    if data.get("method") == "tools/call" and "params" in data:
                        tool_params = data.get("params", {})
                        tool_name = tool_params.get("name", "")
                        tool_args = tool_params.get("arguments", {})
                        logger.debug(f"JSON-RPC工具调用: {tool_name}, 参数: {json.dumps(tool_args)}")
                        
                        # 如果是random_string参数但arguments为空，添加一个random_string参数到arguments
                        if isinstance(tool_args, dict) and not tool_args and "random_string" in tool_params:
                            random_string = tool_params.get("random_string")
                            if random_string:
                                # 修改请求体添加random_string作为参数
                                data["params"]["arguments"] = {"random_string": random_string}
                                request._body = json.dumps(data).encode('utf-8')
                                logger.debug(f"添加random_string参数到arguments: {random_string}")
                    
                    # 将请求传递给下一个处理程序
                    return await call_next(request)
                
                # 如果不是已知格式的请求，继续处理
                return await call_next(request)
                
            except Exception as e:
                logger.error(f"处理请求时出错: {str(e)}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                return JSONResponse({
                    "jsonrpc": "2.0",
                    "id": data.get("id", "unknown"),
                    "error": {
                        "code": -32000,
                        "message": str(e)
                    }
                })
        
        # 对于其他请求，继续处理
        return await call_next(request)
    
    
    def format_stream_event(self, event_type: str, data: Dict[str, Any]) -> str:
        """格式化SSE事件"""
        # 检查data是否为None
        if data is None:
            data = {}
        
        # 将数据直接放入data字段
        event_data = {
            "type": event_type,
            "data": data
        }
        
        # 尝试直接序列化整个事件对象
        try:
            # 转换为JSON
            json_data = json.dumps(event_data, ensure_ascii=False)
            
            # 构建SSE事件
            sse_event = f"data: {json_data}\n\n"
            
            # 详细日志记录发送的SSE事件
            logger.info(f"发送SSE事件: 类型={event_type}, 数据长度={len(json_data)}")
            logger.debug(f"原始SSE事件: {sse_event[:100]}...")
            
            return sse_event
        except Exception as e:
            # 如果序列化失败，返回错误事件
            logger.error(f"序列化事件失败: {str(e)}, 事件类型: {event_type}")
            error_data = {
                "type": "error",
                "message": f"序列化事件失败: {str(e)}"
            }
            return f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"

def init():
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # 获取日志记录器
    logger = logging.getLogger("mcp.adapter")
    
    # 设置日志级别
    if os.environ.get("MCP_DEBUG_ADAPTER", "").lower() == "true":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    logger.info("MCP适配器初始化完成")

# 初始化
init()

# 仅用于测试的示例响应
if __name__ == "__main__":
    # 测试JSON-RPC 2.0格式
    jsonrpc_format = {
        "jsonrpc": "2.0",
        "id": "123",
        "method": "tools/call",
        "params": {
            "name": "nl2sql_query",
            "arguments": {"query": "历史销量最高的是哪一年？"}
        }
    }
    
    print("JSON-RPC 2.0格式:", json.dumps(jsonrpc_format, indent=2))

    # 测试list_tools工具调用
    message = {
        "jsonrpc": "2.0",
        "id": "123",
        "method": "mcp/listTools"
    }
    response = JSONResponse(
        {
            "jsonrpc": "2.0",
            "id": message["id"],
            "result": {
                "tools": [
                    {
                        "name": "nl2sql_query",
                        "description": "Query to convert natural language to SQL",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The natural language query"
                                }
                            },
                            "required": ["query"]
                        }
                    }
                ]
            }
        },
        headers={
            "Access-Control-Allow-Origin": "http://localhost:3100",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Expose-Headers": "*",
            "Access-Control-Max-Age": "3600"
        }
    )
    print("list_tools响应:", json.dumps(response.body, indent=2)) 

def register_routes(app):
    """注册路由和中间件"""
    logger.info("开始注册自定义路由")
    
    @app.post("/nl2sql/stream")
    async def nl2sql_stream(request: Request):
        """NL2SQL流式处理路由"""
        logger.info("接收到NL2SQL流式处理请求")
        adapter = MCPAdapterMiddleware(None)  # 传入None，因为我们不会使用它的中间件功能
        return await adapter.process_nl2sql_query_stream(request)
    
    @app.options("/nl2sql/stream")
    async def nl2sql_stream_options():
        """处理NL2SQL流式接口的OPTIONS请求"""
        logger.info("接收到NL2SQL流式接口OPTIONS请求")
        return Response(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Max-Age": "86400",
            }
        )
    
    logger.info("注册了NL2SQL流式处理路由: /nl2sql/stream")
    
    # 添加中间件
    app.add_middleware(MCPAdapterMiddleware)
    
    return app 