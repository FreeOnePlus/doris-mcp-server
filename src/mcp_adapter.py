#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MCP协议适配器

将旧版MCP协议格式转换为新版JSON-RPC 2.0格式
支持流式响应，提升用户体验
"""

import json
import logging
import os
import inspect
import asyncio
import time
import traceback
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, StreamingResponse
import datetime
import httpx
from urllib.parse import urlencode
from typing import Dict, Any, List, Optional, Callable, AsyncGenerator
import uuid

# 导入工具注册中心
from src.tools.tool_registry import get_registry, get_tool, get_tool_list

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
    将旧版MCP协议格式转换为标准JSON-RPC 2.0格式
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
                        pass
                    
                except json.JSONDecodeError:
                    logger.warning("无法解析JSON请求，跳过处理")
                    return await call_next(request)
                
                # 检查是否是旧格式的MCP请求
                if "type" in data and "id" in data and "session_id" in data:
                    logger.info(f"检测到旧版MCP格式请求: id={data.get('id')}, type={data.get('type')}")
                    
                    # 直接处理特定工具调用
                    tool_name = data.get("tool", "")
                    
                    # 直接处理list_tools工具调用
                    if data.get("type") == "tool" and tool_name == "list_tools":
                        logger.info("处理list_tools特殊工具调用")
                        
                        # 从FastMCP获取工具列表
                        try:
                            # 从应用状态中获取MCP实例
                            mcp = request.app.state.mcp
                            logger.info(f"获取到MCP实例: {mcp}")
                            
                            # 获取工具列表
                            if hasattr(mcp, 'list_tools'):
                                try:
                                    # 注意：list_tools是异步函数，需要await
                                    tools = []
                                    # 检查是否为协程函数
                                    if inspect.iscoroutinefunction(mcp.list_tools):
                                        tools = await mcp.list_tools()
                                    else:
                                        tools = mcp.list_tools()
                                        
                                    logger.info(f"MCP工具列表: {tools}")
                                    
                                    # 构建工具列表
                                    tools_list = []
                                    for tool in tools:
                                        logger.info(f"处理工具: {tool.name} - {tool.description}")
                                        tool_dict = {"name": tool.name, "description": tool.description}
                                        if hasattr(tool, 'parameters'):
                                            tool_dict["inputSchema"] = tool.parameters
                                        tools_list.append(tool_dict)
                                except Exception as e:
                                    logger.error(f"获取MCP工具列表失败: {str(e)}")
                                    tools_list = []
                            else:
                                logger.warning("MCP实例没有list_tools方法，回退到工具注册中心")
                                tools_list = get_tool_list()  # 回退到工具注册中心
                        
                            logger.info(f"获取到工具列表: {json.dumps(tools_list)}")
                        except Exception as e:
                            logger.error(f"获取工具列表失败: {str(e)}")
                            tools_list = get_tool_list()  # 回退到工具注册中心
                        
                        # 如果工具列表仍然为空，添加一些默认工具
                        if not tools_list:
                            logger.warning("工具列表为空，添加默认工具")
                            tools_list = [
                                {
                                    "name": "list_llm_providers",
                                    "description": "列出可用的LLM提供商",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {},
                                        "required": []
                                    }
                                },
                                {
                                    "name": "list_database_tables",
                                    "description": "列出数据库中的所有表",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {},
                                        "required": []
                                    }
                                },
                                {
                                    "name": "nl2sql_query",
                                    "description": "将自然语言查询转换为SQL，并执行查询返回结果",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {
                                            "query": {
                                                "type": "string",
                                                "description": "自然语言查询"
                                            }
                                        },
                                        "required": ["query"]
                                    }
                                },
                                {
                                    "name": "get_nl2sql_status",
                                    "description": "获取当前NL2SQL处理状态",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {},
                                        "required": []
                                    }
                                }
                            ]
                        
                        # 构建旧格式响应
                        response_data = {
                            "id": data.get("id"),
                            "success": True,
                            "result": {"tools": tools_list}
                        }
                        
                        # 返回响应
                        return JSONResponse(
                            response_data,
                            headers={
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Credentials": "true",
                                "Access-Control-Allow-Methods": "*",
                                "Access-Control-Allow-Headers": "*",
                                "Access-Control-Expose-Headers": "*"
                            }
                        )
                    
                    # 处理其他工具调用
                    if data.get("type") == "tool":
                        logger.info(f"处理工具调用: {tool_name}")
                        
                        # 检查工具名称并进行特殊处理
                        if tool_name == "list_llm_providers" or tool_name == "mcp_doris_list_llm_providers":
                            logging.info(f"特殊处理list_llm_providers工具调用")
                            # 返回默认LLM供应商列表
                            response_json = {
                                "jsonrpc": "2.0",
                                "id": data.get("id", str(uuid.uuid4())),
                                "result": {
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": json.dumps({
                                                "providers": [
                                                    {
                                                        "name": "OpenAI",
                                                        "models": ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"]
                                                    },
                                                    {
                                                        "name": "Anthropic",
                                                        "models": ["claude-2", "claude-instant-1", "claude-3-opus", "claude-3-sonnet"]
                                                    }
                                                ]
                                            }, ensure_ascii=False, indent=2)
                                        }
                                    ]
                                }
                            }
                            return JSONResponse(
                                content=response_json,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*"
                                }
                            )
                        
                        elif tool_name == "get_nl2sql_status" or tool_name == "mcp_doris_get_nl2sql_status":
                            logging.info(f"特殊处理get_nl2sql_status工具调用")
                            # 返回默认状态
                            from datetime import datetime
                            response_json = {
                                "jsonrpc": "2.0",
                                "id": data.get("id", str(uuid.uuid4())),
                                "result": {
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": json.dumps({
                                                "status": "idle",
                                                "message": "系统准备就绪",
                                                "timestamp": datetime.now().isoformat()
                                            }, ensure_ascii=False, indent=2)
                                        }
                                    ]
                                }
                            }
                            return JSONResponse(
                                content=response_json,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*"
                                }
                            )
                        
                        elif tool_name == "list_database_tables" or tool_name == "mcp_doris_list_database_tables":
                            logging.info(f"特殊处理list_database_tables工具调用")
                            # 返回默认表列表
                            response_json = {
                                "jsonrpc": "2.0",
                                "id": data.get("id", str(uuid.uuid4())),
                                "result": {
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": json.dumps({
                                                "tables": [
                                                    {"name": "sales", "description": "销售数据表"},
                                                    {"name": "products", "description": "产品信息表"},
                                                    {"name": "customers", "description": "客户信息表"}
                                                ]
                                            }, ensure_ascii=False, indent=2)
                                        }
                                    ]
                                }
                            }
                            return JSONResponse(
                                content=response_json,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*"
                                }
                            )
                        
                        # 处理nl2sql_query标准查询
                        elif tool_name == "nl2sql_query" or tool_name == "mcp_doris_nl2sql_query":
                            # 导入NL2SQLService
                            from src.nl2sql_service import NL2SQLService
                            logging.info(f"处理nl2sql_query工具调用，使用真实NL2SQLService")
                            
                            # 获取查询参数
                            query = ""
                            if isinstance(data.get("params"), dict):
                                if "arguments" in data["params"]:
                                    if "query" in data["params"]["arguments"]:
                                        query = data["params"]["arguments"]["query"]
                                elif "query" in data["params"]:
                                    query = data["params"]["query"]
                            elif "arguments" in data:
                                if "query" in data["arguments"]:
                                    query = data["arguments"]["query"]
                            
                            if not query:
                                # 尝试从random_string中获取
                                if isinstance(data.get("params"), dict) and "random_string" in data["params"]:
                                    query = data["params"]["random_string"]
                            
                            logging.info(f"提取到的查询内容: {query}")
                            
                            try:
                                # 实例化NL2SQLService并处理查询
                                nl2sql_service = NL2SQLService()
                                result = nl2sql_service.process_query(query)
                                
                                # 格式化返回结果
                                response_json = {
                                    "jsonrpc": "2.0",
                                    "id": data.get("id", str(uuid.uuid4())),
                                    "result": {
                                        "content": [
                                            {
                                                "type": "text",
                                                "text": json.dumps(result, ensure_ascii=False, indent=2)
                                            }
                                        ]
                                    }
                                }
                                return JSONResponse(
                                    content=response_json,
                                    headers={
                                        "Access-Control-Allow-Origin": "*",
                                        "Access-Control-Allow-Methods": "*",
                                        "Access-Control-Allow-Headers": "*"
                                    }
                                )
                            except Exception as e:
                                logging.error(f"使用NL2SQLService处理查询时出错: {str(e)}")
                                error_json = {
                                    "jsonrpc": "2.0",
                                    "id": data.get("id", str(uuid.uuid4())),
                                    "error": {
                                        "code": -32000,
                                        "message": f"查询处理失败: {str(e)}"
                                    }
                                }
                                return JSONResponse(
                                    content=error_json,
                                    status_code=500,
                                    headers={
                                        "Access-Control-Allow-Origin": "*",
                                        "Access-Control-Allow-Methods": "*",
                                        "Access-Control-Allow-Headers": "*"
                                    }
                                )
                        
                        # 处理流式工具调用
                        elif tool_name == "nl2sql_query_stream" or tool_name == "mcp_doris_nl2sql_query_stream":
                            # 导入流式处理器
                            from src.nl2sql_stream_processor import StreamNL2SQLProcessor
                            logging.info(f"处理流式查询工具调用: {tool_name}")
                            
                            # 获取查询参数
                            query = ""
                            if isinstance(data.get("params"), dict):
                                if "arguments" in data["params"]:
                                    if "query" in data["params"]["arguments"]:
                                        query = data["params"]["arguments"]["query"]
                                elif "query" in data["params"]:
                                    query = data["params"]["query"]
                            elif "arguments" in data:
                                if "query" in data["arguments"]:
                                    query = data["arguments"]["query"]
                            
                            if not query:
                                # 尝试从random_string中获取
                                if isinstance(data.get("params"), dict) and "random_string" in data["params"]:
                                    query = data["params"]["random_string"]
                            
                            logging.info(f"提取到的流式查询内容: {query}")
                            
                            # 创建直接的流式响应
                            session_id = data.get("session_id", "")
                            adapter = MCPAdapterMiddleware(None)
                            
                            # 这里要用StreamingResponse返回流式响应
                            return StreamingResponse(
                                adapter.stream_tool_response(data, tool_name, {"query": query}, session_id),
                                media_type="text/event-stream",
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Cache-Control": "no-cache",
                                    "Connection": "keep-alive"
                                }
                            )
                        
                        # 正常调用工具
                        try:
                            # 检查是否有tool_registry
                            if hasattr(request.app.state, "tool_registry"):
                                tool_registry = request.app.state.tool_registry
                                
                                # 获取工具实例
                                logging.info(f"从工具注册表中获取工具: {tool_name}")
                                tool_instance = tool_registry.get_tool(tool_name)
                                
                                if not tool_instance:
                                    # 如果工具不存在，返回错误
                                    logging.error(f"工具不存在: {tool_name}")
                                    error_json = {
                                        "jsonrpc": "2.0",
                                        "id": data.get("id", str(uuid.uuid4())),
                                        "error": {
                                            "code": -32601,
                                            "message": f"工具 '{tool_name}' 不存在"
                                        }
                                    }
                                    return JSONResponse(
                                        content=error_json,
                                        status_code=404,
                                        headers={
                                            "Access-Control-Allow-Origin": "*",
                                            "Access-Control-Allow-Methods": "*",
                                            "Access-Control-Allow-Headers": "*"
                                        }
                                    )
                                
                                # 从数据中获取参数
                                params = {}
                                if "arguments" in data:
                                    params = data["arguments"]
                                elif "params" in data:
                                    if isinstance(data["params"], dict) and "arguments" in data["params"]:
                                        params = data["params"]["arguments"]
                                    else:
                                        params = data["params"]
                                
                                # 使用工具执行
                                logging.info(f"执行工具 {tool_name}, 参数: {params}")
                                result = await tool_instance(**params)
                                
                                # 格式化返回结果
                                if isinstance(result, str):
                                    # 如果结果是字符串，尝试解析为JSON
                                    try:
                                        json_result = json.loads(result)
                                        formatted_result = {
                                            "content": [
                                                {
                                                    "type": "text",
                                                    "text": json.dumps(json_result, ensure_ascii=False, indent=2)
                                                }
                                            ]
                                        }
                                    except json.JSONDecodeError:
                                        # 如果不是有效的JSON，直接作为文本返回
                                        formatted_result = {
                                            "content": [
                                                {
                                                    "type": "text",
                                                    "text": result
                                                }
                                            ]
                                        }
                                else:
                                    # 如果结果是其他类型，转换为JSON
                                    formatted_result = {
                                        "content": [
                                            {
                                                "type": "text",
                                                "text": json.dumps(result, ensure_ascii=False, indent=2)
                                            }
                                        ]
                                    }
                                
                                # 返回结果
                                response_json = {
                                    "jsonrpc": "2.0",
                                    "id": data.get("id", str(uuid.uuid4())),
                                    "result": formatted_result
                                }
                                return JSONResponse(
                                    content=response_json,
                                    headers={
                                        "Access-Control-Allow-Origin": "*",
                                        "Access-Control-Allow-Methods": "*",
                                        "Access-Control-Allow-Headers": "*"
                                    }
                                )
                            else:
                                # 如果没有工具注册表，可能是在直接使用FastMCP实例
                                logging.info(f"工具注册表不存在，尝试使用FastMCP实例调用工具")
                                
                                # 构建工具参数
                                tool_params = {
                                    "name": tool_name
                                }
                                
                                # 获取工具参数
                                if "arguments" in data:
                                    tool_params["arguments"] = data["arguments"]
                                elif "params" in data:
                                    if isinstance(data["params"], dict) and "arguments" in data["params"]:
                                        tool_params["arguments"] = data["params"]["arguments"]
                                    else:
                                        tool_params["arguments"] = data["params"]
                                
                                # 检查app状态中是否有MCP实例
                                if hasattr(request.app.state, "mcp"):
                                    mcp = request.app.state.mcp
                                    
                                    # 尝试调用工具
                                    try:
                                        # 构建工具调用格式
                                        tool_call = {
                                            "jsonrpc": "2.0",
                                            "method": "mcp/callTool",
                                            "params": tool_params,
                                            "id": data.get("id", str(uuid.uuid4()))
                                        }
                                        
                                        # 调用MCP处理工具
                                        logging.info(f"使用MCP实例调用工具: {tool_name}")
                                        result = await mcp.process_message(tool_call)
                                        
                                        # 格式化返回结果
                                        if isinstance(result, dict) and "result" in result:
                                            formatted_result = result["result"]
                                        else:
                                            # 如果结果不包含result字段，格式化为content结构
                                            formatted_result = {
                                                "content": [
                                                    {
                                                        "type": "text",
                                                        "text": json.dumps(result, ensure_ascii=False, indent=2)
                                                    }
                                                ]
                                            }
                                        
                                        # 返回结果
                                        response_json = {
                                            "jsonrpc": "2.0",
                                            "id": data.get("id", str(uuid.uuid4())),
                                            "result": formatted_result
                                        }
                                        return JSONResponse(
                                            content=response_json,
                                            headers={
                                                "Access-Control-Allow-Origin": "*",
                                                "Access-Control-Allow-Methods": "*",
                                                "Access-Control-Allow-Headers": "*"
                                            }
                                        )
                                    except Exception as e:
                                        # 处理工具调用错误
                                        logging.error(f"MCP工具调用失败: {str(e)}")
                                        error_json = {
                                            "jsonrpc": "2.0",
                                            "id": data.get("id", str(uuid.uuid4())),
                                            "error": {
                                                "code": -32000,
                                                "message": f"工具调用失败: {str(e)}"
                                            }
                                        }
                                        return JSONResponse(
                                            content=error_json,
                                            status_code=500,
                                            headers={
                                                "Access-Control-Allow-Origin": "*",
                                                "Access-Control-Allow-Methods": "*",
                                                "Access-Control-Allow-Headers": "*"
                                            }
                                        )
                                else:
                                    # 如果没有MCP实例，返回错误
                                    logging.error(f"MCP实例不存在")
                                    error_json = {
                                        "jsonrpc": "2.0",
                                        "id": data.get("id", str(uuid.uuid4())),
                                        "error": {
                                            "code": -32603,
                                            "message": "内部错误: MCP实例不存在"
                                        }
                                    }
                                    return JSONResponse(
                                        content=error_json,
                                        status_code=500,
                                        headers={
                                            "Access-Control-Allow-Origin": "*",
                                            "Access-Control-Allow-Methods": "*",
                                            "Access-Control-Allow-Headers": "*"
                                        }
                                    )
                        except Exception as e:
                            # 处理一般错误
                            logging.error(f"工具调用处理失败: {str(e)}")
                            error_json = {
                                "jsonrpc": "2.0",
                                "id": data.get("id", str(uuid.uuid4())),
                                "error": {
                                    "code": -32000,
                                    "message": str(e)
                                }
                            }
                            return JSONResponse(
                                content=error_json,
                                status_code=500,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*"
                                }
                            )
                
                # 如果不是旧格式请求，继续处理
                return await call_next(request)
                
            except Exception as e:
                logger.error(f"处理请求时出错: {str(e)}")
                return JSONResponse({
                    "success": False,
                    "error": str(e)
                })
        
        # 对于其他请求，继续处理
        return await call_next(request)
    
    async def stream_nl2sql_response(self, data: Dict[str, Any]) -> AsyncGenerator[str, None]:
        """流式处理NL2SQL查询响应"""
        # 获取查询参数
        query = data.get("arguments", {}).get("query", "")
        request_id = data.get("id", "")
        
        # 记录开始处理请求
        start_time = datetime.datetime.now()
        
        # 记录请求到审计日志
        from src.utils.logger import audit_logger
        audit_data = {
            "request_id": request_id,
            "timestamp": start_time.isoformat(),
            "query": query,
            "status": "started",
            "mode": "stream"
        }
        audit_logger.info(json.dumps(audit_data))
        
        # 获取流式处理器
        from src.nl2sql_stream_processor import StreamNL2SQLProcessor
        stream_processor = StreamNL2SQLProcessor()
        
        # 创建回调函数
        async def callback(content: str, metadata: Dict[str, Any]):
            logger.info(f"NL2SQL流式处理回调: {metadata.get('type', 'unknown')} - {content[:50]}...")
            
            # 发送思考过程
            if metadata.get("type") == "thinking":
                # 调整为前端期望的格式
                thinking_data = {
                    "type": "thinking",
                    "stage": metadata.get("stage", "thinking"),
                    "content": content,
                    "progress": metadata.get("progress", 0)
                }
                event = self.format_stream_event(ResponseType.THINKING, thinking_data)
                logger.debug(f"发送思考事件: {event[:100]}...")
                yield event
                
            # 发送进度更新
            elif metadata.get("type") == "progress":
                progress_data = {
                    "type": "progress",
                    "content": content,
                    "metadata": metadata
                }
                event = self.format_stream_event(ResponseType.PROGRESS, progress_data)
                logger.debug(f"发送进度事件: {event[:100]}...")
                yield event
                
            # 发送部分结果
            elif metadata.get("type") == "partial":
                partial_data = {
                    "type": "partial",
                    "content": content,
                    "metadata": metadata
                }
                event = self.format_stream_event(ResponseType.PARTIAL, partial_data)
                logger.debug(f"发送部分结果事件: {event[:100]}...")
                yield event
                
            # 发送最终结果
            elif metadata.get("type") == "final":
                final_data = {
                    "type": "final",
                    "content": content,
                    "metadata": metadata
                }
                event = self.format_stream_event(ResponseType.FINAL, final_data)
                logger.debug(f"发送最终结果事件: {event[:100]}...")
                yield event
            
            # 记录流式结果到审计日志
            audit_data = {
                "request_id": request_id,
                "timestamp": datetime.datetime.now().isoformat(),
                "query": query,
                "status": "streaming",
                "content": content,
                "metadata": metadata
            }
            audit_logger.info(json.dumps(audit_data))
        
        try:
            # 处理查询并生成流式响应
            logger.info(f"开始调用流式处理器 process_stream 方法处理查询: {query}")
            
            # 正确处理回调方法的调用，使其绑定到当前实例
            bound_callback = callback.__get__(self, type(self))
            
            # 处理查询
            async for event in stream_processor.process_stream(query, bound_callback):
                # 确保返回的是字符串
                if isinstance(event, str):
                    yield event
                else:
                    # 如果返回的不是字符串，可能是一个字典，需要格式化
                    logger.warning(f"process_stream返回了非字符串事件: {type(event)}, 尝试格式化")
                    if isinstance(event, dict):
                        event_type = event.get("type", "thinking")
                        formatted_event = self.format_stream_event(event_type, event)
                        yield formatted_event
                    else:
                        logger.error(f"无法处理的事件类型: {type(event)}, 跳过")
        
        except Exception as e:
            logger.error(f"流式处理查询出错: {str(e)}")
            error_response = {
                "type": "error",
                "error": {
                    "message": f"处理查询出错: {str(e)}"
                }
            }
            yield self.format_stream_event(ResponseType.ERROR, error_response)
    
    async def stream_tool_response(self, request_data, tool_name, tool_args, session_id=None):
        """返回流式工具响应"""
        logger.info(f"开始处理流式工具调用: {tool_name}")
        
        # 确保连贯性，避免可能的编码错误
        header_sent = False
        
        if tool_name == "nl2sql_query_stream":
            try:
                # 使用NL2SQLStreamProcessor处理流式响应
                logger.info(f"开始处理NL2SQL流式查询: {tool_args}")
                
                # 获取请求体中的查询参数
                query = tool_args.get("query", "")
                if not query:
                    query = request_data.get("query", "")
                
                if not query:
                    error_msg = "未提供查询参数"
                    logger.error(error_msg)
                    yield f"data: {json.dumps({'type': 'error', 'message': error_msg}, ensure_ascii=False)}\n\n"
                    yield "event: close\ndata: {}\n\n"
                    return
                
                # 使用流处理器处理查询
                processor = self.get_stream_processor(session_id)
                
                # 定义回调函数，将处理结果格式化为SSE事件
                async def simple_callback(message, metadata):
                    event_type = metadata.get("type", "thinking")
                    stage = metadata.get("stage", "")
                    progress = metadata.get("progress", 0)
                    
                    # 构建事件数据
                    event_data = {
                        "type": event_type,
                        "content": message,
                        "stage": stage,
                        "progress": progress
                    }
                    
                    # 检查是否有额外的元数据需要传递
                    for key in metadata:
                        if key not in ["type", "stage", "progress"]:
                            event_data[key] = metadata[key]
                        
                    # 如果是final类型且消息内容是JSON字符串，尝试解析并添加完整的结果数据
                    if event_type == "final" and isinstance(message, str):
                        try:
                            result_data = json.loads(message)
                            # 保留原始消息，同时添加解析后的结果对象
                            event_data["result"] = result_data
                            
                            # 检查结果中是否包含业务分析和可视化建议
                            if "analysis" in result_data:
                                event_data["analysis"] = result_data["analysis"]
                                logger.info(f"添加业务分析到最终事件: {result_data['analysis'][:100]}...")
                            if "visualization" in result_data:
                                event_data["visualization"] = result_data["visualization"]
                                logger.info(f"添加可视化建议到最终事件: {result_data['visualization'][:100]}...")
                                
                            # 检查嵌套结构中的分析和可视化
                            if "result" in result_data and isinstance(result_data["result"], dict):
                                if "analysis" in result_data["result"] and "analysis" not in event_data:
                                    event_data["analysis"] = result_data["result"]["analysis"]
                                    logger.info(f"从嵌套结果中添加业务分析: {result_data['result']['analysis'][:100]}...")
                                if "visualization" in result_data["result"] and "visualization" not in event_data:
                                    event_data["visualization"] = result_data["result"]["visualization"]
                                    logger.info(f"从嵌套结果中添加可视化建议: {result_data['result']['visualization'][:100]}...")
                                
                            logger.info(f"解析final事件结果JSON成功，包含分析: {'analysis' in event_data}, 包含可视化: {'visualization' in event_data}")
                        except json.JSONDecodeError:
                            logger.warning(f"无法解析final事件消息为JSON: {message[:100]}...")
                    
                    # 格式化为SSE事件
                    sse_event = f"data: {json.dumps(event_data, ensure_ascii=False)}\n\n"
                    logger.debug(f"生成SSE事件: {sse_event[:100]}...")
                    return sse_event
                
                # 处理流
                logger.info(f"开始流式处理查询: {query}")
                
                # 处理查询
                has_final_event = False  # 用于跟踪是否发送了最终结果事件
                async for event in processor.process_stream(query, simple_callback):
                    if event and isinstance(event, str):
                        logger.debug(f"转发事件: {event[:50]}...")
                        
                        # 检查是否为最终结果事件
                        if '"type":"final"' in event or '"type": "final"' in event:
                            has_final_event = True
                            logger.info("检测到最终结果事件，标记为已发送")
                        
                        yield event
                
                # 确保至少有一个最终结果事件被发送
                if not has_final_event:
                    logger.warning("未检测到最终结果事件，尝试生成默认结果")
                    try:
                        # 构造默认的最终结果事件
                        default_result = {
                            "type": "final",
                            "data": {
                                "content": json.dumps({
                                    "message": "查询处理完成，但未获取到结果",
                                    "query": query
                                }, ensure_ascii=False),
                                "error": False
                            }
                        }
                        final_event = f"data: {json.dumps(default_result, ensure_ascii=False)}\n\n"
                        logger.info("发送默认的最终结果事件")
                        yield final_event
                    except Exception as e:
                        logger.error(f"发送默认最终结果失败: {str(e)}")
                
                # 最后发送结束信号
                logger.info("所有事件处理完成，发送关闭信号")
                yield "event: close\ndata: {}\n\n"
                
            except Exception as e:
                logger.error(f"处理NL2SQL流式查询时错误: {str(e)}")
                logger.exception("详细错误堆栈")
                
                # 如果尚未发送任何事件，确保发送错误
                if not header_sent:
                    error_data = {
                        "type": "error",
                        "message": f"处理查询时错误: {str(e)}"
                    }
                    yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
                    yield "event: close\ndata: {}\n\n"
        
        else:
            # 处理其他工具的流式响应
            yield f"data: {json.dumps({'type': 'message', 'content': '不支持的流式工具: ' + tool_name}, ensure_ascii=False)}\n\n"
            yield "event: close\ndata: {}\n\n"
    
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

    def get_stream_processor(self, session_id=None):
        """获取NL2SQL流处理器实例

        Args:
            session_id: 会话ID，用于区分不同会话的处理器

        Returns:
            StreamNL2SQLProcessor: NL2SQL流处理器实例
        """
        from src.nl2sql_stream_processor import StreamNL2SQLProcessor
        
        # 如果需要，可以基于session_id来管理不同的处理器实例
        # 例如，可以使用字典来存储不同会话的处理器
        # 这里简单返回一个新的处理器实例
        logger.info(f"为会话 {session_id} 创建新的NL2SQL流处理器")
        return StreamNL2SQLProcessor()

    # 流式处理NL2SQL查询的终端点
    async def process_nl2sql_query_stream(self, request: Request):
        """处理NL2SQL查询流式接口"""
        try:
            # 解析请求体
            body = await request.json()
            logger.info(f"收到NL2SQL流式查询请求: {json.dumps(body)[:200]}...")

            # 提取查询参数
            query = ""
            if "params" in body and "query" in body["params"]:
                query = body["params"]["query"]
            elif "arguments" in body and "query" in body["arguments"]:
                query = body["arguments"]["query"]
            elif "query" in body:
                query = body["query"]
            
            if not query:
                # 返回错误
                error_json = {"type": "error", "message": "查询参数为空"}
                return JSONResponse(content=error_json, status_code=400)
            
            # 获取会话ID
            session_id = body.get("session_id", "")
            
            # 创建流式处理器实例
            processor = self.get_stream_processor(session_id)
            
            # 定义SSE流式响应生成器
            async def event_generator():
                try:
                    # 定义回调函数，将事件格式化为SSE事件
                    async def callback(message, metadata):
                        event_type = metadata.get("type", "thinking")
                        stage = metadata.get("stage", "")
                        progress = metadata.get("progress", 0)
                        
                        # 构建事件数据
                        event_data = {
                            "type": event_type,
                            "content": message,
                            "stage": stage,
                            "progress": progress
                        }
                        
                        # 格式化为SSE事件
                        return f"data: {json.dumps(event_data, ensure_ascii=False)}\n\n"
                    
                    # 调用处理器处理查询，并生成流式响应
                    logger.info(f"开始流式处理查询: {query}")
                    async for event in processor.process_stream(query, callback):
                        if event:
                            logger.debug(f"生成事件: {event[:100]}...")
                            yield event
                    
                    # 发送结束信号
                    yield "event: close\ndata: {}\n\n"
                    logger.info("流式查询处理完成")
                
                except Exception as e:
                    logger.error(f"流式处理查询时错误: {str(e)}")
                    logger.exception("详细错误堆栈")
                    # 发送错误事件
                    error_data = {
                        "type": "error",
                        "message": f"处理查询时错误: {str(e)}"
                    }
                    yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
                    yield "event: close\ndata: {}\n\n"
            
            # 返回流式响应
            logger.info("创建流式响应对象")
            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*"
                }
            )
        
        except Exception as e:
            logger.error(f"处理NL2SQL流式查询请求时出错: {str(e)}")
            logger.exception("详细错误堆栈")
            error_json = {"type": "error", "message": f"处理请求时出错: {str(e)}"}
            return JSONResponse(content=error_json, status_code=500)

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

# 简单测试适配器
if __name__ == "__main__":
    # 测试旧格式到新格式的转换
    old_format = {
        "id": "123",
        "session_id": "test-session",
        "type": "tool",
        "tool": "nl2sql_query",
        "params": {"query": "历史销量最高的是哪一年？"}
    }
    
    # 转换为新格式
    new_format = {
        "jsonrpc": "2.0",
        "id": old_format["id"],
        "method": old_format["tool"],
        "params": old_format["params"]
    }
    
    print("旧格式:", json.dumps(old_format, indent=2))
    print("新格式:", json.dumps(new_format, indent=2)) 

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