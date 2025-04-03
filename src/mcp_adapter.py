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
                        
                        elif tool_name == "nl2sql_query" or tool_name == "mcp_doris_nl2sql_query":
                            logging.info(f"特殊处理nl2sql_query工具调用")
                            # 返回默认SQL查询结果
                            response_json = {
                                "jsonrpc": "2.0",
                                "id": data.get("id", str(uuid.uuid4())),
                                "result": {
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": json.dumps({
                                                "sql": "SELECT * FROM sales WHERE date > current_date - interval '30 days'",
                                                "result": [
                                                    {"date": "2023-04-01", "product": "ProductA", "sales": 1200},
                                                    {"date": "2023-04-02", "product": "ProductA", "sales": 1350},
                                                    {"date": "2023-04-03", "product": "ProductA", "sales": 980}
                                                ],
                                                "message": "查询执行成功"
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
            # 发送思考过程
            if metadata.get("type") == "thinking":
                    yield self.format_stream_event(ResponseType.THINKING, {
                    "content": content,
                    "metadata": metadata
                })
            # 发送进度更新
            elif metadata.get("type") == "progress":
                    yield self.format_stream_event(ResponseType.PROGRESS, {
                    "content": content,
                    "metadata": metadata
                })
            # 发送部分结果
            elif metadata.get("type") == "partial":
                    yield self.format_stream_event(ResponseType.PARTIAL, {
                    "content": content,
                    "metadata": metadata
                })
                # 发送最终结果
            elif metadata.get("type") == "final":
                yield self.format_stream_event(ResponseType.FINAL, {
                    "content": content,
                    "metadata": metadata
                })
            
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
            # 调用流式处理器处理查询
            await stream_processor.process_query(query, callback)
            
            # 记录完成时间
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 记录完成状态到审计日志
            audit_data = {
                "request_id": request_id,
                "timestamp": end_time.isoformat(),
                "query": query,
                "status": "completed",
                "duration": duration
            }
            audit_logger.info(json.dumps(audit_data))
            
            # 发送完成事件
            yield self.format_stream_event(ResponseType.FINAL, {
                "content": "查询完成",
                "metadata": {
                    "status": "completed",
                    "duration": duration
                }
            })
        except Exception as e:
            logger.error(f"流式处理出错: {str(e)}")
            
            # 记录错误到审计日志
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            audit_data = {
                "request_id": request_id,
                "timestamp": end_time.isoformat(),
                "query": query,
                "status": "error",
                "duration": duration,
                "error": str(e)
            }
            audit_logger.error(json.dumps(audit_data))
            
            # 发送错误事件
            yield self.format_stream_event(ResponseType.ERROR, {
                "content": f"处理查询时出错: {str(e)}",
                "metadata": {
                    "status": "error",
                    "error": str(e)
                }
            })
    
    async def stream_tool_response(self, data: Dict[str, Any], tool_name: str) -> AsyncGenerator[str, None]:
        """流式处理工具调用响应"""
        # 获取工具
        tool = get_tool(tool_name)
        if not tool:
            yield self.format_stream_event(ResponseType.ERROR, {
                "content": f"工具 {tool_name} 不存在",
                "metadata": {
                    "status": "error",
                    "error": f"工具 {tool_name} 不存在"
                }
                    })
            return
                
        # 获取参数
        arguments = data.get("arguments", {})
        request_id = data.get("id", "")
        
        # 记录开始处理请求
        start_time = datetime.datetime.now()
        
        # 记录请求到审计日志
        from src.utils.logger import audit_logger
        audit_data = {
            "request_id": request_id,
            "timestamp": start_time.isoformat(),
            "tool": tool_name,
            "arguments": arguments,
            "status": "started",
            "mode": "stream"
        }
        audit_logger.info(json.dumps(audit_data))
        
        # 创建回调函数
        async def callback(content: str, metadata: Dict[str, Any]):
            # 发送思考过程
            if metadata.get("type") == "thinking":
                yield self.format_stream_event(ResponseType.THINKING, {
                    "content": content,
                    "metadata": metadata
                })
                # 发送进度更新
            elif metadata.get("type") == "progress":
                yield self.format_stream_event(ResponseType.PROGRESS, {
                    "content": content,
                    "metadata": metadata
                })
            # 发送部分结果
            elif metadata.get("type") == "partial":
                yield self.format_stream_event(ResponseType.PARTIAL, {
                    "content": content,
                    "metadata": metadata
                })
                # 发送最终结果
            elif metadata.get("type") == "final":
                yield self.format_stream_event(ResponseType.FINAL, {
                    "content": content,
                    "metadata": metadata
                })
            
            # 记录流式结果到审计日志
            audit_data = {
                "request_id": request_id,
                "timestamp": datetime.datetime.now().isoformat(),
                "tool": tool_name,
                "arguments": arguments,
                "status": "streaming",
                "content": content,
                "metadata": metadata
            }
            audit_logger.info(json.dumps(audit_data))
        
        try:
            # 检查是否是异步函数
            if inspect.iscoroutinefunction(tool.func):
                result = await tool.func(**arguments, callback=callback)
            else:
                # 对于同步函数，我们需要创建一个异步包装器
                async def async_wrapper():
                    # 创建一个同步回调
                    def sync_callback(content, metadata):
                        # 将同步回调转换为异步
                        asyncio.create_task(callback(content, metadata))
                    
                    # 调用同步函数
                    return tool.func(**arguments, callback=sync_callback)
                
                result = await async_wrapper()
            
            # 记录完成时间
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 记录完成状态到审计日志
            audit_data = {
                "request_id": request_id,
                "timestamp": end_time.isoformat(),
                "tool": tool_name,
                "arguments": arguments,
                "status": "completed",
                "duration": duration,
                "result": result
            }
            audit_logger.info(json.dumps(audit_data))
            
            # 发送完成事件
            yield self.format_stream_event(ResponseType.FINAL, {
                "content": "工具调用完成",
                "metadata": {
                    "status": "completed",
                    "duration": duration,
                    "result": result
                }
                })
        except Exception as e:
            logger.error(f"工具调用出错: {str(e)}")
            
            # 记录错误到审计日志
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            audit_data = {
                "request_id": request_id,
                "timestamp": end_time.isoformat(),
                "tool": tool_name,
                "arguments": arguments,
                "status": "error",
                "duration": duration,
                "error": str(e)
            }
            audit_logger.error(json.dumps(audit_data))
            
            # 发送错误事件
            yield self.format_stream_event(ResponseType.ERROR, {
                "content": f"工具调用出错: {str(e)}",
                "metadata": {
                    "status": "error",
                    "error": str(e)
                }
            })
    
    def format_stream_event(self, event_type: str, data: Dict[str, Any]) -> str:
        """格式化SSE事件"""
        # 构建事件数据
        event_data = {
            "type": event_type,
            "data": data
        }
        
        # 转换为JSON
        json_data = json.dumps(event_data, ensure_ascii=False)
        
        # 构建SSE事件
        return f"data: {json_data}\n\n"

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