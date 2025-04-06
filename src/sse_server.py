#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Doris MCP SSE 服务器实现

基于MCP的SseServerTransport实现一个符合标准的MCP SSE服务器，
支持和客户端的双向通信，同时与现有的Doris-MCP-Server集成。
"""

import os
import asyncio
import json
import uuid
import logging
import time
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from starlette.applications import Starlette
from src.tools.tool_registry import get_registry

# 获取日志记录器
logger = logging.getLogger("doris-mcp-sse")

class DorisMCPSseServer:
    """Doris MCP SSE 服务器实现"""
    
    def __init__(self, mcp_server, app: FastAPI):
        """
        初始化Doris MCP SSE服务器
        
        参数:
            mcp_server: FastMCP服务器实例
            app: FastAPI应用实例
        """
        self.mcp_server = mcp_server
        
        # 确保app是FastAPI实例
        if not isinstance(app, FastAPI):
            logger.warning("传入的应用不是FastAPI实例，将使用现有的FastAPI实例")
        
        self.app = app
        
        # 添加CORS中间件
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["http://localhost:3100"],  # 指定前端域名
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"]
        )
        
        # 客户端会话管理
        self.client_sessions = {}
        
        # 设置SSE路由
        self.setup_sse_routes()
        
        # 注册启动事件
        @self.app.on_event("startup")
        async def startup_event():
            # 启动会话清理任务
            asyncio.create_task(self.cleanup_idle_sessions())
            # 启动定时发送状态更新的任务
            asyncio.create_task(self.send_periodic_updates())
    
    def setup_sse_routes(self):
        """设置SSE相关路由"""
        
        @self.app.get("/status")
        async def status():
            """获取服务器状态"""
            try:
                # 获取工具列表
                tools = await self.mcp_server.list_tools()
                tool_names = [tool.name if hasattr(tool, 'name') else str(tool) for tool in tools]
                logger.info(f"获取工具列表，当前注册的工具: {tool_names}")
                
                # 获取资源列表
                resources = await self.mcp_server.list_resources()
                resource_names = [res.name if hasattr(res, 'name') else str(res) for res in resources]
                
                # 获取提示模板列表
                prompts = await self.mcp_server.list_prompts()
                prompt_names = [prompt.name if hasattr(prompt, 'name') else str(prompt) for prompt in prompts]
                
                return {
                    "status": "running",
                    "name": self.mcp_server.name,
                    "mode": "mcp_sse",
                    "clients": len(self.client_sessions),
                    "tools": tool_names,
                    "resources": resource_names,
                    "prompts": prompt_names
                }
            except Exception as e:
                logger.error(f"获取状态时出错: {str(e)}")
                return {
                    "status": "error",
                    "error": str(e)
                }
        
        @self.app.get("/mcp")
        async def mcp_sse(request: Request):
            """SSE服务入口点，建立客户端连接"""
            # 生成会话ID
            session_id = str(uuid.uuid4())
            logger.info(f"新的SSE连接 [会话ID: {session_id}]")
            
            # 创建客户端会话
            self.client_sessions[session_id] = {
                "client_id": request.headers.get("X-Client-ID", f"client_{str(uuid.uuid4())[:8]}"),
                "created_at": time.time(),
                "last_active": time.time(),
                "queue": asyncio.Queue()
            }
            
            # 立即将端点信息放入队列
            endpoint_data = f"/mcp/messages?session_id={session_id}"
            await self.client_sessions[session_id]["queue"].put({
                "event": "endpoint",
                "data": endpoint_data
            })
            
            # 创建事件发生器
            async def event_generator():
                try:
                    while True:
                        # 使用超时获取新消息，以便能够检测客户端断开连接
                        try:
                            message = await asyncio.wait_for(
                                self.client_sessions[session_id]["queue"].get(),
                                timeout=30
                            )
                            
                            # 检查是否是关闭命令
                            if isinstance(message, dict) and message.get("event") == "close":
                                logger.info(f"收到关闭命令 [会话ID: {session_id}]")
                                break
                            
                            # 返回消息
                            if isinstance(message, dict):
                                if "event" in message:
                                    # 如果有event字段，则是系统事件
                                    event_type = message["event"]
                                    event_data = message["data"]
                                    yield {
                                        "event": event_type,
                                        "data": event_data
                                    }
                                else:
                                    # 否则是普通消息，使用message事件
                                    yield {
                                        "event": "message",
                                        "data": json.dumps(message)
                                    }
                            elif isinstance(message, str):
                                # 如果是字符串，直接发送
                                yield {
                                    "event": "message",
                                    "data": message
                                }
                            else:
                                # 其他类型，转换为JSON
                                yield {
                                    "event": "message",
                                    "data": json.dumps(message)
                                }
                        except asyncio.TimeoutError:
                            # 发送ping保持连接
                            yield {
                                "event": "ping",
                                "data": "keepalive"
                            }
                            continue
                except asyncio.CancelledError:
                    # 连接被取消
                    logger.info(f"SSE连接被取消 [会话ID: {session_id}]")
                except Exception as e:
                    # 发生其他错误
                    logger.error(f"SSE事件生成器出错 [会话ID: {session_id}]: {str(e)}")
                finally:
                    # 清理会话
                    if session_id in self.client_sessions:
                        logger.info(f"清理会话 [会话ID: {session_id}]")
                        del self.client_sessions[session_id]
            
            # 返回标准SSE响应
            return EventSourceResponse(
                event_generator(),
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive"
                }
            )
        
        @self.app.options("/mcp/messages")
        async def mcp_messages_options(request: Request):
            """处理预检请求"""
            return JSONResponse(
                {},
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*"
                }
            )
        
        @self.app.post("/mcp/messages")
        async def mcp_messages_handler(request: Request):
            """处理客户端消息请求，使用类方法"""
            return await self.mcp_message(request)
    
    async def cleanup_idle_sessions(self):
        """清理空闲的客户端会话"""
        while True:
            await asyncio.sleep(60)  # 每分钟检查一次
            current_time = time.time()
            
            # 找出空闲超过5分钟的会话
            idle_sessions = []
            for session_id, session in self.client_sessions.items():
                if current_time - session["last_active"] > 300:  # 5分钟
                    idle_sessions.append(session_id)
            
            # 关闭并移除空闲会话
            for session_id in idle_sessions:
                try:
                    # 发送关闭消息
                    await self.client_sessions[session_id]["queue"].put({"event": "close"})
                    # 清理会话
                    logger.info(f"已清理空闲会话: {session_id}")
                except Exception as e:
                    logger.error(f"清理会话出错: {str(e)}")
                finally:
                    # 确保会话被移除
                    if session_id in self.client_sessions:
                        del self.client_sessions[session_id]
    
    async def send_periodic_updates(self):
        """定期向所有客户端发送状态更新"""
        while True:
            try:
                # 每5秒发送一次状态更新
                await asyncio.sleep(5)
                
                # 如果没有客户端连接，跳过本次更新
                if not self.client_sessions:
                    continue
                
                # 获取当前状态
                status_data = {
                    "timestamp": time.time(),
                    "clients_count": len(self.client_sessions),
                    "server_status": "running"
                }
                
                # 尝试获取NL2SQL状态
                try:
                    # 获取MCP实例
                    mcp = self.app.state.mcp if hasattr(self.app.state, 'mcp') else self.mcp_server
                    
                    # 尝试找到get_nl2sql_status工具并调用
                    nl2sql_status_tool = None
                    for tool in await mcp.list_tools():
                        if getattr(tool, 'name', '') == 'mcp_doris_get_nl2sql_status':
                            nl2sql_status_tool = tool
                            break
                    
                    # 如果找到了工具，调用它获取状态
                    if nl2sql_status_tool:
                        logger.info("发送NL2SQL状态更新")
                        func = nl2sql_status_tool.func if hasattr(nl2sql_status_tool, 'func') else nl2sql_status_tool
                        nl2sql_status = await func()
                        
                        # 如果获取到状态，广播给所有客户端
                        if nl2sql_status:
                            # 尝试解析结果
                            if isinstance(nl2sql_status, str):
                                try:
                                    nl2sql_status = json.loads(nl2sql_status)
                                except:
                                    pass
                            
                            # 构造NL2SQL状态广播消息
                            nl2sql_status_data = {
                                "type": "nl2sql_status",
                                "status": nl2sql_status,
                                "timestamp": time.time()
                            }
                            
                            # 广播NL2SQL状态
                            await self.broadcast_tool_result('mcp_doris_get_nl2sql_status', nl2sql_status_data)
                    else:
                        logger.debug("未找到mcp_doris_get_nl2sql_status工具，跳过NL2SQL状态更新")
                except Exception as e:
                    logger.error(f"获取NL2SQL状态出错: {str(e)}")
                
                # 向所有客户端发送状态更新
                await self.broadcast_status_update(status_data)
            except Exception as e:
                logger.error(f"发送周期性更新时出错: {str(e)}")
                # 出错后稍微等待一下再继续
                await asyncio.sleep(1)
    
    async def broadcast_status_update(self, status_data):
        """向所有客户端广播状态更新
        
        Args:
            status_data: 状态数据
        """
        logger.debug(f"广播状态更新: {status_data}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/status",
            "params": {
                "type": "status_update",
                "data": status_data
            }
        }
        await self.broadcast_message(message)
    
    async def broadcast_visualization_data(self, visualization_data):
        """广播可视化数据到所有客户端
        
        Args:
            visualization_data: 可视化数据，应包含type字段
        """
        if not visualization_data or not isinstance(visualization_data, dict) or "type" not in visualization_data:
            logger.warning(f"无效的可视化数据: {visualization_data}")
            return
        
        logger.info(f"广播可视化数据: {visualization_data['type']}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/visualization",
            "params": {
                "type": "visualization",
                "data": visualization_data
            }
        }
        await self.broadcast_message(message)
    
    async def send_visualization_data(self, session_id, visualization_data):
        """向特定客户端发送可视化数据
        
        Args:
            session_id: 会话ID
            visualization_data: 可视化数据，应包含type字段
        """
        if not visualization_data or not isinstance(visualization_data, dict) or "type" not in visualization_data:
            logger.warning(f"无效的可视化数据: {visualization_data}")
            return
        
        if session_id not in self.client_sessions:
            logger.warning(f"会话不存在: {session_id}")
            return
        
        logger.info(f"向会话 {session_id} 发送可视化数据: {visualization_data['type']}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/visualization",
            "params": {
                "type": "visualization",
                "data": visualization_data
            }
        }
        await self.client_sessions[session_id]["queue"].put(message)
    
    async def send_tool_result(self, session_id, tool_name, result_data, is_final=True):
        """向客户端发送工具执行结果
        
        Args:
            session_id: 会话ID
            tool_name: 工具名称
            result_data: 结果数据
            is_final: 是否是最终结果
        """
        if session_id not in self.client_sessions:
            logger.warning(f"会话不存在: {session_id}")
            return
        
        logger.info(f"向会话 {session_id} 发送工具结果: {tool_name}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/tool_result",
            "params": {
                "type": "tool_result",
                "tool": tool_name,
                "result": result_data,
                "is_final": is_final
            }
        }
        await self.client_sessions[session_id]["queue"].put(message)
    
    async def broadcast_message(self, message):
        """向所有活动会话广播消息
        
        Args:
            message: 要广播的消息
        """
        # 如果没有客户端连接，直接返回
        if not self.client_sessions:
            return
        
        # 创建会话ID列表的副本，以便在迭代过程中可以安全地修改原始字典
        session_ids = list(self.client_sessions.keys())
        
        # 向所有会话发送消息
        for session_id in session_ids:
            try:
                if session_id in self.client_sessions:  # 再次检查，因为可能在迭代过程中有会话被移除
                    await self.client_sessions[session_id]["queue"].put(message)
            except Exception as e:
                logger.error(f"向会话 {session_id} 发送消息时出错: {str(e)}")
    
    async def get_status(self):
        """获取服务器状态"""
        return {
            "status": "running",
            "name": self.mcp_server.name,
            "mode": "mcp_sse",
            "clients": len(self.client_sessions)
        }

    async def mcp_message(self, request: Request):
        """接收客户端消息的端点"""
        # 从URL参数中获取会话ID
        session_id = request.query_params.get("session_id")
        if not session_id or session_id not in self.client_sessions:
            return JSONResponse(
                {"status": "error", "message": "无效的会话ID"}, 
                status_code=403,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*"
                }
            )
        
        # 更新会话活动时间
        self.client_sessions[session_id]["last_active"] = time.time()
        
        # 获取请求体
        try:
            body = await request.json()
            logger.info(f"接收到消息 [会话ID: {session_id}]: {json.dumps(body)}")
            
            # 处理消息
            message_id = body.get("id", str(uuid.uuid4()))
            
            # 特殊处理JSON-RPC格式的命令
            method = body.get("method")
            if method:
                # 处理特殊命令
                if method == "initialize":
                    # 初始化请求
                    logger.info(f"处理initialize命令 [会话ID: {session_id}]")
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "name": self.mcp_server.name,
                            "instructions": "这是一个用于Apache Doris数据库的MCP服务器，提供NL2SQL查询功能",
                            "serverInfo": {
                                "version": "1.0.0",
                                "name": "Doris MCP Server"
                            },
                            "capabilities": {
                                "tools": {
                                    "supportsStreaming": True,
                                    "supportsProgress": True
                                },
                                "resources": {
                                    "supportsStreaming": False
                                },
                                "prompts": {
                                    "supported": True
                                }
                            }
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                elif method == "notifications/initialized":
                    # 初始化完成通知
                    logger.info(f"处理initialized通知 [会话ID: {session_id}]")
                    response = {
                        "jsonrpc": "2.0",
                        "method": "notifications/initialized",
                        "params": {}
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                elif method == "mcp/listOfferings":
                    # 列出所有可用功能
                    logger.info(f"处理listOfferings命令 [会话ID: {session_id}]")
                    
                    # 获取工具列表
                    tools = await self.mcp_server.list_tools()
                    tools_json = [
                        {
                            "name": tool.name if hasattr(tool, "name") else str(tool),
                            "description": tool.description if hasattr(tool, "description") else "",
                            "inputSchema": tool.parameters if hasattr(tool, "parameters") else {
                                "type": "object",
                                "properties": {},
                                "required": []
                            }
                        }
                        for tool in tools
                    ]
                    
                    # 获取资源列表
                    resources = await self.mcp_server.list_resources()
                    resources_json = [res.model_dump() if hasattr(res, "model_dump") else res for res in resources]
                    
                    # 获取提示模板列表
                    prompts = await self.mcp_server.list_prompts()
                    prompts_json = [prompt.model_dump() if hasattr(prompt, "model_dump") else prompt for prompt in prompts]
                    
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "tools": tools_json,
                            "resources": resources_json,
                            "prompts": prompts_json
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                elif method == "mcp/listTools" or method == "tools/list":
                    # 列出所有工具
                    logger.info(f"处理listTools命令 [会话ID: {session_id}]")
                    tools = await self.mcp_server.list_tools()
                    tools_json = [
                        {
                            "name": tool.name if hasattr(tool, "name") else str(tool),
                            "description": tool.description if hasattr(tool, "description") else "",
                            "inputSchema": tool.parameters if hasattr(tool, "parameters") else {
                                "type": "object",
                                "properties": {},
                                "required": []
                            }
                        }
                        for tool in tools
                    ]
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "tools": tools_json
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                elif method == "mcp/listResources":
                    # 列出所有资源
                    logger.info(f"处理listResources命令 [会话ID: {session_id}]")
                    resources = await self.mcp_server.list_resources()
                    resources_json = [res.model_dump() if hasattr(res, "model_dump") else res for res in resources]
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "resources": resources_json
                        }
                    }
                    await self.client_sessions[session_id]["queue"].put(response)
                    return JSONResponse(
                        {"status": "success"},
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                elif method == "mcp/callTool":
                    # 调用工具 - 特殊处理
                    params = body.get("params", {})
                    tool_name = params.get("name")
                    arguments = params.get("arguments", {})
                    
                    if not tool_name:
                        error_response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "error": {
                                "code": -32602,
                                "message": "Invalid params: tool name is required"
                            }
                        }
                        await self.client_sessions[session_id]["queue"].put(error_response)
                        return JSONResponse(
                            {"status": "error", "message": "Tool name is required"},
                            status_code=400,
                            headers={
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Credentials": "true",
                                "Access-Control-Allow-Methods": "*",
                                "Access-Control-Allow-Headers": "*",
                                "Access-Control-Expose-Headers": "*"
                            }
                        )
                    
                    # 获取MCP实例
                    mcp = request.app.state.mcp
                    
                    # 检查是否为流式工具调用
                    stream_mode = "stream" in request.query_params or params.get("stream", False)
                    
                    logger.info(f"调用工具 [会话ID: {session_id}, 工具: {tool_name}, 参数: {arguments}, 流模式: {stream_mode}]")
                    
                    if stream_mode:
                        # 流式工具调用
                        logger.info(f"使用流式响应处理工具调用 [会话ID: {session_id}, 工具: {tool_name}]")
                        
                        try:
                            # 查找工具
                            tool_instance = None
                            for tool in await mcp.list_tools():
                                if getattr(tool, 'name', '') == tool_name:
                                    tool_instance = tool
                                    break
                            
                            if not tool_instance:
                                raise Exception(f"工具 {tool_name} 不存在")
                            
                            # 定义回调函数
                            async def callback(content, metadata):
                                # 发送部分结果
                                partial_message = {
                                    "jsonrpc": "2.0",
                                    "id": message_id,
                                    "partial": True,
                                    "result": {
                                        "content": content,
                                        "metadata": metadata
                                    }
                                }
                                # 将消息放入队列
                                await self.client_sessions[session_id]["queue"].put(partial_message)
                                
                                # 如果包含可视化数据，则广播到所有客户端
                                if metadata and "visualization" in metadata:
                                    await self.broadcast_visualization_data(metadata["visualization"])
                            
                            # 构建参数字典
                            kwargs = dict(arguments)
                            kwargs['callback'] = callback
                            
                            # 执行工具调用
                            func = tool_instance.func if hasattr(tool_instance, 'func') else tool_instance
                            
                            # 启动异步任务执行流式工具
                            asyncio.create_task(self._execute_stream_tool(func, kwargs, message_id, session_id))
                            
                            # 返回接收确认
                            return JSONResponse(
                                {"status": "processing"}, 
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
                        except Exception as e:
                            logger.error(f"流式工具处理错误: {str(e)}")
                            error_message = {
                                "jsonrpc": "2.0",
                                "id": message_id,
                                "success": False,
                                "error": str(e)
                            }
                            # 将错误消息放入队列
                            await self.client_sessions[session_id]["queue"].put(error_message)
                            return JSONResponse(
                                {"status": "error", "message": str(e)}, 
                                status_code=500,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
                    else:
                        # 非流式工具调用
                        logger.info(f"使用标准响应处理工具调用 [会话ID: {session_id}, 工具: {tool_name}]")
                        
                        try:
                            # 查找工具
                            tool_instance = None
                            for tool in await mcp.list_tools():
                                if getattr(tool, 'name', '') == tool_name:
                                    tool_instance = tool
                                    break
                            
                            if not tool_instance:
                                error_response = {
                                    "jsonrpc": "2.0",
                                    "id": message_id,
                                    "error": {
                                        "code": -32601,
                                        "message": f"Tool '{tool_name}' not found"
                                    }
                                }
                                await self.client_sessions[session_id]["queue"].put(error_response)
                                return JSONResponse(
                                    {"status": "error", "message": f"Tool '{tool_name}' not found"},
                                    status_code=404,
                                    headers={
                                        "Access-Control-Allow-Origin": "*",
                                        "Access-Control-Allow-Credentials": "true",
                                        "Access-Control-Allow-Methods": "*",
                                        "Access-Control-Allow-Headers": "*",
                                        "Access-Control-Expose-Headers": "*"
                                    }
                                )
                            
                            # 执行工具调用
                            func = tool_instance.func if hasattr(tool_instance, 'func') else tool_instance
                            result = await func(**arguments)
                            
                            # 特殊格式化结果
                            formatted_result = {
                                "content": [
                                    {
                                        "type": "text",
                                        "text": result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, indent=2)
                                    }
                                ]
                            }
                            
                            # 构建响应
                            response = {
                                "jsonrpc": "2.0",
                                "id": message_id,
                                "result": formatted_result
                            }
                            await self.client_sessions[session_id]["queue"].put(response)
                            return JSONResponse(
                                {"status": "success"},
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
                        except Exception as e:
                            logger.error(f"工具调用错误 [会话ID: {session_id}, 工具: {tool_name}]: {str(e)}")
                            error_response = {
                                "jsonrpc": "2.0",
                                "id": message_id,
                                "error": {
                                    "code": -32000,
                                    "message": str(e)
                                }
                            }
                            await self.client_sessions[session_id]["queue"].put(error_response)
                            return JSONResponse(
                                {"status": "error", "message": str(e)},
                                status_code=500,
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Access-Control-Allow-Credentials": "true",
                                    "Access-Control-Allow-Methods": "*",
                                    "Access-Control-Allow-Headers": "*",
                                    "Access-Control-Expose-Headers": "*"
                                }
                            )
            
            # 继续处理普通工具调用或者其他消息类型
            # 获取MCP实例
            mcp = request.app.state.mcp

            # 检查是否为流式工具调用
            stream_mode = "stream" in request.query_params or body.get("stream", False)

            # 获取工具名称
            tool_name = None
            if "tool" in body:
                tool_name = body.get("tool")
            elif "method" in body and not method:  # 如果上面已经处理过method，则跳过
                # 可能是直接使用方法名作为工具名
                tool_name = body.get("method")
            
            # 获取参数
            arguments = {}
            if "arguments" in body:
                arguments = body.get("arguments", {})
            elif "params" in body:
                params = body.get("params", {})
                if isinstance(params, dict) and "arguments" in params:
                    arguments = params.get("arguments", {})
                else:
                    arguments = params
            
            logger.info(f"工具调用 [会话ID: {session_id}, 工具: {tool_name}, 参数: {arguments}, 流模式: {stream_mode}]")
            
            if tool_name and stream_mode:
                # 流式工具调用
                logger.info(f"使用流式响应处理工具调用 [会话ID: {session_id}, 工具: {tool_name}]")
                
                try:
                    # 查找工具
                    tool_instance = None
                    for tool in await mcp.list_tools():
                        if getattr(tool, 'name', '') == tool_name:
                            tool_instance = tool
                            break
                    
                    if not tool_instance:
                        raise Exception(f"工具 {tool_name} 不存在")
                    
                    # 定义回调函数
                    async def callback(content, metadata):
                        # 发送部分结果
                        partial_message = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "partial": True,
                            "result": {
                                "content": content,
                                "metadata": metadata
                            }
                        }
                        # 将消息放入队列
                        await self.client_sessions[session_id]["queue"].put(partial_message)
                        
                        # 如果包含可视化数据，则广播到所有客户端
                        if metadata and "visualization" in metadata:
                            await self.broadcast_visualization_data(metadata["visualization"])
                    
                    # 构建参数字典
                    kwargs = dict(arguments)
                    kwargs['callback'] = callback
                    
                    # 执行工具调用
                    func = tool_instance.func if hasattr(tool_instance, 'func') else tool_instance
                    
                    # 启动异步任务执行流式工具
                    asyncio.create_task(self._execute_stream_tool(func, kwargs, message_id, session_id))
                    
                    # 返回接收确认
                    return JSONResponse(
                        {"status": "processing"}, 
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
                except Exception as e:
                    logger.error(f"流式工具处理错误: {str(e)}")
                    error_message = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "success": False,
                        "error": str(e)
                    }
                    # 将错误消息放入队列
                    await self.client_sessions[session_id]["queue"].put(error_message)
                    return JSONResponse(
                        {"status": "error", "message": str(e)}, 
                        status_code=500,
                        headers={
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Credentials": "true",
                            "Access-Control-Allow-Methods": "*",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Expose-Headers": "*"
                        }
                    )
            else:
                # 非流式工具调用或其他消息
                if tool_name:
                    # 非流式工具调用
                    logger.info(f"使用标准响应处理工具调用 [会话ID: {session_id}, 工具: {tool_name}]")
                    
                    try:
                        # 查找工具
                        tool_instance = None
                        for tool in await mcp.list_tools():
                            if getattr(tool, 'name', '') == tool_name:
                                tool_instance = tool
                                break
                        
                        if not tool_instance:
                            raise Exception(f"工具 {tool_name} 不存在")
                        
                        # 执行工具调用
                        func = tool_instance.func if hasattr(tool_instance, 'func') else tool_instance
                        result = await func(**arguments)
                        
                        # 构建响应
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "success": True,
                            "result": result
                        }
                    except Exception as e:
                        logger.error(f"工具调用错误 [会话ID: {session_id}, 工具: {tool_name}]: {str(e)}")
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "success": False,
                            "error": str(e)
                        }
                else:
                    # 其他类型的消息，直接转发给MCP处理
                    logger.info(f"处理一般消息 [会话ID: {session_id}]")
                    
                    try:
                        # 处理消息
                        result = await mcp.process_message(body)
                        
                        # 构建响应
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "success": True,
                            "result": result
                        }
                    except Exception as e:
                        logger.error(f"处理消息错误 [会话ID: {session_id}]: {str(e)}")
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "success": False,
                            "error": str(e)
                        }

                # 将响应放入队列
                if response:
                    await self.client_sessions[session_id]["queue"].put(response)

                # 给HTTP请求返回接收确认
                return JSONResponse(
                    {"status": "received"}, 
                    headers={
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Credentials": "true",
                        "Access-Control-Allow-Methods": "*",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Expose-Headers": "*"
                    }
                )
        except Exception as e:
            logger.error(f"处理消息错误: {str(e)}")
            # 发送错误响应
            error_response = {
                "jsonrpc": "2.0",
                "id": message_id if 'message_id' in locals() else str(uuid.uuid4()),
                "error": {
                    "code": -32000,
                    "message": str(e)
                }
            }
            
            await self.client_sessions[session_id]["queue"].put(error_response)
            return JSONResponse(
                {"status": "error", "message": str(e)}, 
                status_code=500,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*"
                }
            )

    async def _execute_stream_tool(self, func, kwargs, message_id, session_id):
        """执行流式工具并将结果放入会话消息队列
        
        Args:
            func: 要执行的函数
            kwargs: 函数参数
            message_id: 消息ID
            session_id: 会话ID
        """
        try:
            # 执行工具调用
            result = await func(**kwargs)
            
            # 发送完成消息
            final_message = {
                "jsonrpc": "2.0",
                "id": message_id,
                "success": True,
                "result": result
            }
            
            # 检查会话是否仍然存在
            if session_id in self.client_sessions:
                await self.client_sessions[session_id]["queue"].put(final_message)
                logger.info(f"流式工具执行完成 [会话ID: {session_id}, 消息ID: {message_id}]")
            else:
                logger.warning(f"流式工具执行完成但会话已关闭 [会话ID: {session_id}]")
        except Exception as e:
            logger.error(f"流式工具执行失败 [会话ID: {session_id}]: {str(e)}")
            
            # 发送错误消息
            error_message = {
                "jsonrpc": "2.0",
                "id": message_id,
                "success": False,
                "error": str(e)
            }
            
            # 检查会话是否仍然存在
            if session_id in self.client_sessions:
                await self.client_sessions[session_id]["queue"].put(error_message)
            else:
                logger.warning(f"流式工具执行失败但会话已关闭 [会话ID: {session_id}]") 

    async def broadcast_tool_result(self, tool_name, result_data):
        """广播工具调用结果到所有客户端
        
        Args:
            tool_name: 工具名称
            result_data: 结果数据
        """
        logger.info(f"广播工具结果: {tool_name}")
        message = {
            "jsonrpc": "2.0",
            "method": "notifications/tool_result",
            "params": {
                "type": "tool_result",
                "tool": tool_name,
                "result": result_data
            }
        }
        await self.broadcast_message(message)

