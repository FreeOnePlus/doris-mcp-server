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
import traceback
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from starlette.applications import Starlette

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
        
        @self.app.get("/health")
        async def health_check():
            """健康检查端点"""
            from src.nl2sql_service import NL2SQLService
            
            try:
                nl2sql = NL2SQLService()
                result = await nl2sql.mcp_doris_health()
                
                # 如果是MCP格式结果，解包返回原始结果
                if isinstance(result, dict) and "content" in result and len(result["content"]) > 0:
                    content = result["content"][0]
                    if content.get("type") == "text" and "text" in content:
                        try:
                            return json.loads(content["text"])
                        except:
                            # 如果不能解析为JSON，则直接返回文本
                            return {"status": "healthy", "text": content["text"]}
                
                # 如果不是MCP格式，直接返回
                return result
            except Exception as e:
                return {
                    "status": "error",
                    "error": str(e),
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }
        
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
        try:
            # 解析请求参数
            session_id = self._get_session_id(request)
            
            # 检查会话是否存在
            if not session_id or session_id not in self.client_sessions:
                logger.warning(f"会话不存在: {session_id}")
                return JSONResponse(
                    {"jsonrpc": "2.0", "error": {"code": -32000, "message": "会话不存在或已过期"}},
                    status_code=401
                )
            
            # 更新会话最后活动时间
            self.client_sessions[session_id]["last_active"] = time.time()
            
            # 获取请求体
            try:
                body = await request.json()
                logger.info(f"接收到消息 [会话ID: {session_id}]: {json.dumps(body)}")
                
                # 处理消息
                message_id = body.get("id", str(uuid.uuid4()))
                
                # 处理JSON-RPC 2.0格式的命令
                if "jsonrpc" not in body or body.get("jsonrpc") != "2.0" or "method" not in body:
                    return JSONResponse(
                        {"jsonrpc": "2.0", "id": message_id, "error": {"code": -32600, "message": "无效的请求，需要是JSON-RPC 2.0格式"}},
                        status_code=400
                    )
                
                # 获取方法和参数
                method = body.get("method")
                params = body.get("params", {})
                
                # 特殊处理JSON-RPC格式的命令
                if method == "initialize":
                    # 初始化请求
                    logger.info(f"处理initialize命令 [会话ID: {session_id}]")
                    response = {
                        "jsonrpc": "2.0",
                        "id": message_id,
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "name": self.mcp_server.name,
                            "instructions": "这是一个用于Apache Doris数据库的MCP服务器",
                            "serverInfo": {
                                "version": "0.1.0",
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
                    
                    # 构建响应
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
                
                elif method == "mcp/callTool" or method == "tools/call":
                    # 调用工具 - 特殊处理
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
                            # 修复: 不直接调用工具对象，而是创建异步任务调用call_tool方法
                            # func = tool_instance.func if hasattr(tool_instance, 'func') else tool_instance
                            
                            # 启动异步任务执行流式工具
                            # asyncio.create_task(self._execute_stream_tool(func, kwargs, message_id, session_id))
                            # 修改为使用call_tool方法
                            asyncio.create_task(self._execute_stream_tool_wrapper(tool_name, kwargs, message_id, session_id, request))
                            
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
                            # 修复: 不直接调用工具对象，而是调用自定义的call_tool方法
                            result = await self.call_tool(tool_name, arguments, request)
                            
                            # 特殊格式化结果
                            # 如果结果已经是符合格式的，就直接使用
                            if isinstance(result, dict) and "content" in result and isinstance(result["content"], list):
                                formatted_result = result
                            else:
                                # 否则，格式化为标准格式
                                formatted_result = {
                                    "content": [
                                        {
                                            "type": "json", 
                                            "text": result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
                                        }
                                    ]
                                }
                            
                            # 构建响应
                            response = {
                                "jsonrpc": "2.0",
                                "id": message_id,
                                "result": formatted_result
                            }
                            
                            # 将响应放入队列
                            await self.client_sessions[session_id]["queue"].put(response)
                            
                            # 返回接收确认
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
                            logger.error(f"工具调用错误: {str(e)}", exc_info=True)
                            
                            # 构建错误响应
                            if str(e).startswith('{"code":'):
                                # 如果是JSON格式的错误，直接使用
                                try:
                                    error_obj = json.loads(str(e))
                                    error_response = {
                                        "jsonrpc": "2.0",
                                        "id": message_id,
                                        "error": error_obj
                                    }
                                except:
                                    # 如果解析失败，使用标准格式
                                    error_response = {
                                        "jsonrpc": "2.0",
                                        "id": message_id,
                                        "error": {
                                            "code": -32000,
                                            "message": str(e)
                                        }
                                    }
                            else:
                                # 普通错误字符串
                                error_response = {
                                    "jsonrpc": "2.0",
                                    "id": message_id,
                                    "error": {
                                        "code": -32000,
                                        "message": str(e)
                                    }
                                }
                            
                            # 将错误响应放入队列
                            await self.client_sessions[session_id]["queue"].put(error_response)
                            
                            # 返回错误状态
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
                    # 其他类型的消息，直接转发给MCP处理
                    logger.info(f"处理一般消息 [会话ID: {session_id}]")
                    
                    try:
                        # 处理消息
                        # FastMCP对象没有process_message方法，改为直接构造响应
                        # result = await mcp.process_message(body)
                        
                        # 构建响应
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "result": {
                                "status": "ok",
                                "message": "消息已接收，但无法处理未识别的消息类型"
                            }
                        }
                    except Exception as e:
                        logger.error(f"处理消息错误 [会话ID: {session_id}]: {str(e)}")
                        response = {
                            "jsonrpc": "2.0",
                            "id": message_id,
                            "error": {
                                "code": -32000,
                                "message": f"消息处理错误: {str(e)}"
                            }
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
        except Exception as e:
            logger.error(f"处理请求出错: {str(e)}", exc_info=True)
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": "unknown",
                    "error": {
                        "code": -32000,
                        "message": f"处理请求出错: {str(e)}"
                    }
                },
                status_code=500,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Expose-Headers": "*"
                }
            )

    async def _execute_stream_tool_wrapper(self, tool_name, kwargs, message_id, session_id, request):
        """包装流式工具调用
        
        Args:
            tool_name: 工具名称
            kwargs: 函数参数
            message_id: 消息ID
            session_id: 会话ID
            request: 请求对象
        """
        try:
            # 通过调用标准工具方法执行
            result = await self.call_tool(tool_name, kwargs, request)
            
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

    async def call_tool(self, tool_name, arguments, request):
        """
        调用指定的工具并返回结果
        
        Args:
            tool_name: 工具名称
            arguments: 工具参数
            request: 原始请求对象
        
        Returns:
            工具调用结果
        """
        logger.info(f"调用工具: {tool_name}, 参数: {json.dumps(arguments, ensure_ascii=False)}")
        
        # 获取最近的查询内容，用于处理random_string参数
        recent_query = self._extract_recent_query(request)
        
        # 处理工具名称映射 - 添加对标准名称工具的支持
        tool_mapping = {
            # 标准名称到Doris MCP名称的映射
            "nl2sql_query": "mcp_doris_nl2sql_query",
            "nl2sql_query_stream": "mcp_doris_nl2sql_query_stream",
            "list_database_tables": "mcp_doris_list_database_tables",
            "explain_table": "mcp_doris_explain_table",
            "get_business_overview": "mcp_doris_get_business_overview",
            "get_nl2sql_status": "mcp_doris_get_nl2sql_status",
            "refresh_metadata": "mcp_doris_refresh_metadata",
            "sql_optimize": "mcp_doris_sql_optimize",
            "fix_sql": "mcp_doris_fix_sql",
            "health": "mcp_doris_health",
            "status": "mcp_doris_status",
            "count_chars": "mcp_doris_count_chars"
        }
        
        # 如果是标准名称，转换为MCP名称
        mapped_tool_name = tool_mapping.get(tool_name, tool_name)
        
        # 从mcp_doris_tools导入工具函数
        try:
            # 导入工具模块
            import src.tools.mcp_doris_tools as mcp_tools
            
            # 获取对应的工具函数
            tool_function = getattr(mcp_tools, mapped_tool_name, None)
            
            if not tool_function:
                # 如果在mcp_tools中不存在，尝试使用MCP工具
                mcp = self.app.state.mcp if hasattr(self.app.state, 'mcp') else self.mcp_server
                # 查找对应的工具
                for tool in await mcp.list_tools():
                    if getattr(tool, 'name', '') == mapped_tool_name:
                        tool_function = tool
                        break
                
                if not tool_function:
                    raise ValueError(f"未找到工具: {tool_name} / {mapped_tool_name}")
            
            # 处理常见输入参数转换
            processed_args = self._process_tool_arguments(mapped_tool_name, arguments, recent_query)
            
            # 调用工具函数
            result = await tool_function(**processed_args)
            
            # 返回工具执行结果
            return result
        except AttributeError as e:
            logger.error(f"工具函数不存在: {mapped_tool_name}, 错误: {str(e)}")
            raise ValueError(f"工具函数不存在: {mapped_tool_name}")
        except Exception as e:
            logger.error(f"调用工具时出错: {str(e)}", exc_info=True)
            raise ValueError(f"调用工具时出错: {str(e)}")
    
    def _process_tool_arguments(self, tool_name, arguments, recent_query):
        """
        处理工具参数，支持特殊处理逻辑
        
        Args:
            tool_name: 工具名称
            arguments: 原始参数
            recent_query: 最近的查询内容
            
        Returns:
            处理后的参数字典
        """
        # 复制参数，避免修改原始对象
        processed_args = dict(arguments)
        
        # 处理random_string参数
        if "random_string" in processed_args and tool_name.startswith("mcp_doris_"):
            random_string = processed_args.pop("random_string", "")
            
            # 根据工具类型特殊处理
            if tool_name in ["mcp_doris_nl2sql_query", "mcp_doris_nl2sql_query_stream"]:
                # 对于NL2SQL查询，将random_string作为查询内容
                if not processed_args.get("query"):
                    processed_args["query"] = random_string or recent_query or "显示所有表的数据量"
            
            elif tool_name == "mcp_doris_explain_table":
                # 对于表结构解释，将random_string作为表名
                if not processed_args.get("table_name"):
                    if random_string:
                        processed_args["table_name"] = random_string
                    elif recent_query:
                        # 简单的表名提取逻辑
                        import re
                        table_matches = re.findall(r'表\s*[\'"]?([a-zA-Z0-9_]+)[\'"]?', recent_query)
                        if table_matches:
                            processed_args["table_name"] = table_matches[0]
                        else:
                            processed_args["table_name"] = "lineitem"  # 默认表名
                    else:
                        processed_args["table_name"] = "lineitem"  # 默认表名
            
            elif tool_name in ["mcp_doris_sql_optimize", "mcp_doris_fix_sql"]:
                # 对于SQL优化和修复，将random_string作为SQL内容
                if not processed_args.get("sql"):
                    if random_string:
                        processed_args["sql"] = random_string
                    elif recent_query:
                        # 尝试从最近的查询中提取SQL
                        import re
                        sql_matches = re.findall(r'```sql\s*([\s\S]+?)\s*```', recent_query)
                        if sql_matches:
                            processed_args["sql"] = sql_matches[0].strip()
                        else:
                            processed_args["sql"] = recent_query
            
            elif tool_name == "mcp_doris_count_chars":
                # 对于字符串计数，将random_string作为输入字符串
                if not processed_args.get("input_string"):
                    processed_args["input_string"] = random_string or recent_query or ""
        
        return processed_args

    def _extract_recent_query(self, request: Request) -> Optional[str]:
        """
        从请求中提取最近的用户查询
        
        Args:
            request: 请求对象
            
        Returns:
            Optional[str]: 最近的用户查询，如果没有找到则返回None
        """
        try:
            # 尝试从请求体中提取消息历史
            body = None
            body_bytes = getattr(request, "_body", None)
            if body_bytes:
                try:
                    body = json.loads(body_bytes)
                except:
                    pass
            
            if not body:
                body = getattr(request, "_json", {})
            
            # 从消息历史中查找最近的用户消息
            messages = body.get("params", {}).get("messages", [])
            if messages:
                # 反向遍历消息，查找最近的用户消息
                for msg in reversed(messages):
                    if msg.get("role") == "user":
                        return msg.get("content", "")
            
            # 如果没有在消息历史中找到，尝试从原始消息中提取
            message = body.get("params", {}).get("message", {})
            if message and message.get("role") == "user":
                return message.get("content", "")
            
            return None
        except Exception as e:
            logger.error(f"提取最近查询时出错: {str(e)}")
            return None

    def format_tool_result(self, result):
        """
        格式化工具调用结果为统一格式
        
        Args:
            result: 工具调用原始结果
        
        Returns:
            格式化后的结果
        """
        try:
            # 如果结果已经是字典格式，直接返回
            if isinstance(result, dict):
                return result
            
            # 如果是字符串，尝试解析为JSON
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    # 纯文本结果
                    return {"content": result}
            
            # 如果是其他类型，转换为字符串
            return {"content": str(result)}
        
        except Exception as e:
            logger.error(f"格式化工具结果时出错: {str(e)}")
            return {"error": str(e)}

    def _get_session_id(self, request: Request) -> str:
        """
        从请求中获取会话ID
        
        尝试从以下位置获取会话ID（按优先级排序）：
        1. 查询参数session_id
        2. 请求体中的session_id字段
        3. 请求头中的X-Session-ID
        
        Args:
            request: 请求对象
            
        Returns:
            str: 会话ID，如果未找到则返回None
        """
        # 从查询参数中获取
        session_id = request.query_params.get("session_id")
        if session_id:
            return session_id
            
        # 尝试从请求体中获取
        try:
            body = getattr(request, "_json", None)
            if not body:
                body_bytes = getattr(request, "_body", None)
                if body_bytes:
                    try:
                        body = json.loads(body_bytes)
                    except:
                        pass
            
            if body and isinstance(body, dict) and "session_id" in body:
                return body["session_id"]
        except:
            pass
            
        # 从请求头中获取
        session_id = request.headers.get("X-Session-ID")
        if session_id:
            return session_id
            
        return None

