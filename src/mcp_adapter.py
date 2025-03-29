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

# 导入工具注册中心
from src.utils.tool_registry import registry, get_tool, get_tool_list

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
                        print("\n==================== MCP请求 ====================")
                        print(f"路径: {request.url.path}")
                        print(f"查询参数: {request.url.query}")
                        print(f"请求体: {json.dumps(data, ensure_ascii=False, indent=2)}")
                        print(f"流式响应: {use_stream}")
                        print("=================================================\n")
                    
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
                        
                        # 使用工具注册中心获取工具列表
                        tools_list = {
                            "tools": get_tool_list()
                        }
                        
                        # 构建旧格式响应
                        response_data = {
                            "id": data.get("id"),
                            "success": True,
                            "result": tools_list,
                            "session_id": data.get("session_id")
                        }
                        
                        if os.environ.get("MCP_DEBUG_ADAPTER", "").lower() == "true":
                            print("\n================ 生成的工具列表响应 ================")
                            print(json.dumps(response_data, ensure_ascii=False, indent=2))
                            print("=================================================\n")
                        
                        return JSONResponse(content=response_data)
                    
                    # 处理health工具调用
                    elif data.get("type") == "tool" and tool_name == "health":
                        logger.info("处理health特殊工具调用")
                        
                        # 构建健康检查响应
                        health_data = {
                            "status": "healthy",
                            "timestamp": str(datetime.datetime.now().isoformat()),
                            "version": "1.0.0"
                        }
                        
                        # 构建旧格式响应
                        response_data = {
                            "id": data.get("id"),
                            "success": True,
                            "result": health_data,
                            "session_id": data.get("session_id")
                        }
                        
                        if os.environ.get("MCP_DEBUG_ADAPTER", "").lower() == "true":
                            print("\n================ 手动生成的健康检查响应 ================")
                            print(json.dumps(response_data, ensure_ascii=False, indent=2))
                            print("=================================================\n")
                        
                        return JSONResponse(content=response_data)

                    # NL2SQL查询处理 - 特殊处理以支持流式响应
                    elif data.get("type") == "tool" and tool_name == "nl2sql_query" and use_stream:
                        logger.info(f"使用流式响应处理nl2sql_query调用")
                        
                        # 使用流式响应
                        return StreamingResponse(
                            self.stream_nl2sql_response(data),
                            media_type="text/event-stream"
                        )

                    # 通用工具调用处理（对于所有其他工具）
                    elif data.get("type") == "tool":
                        logger.info(f"通用工具调用: {tool_name}")
                        
                        # 检查是否使用流式响应
                        if use_stream and tool_name in ["nl2sql_query", "explain_table", "sql_optimize"]:
                            logger.info(f"使用流式响应处理{tool_name}调用")
                            
                            # 使用流式响应
                            return StreamingResponse(
                                self.stream_tool_response(data, tool_name),
                                media_type="text/event-stream"
                            )
                        
                        # 使用普通响应
                        try:
                            # 从工具注册中心获取工具函数
                            tool_func = get_tool(tool_name)
                            
                            if tool_func:
                                params = data.get("params", {})
                                
                                # 检查函数是否为异步函数
                                is_async = inspect.iscoroutinefunction(tool_func)
                                
                                # 调用工具函数
                                if is_async:
                                    logger.info(f"异步调用工具: {tool_name}")
                                    result = await tool_func(**params)
                                else:
                                    logger.info(f"同步调用工具: {tool_name}")
                                    result = tool_func(**params)
                                
                                # 构建成功响应
                                response_data = {
                                    "id": data.get("id"),
                                    "success": True,
                                    "result": result,
                                    "session_id": data.get("session_id")
                                }
                                
                                if os.environ.get("MCP_DEBUG_ADAPTER", "").lower() == "true":
                                    print(f"\n============== {tool_name}工具调用响应 ==============")
                                    print(json.dumps(response_data, ensure_ascii=False, indent=2))
                                    print("=================================================\n")
                                
                                return JSONResponse(content=response_data)
                            else:
                                logger.error(f"找不到工具: {tool_name}")
                                # 构建错误响应
                                error_response = {
                                    "id": data.get("id"),
                                    "success": False,
                                    "error": {"message": f"找不到工具: {tool_name}"},
                                    "session_id": data.get("session_id")
                                }
                                return JSONResponse(content=error_response)
                        except Exception as e:
                            logger.error(f"工具调用出错: {str(e)}", exc_info=True)
                            # 构建错误响应
                            error_response = {
                                "id": data.get("id"),
                                "success": False,
                                "error": {"message": f"调用{tool_name}工具出错: {str(e)}"},
                                "session_id": data.get("session_id")
                            }
                            return JSONResponse(content=error_response)
                    else:
                        # 不是工具调用，跳过处理
                        logger.info(f"非工具调用请求，类型: {data.get('type')}")
                        return await call_next(request)
                else:
                    # 不是旧格式的MCP请求，不处理
                    logger.debug("不是旧格式MCP请求，跳过处理")
            except Exception as e:
                logger.error(f"处理MCP请求时出错: {str(e)}", exc_info=True)
                
                # 如果出现异常，返回错误响应
                error_response = {
                    "id": data.get("id") if "data" in locals() and isinstance(data, dict) else "unknown",
                    "success": False,
                    "error": {"message": f"处理请求时出错: {str(e)}"}
                }
                return JSONResponse(content=error_response)
        
        # 对于所有其他请求，不做处理
        return await call_next(request)
    
    async def stream_nl2sql_response(self, data: Dict[str, Any]) -> AsyncGenerator[str, None]:
        """为NL2SQL查询生成流式响应"""
        request_id = data.get("id")
        session_id = data.get("session_id")
        query = data.get("params", {}).get("query", "")
        
        if not query:
            # 输出错误事件
            yield self.format_stream_event(ResponseType.ERROR, {
                "id": request_id,
                "success": False, 
                "error": {"message": "查询参数为空"},
                "session_id": session_id
            })
            return
        
        try:
            # 导入StreamNL2SQLProcessor而不是NL2SQLProcessor
            from src.nl2sql_stream_processor import StreamNL2SQLProcessor
            processor = StreamNL2SQLProcessor()
            
            # 定义回调函数处理流式思考内容
            async def stream_callback(content: str, metadata: Dict[str, Any]):
                """流式回调函数，处理处理器返回的思考过程和状态更新"""
                step = metadata.get("step", "thinking")
                progress = metadata.get("progress", 0)
                
                if step == "error":
                    # 错误处理
                    yield self.format_stream_event(ResponseType.ERROR, {
                        "id": request_id,
                        "success": False,
                        "error": {"message": content, "details": metadata.get("error", "未知错误")},
                        "session_id": session_id
                    })
                elif step in ["start", "thinking_start", "metadata", "similar_example", 
                            "business_metadata", "previous_error", "thinking"]:
                    # 思考过程
                    yield self.format_stream_event(ResponseType.THINKING, {
                        "id": request_id,
                        "success": True,
                        "result": {
                            "message": content,
                            "progress": progress,
                            "type": step,
                            "step": step
                        },
                        "session_id": session_id
                    })
                elif step in ["analyzing", "query_type", "cache_hit", "no_similar_example", "generating", "executing", "sql_generated"]:
                    # 进度更新
                    yield self.format_stream_event(ResponseType.PROGRESS, {
                        "id": request_id,
                        "success": True,
                        "result": {
                            "message": content,
                            "progress": progress,
                            "type": step,
                            "step": step
                        },
                        "session_id": session_id
                    })
                elif step in ["execute_sql", "sql_success", "business_analysis", "analysis_complete"]:
                    # 部分结果
                    result_data = {
                        "message": content,
                        "progress": progress,
                        "type": step,
                        "step": step
                    }
                    
                    # 如果有SQL，添加到结果中
                    if "sql" in metadata:
                        result_data["sql"] = metadata["sql"]
                    
                    yield self.format_stream_event(ResponseType.PARTIAL, {
                        "id": request_id,
                        "success": True,
                        "result": result_data,
                        "session_id": session_id
                    })
                elif step == "complete":
                    # 处理完成
                    yield self.format_stream_event(ResponseType.PROGRESS, {
                        "id": request_id,
                        "success": True,
                        "result": {
                            "message": content,
                            "progress": 100,
                            "type": "complete",
                            "step": "complete",
                            "execution_time": metadata.get("execution_time", 0)
                        },
                        "session_id": session_id
                    })
            
            # 使用异步生成器收集所有流式回调
            thinking_content = []
            async def collect_stream_events():
                async def callback_wrapper(content: str, metadata: Dict[str, Any]):
                    # 收集思考内容，用于最终结果
                    if metadata.get("step") == "thinking":
                        thinking_content.append(content)
                    
                    # 生成流式事件
                    async for event in stream_callback(content, metadata):
                        yield event
                
                # 处理查询
                result = await processor.process_stream(query, callback_wrapper)
                
                # 追加思考过程到结果
                if thinking_content:
                    result["thinking_process"] = "".join(thinking_content)
                
                # 发送最终结果
                yield self.format_stream_event(ResponseType.FINAL, {
                    "id": request_id,
                    "success": True,
                    "result": result,
                    "session_id": session_id
                })
            
            # 返回所有流式事件
            async for event in collect_stream_events():
                yield event
                
        except Exception as e:
            logger.error(f"流式NL2SQL响应处理出错: {str(e)}", exc_info=True)
            # 发送错误事件
            yield self.format_stream_event(ResponseType.ERROR, {
                "id": request_id,
                "success": False,
                "error": {"message": f"流式NL2SQL响应处理失败: {str(e)}"},
                "session_id": session_id
            })
    
    async def stream_tool_response(self, data: Dict[str, Any], tool_name: str) -> AsyncGenerator[str, None]:
        """通用工具流式响应生成器"""
        request_id = data.get("id")
        session_id = data.get("session_id")
        params = data.get("params", {})
        
        try:
            # 发送思考开始事件
            yield self.format_stream_event(ResponseType.THINKING, {
                "id": request_id,
                "success": True,
                "result": {
                    "message": f"正在执行{tool_name}工具调用...",
                    "type": "thinking_start",
                    "step": "thinking_start"
                },
                "session_id": session_id
            })
            
            # 获取工具函数
            try:
                # 从工具注册中心获取工具函数
                tool_func = get_tool(tool_name)
                
                if not tool_func:
                    # 工具未找到
                    yield self.format_stream_event(ResponseType.ERROR, {
                        "id": request_id,
                        "success": False,
                        "error": {"message": f"找不到工具: {tool_name}"},
                        "session_id": session_id
                    })
                    return
                
                # 发送进度更新
                yield self.format_stream_event(ResponseType.PROGRESS, {
                    "id": request_id,
                    "success": True,
                    "result": {
                        "message": f"工具{tool_name}正在执行...",
                        "progress": 30,
                        "type": "progress",
                        "step": "progress"
                    },
                    "session_id": session_id
                })
                
                # 检查函数是否为异步函数
                is_async = inspect.iscoroutinefunction(tool_func)
                
                # 调用工具函数
                logger.info(f"调用工具函数: {tool_name}")
                if is_async:
                    logger.info(f"异步调用工具: {tool_name}")
                    result = await tool_func(**params)
                else:
                    logger.info(f"同步调用工具: {tool_name}")
                    result = tool_func(**params)
                
                # 发送进度更新
                yield self.format_stream_event(ResponseType.PROGRESS, {
                    "id": request_id,
                    "success": True,
                    "result": {
                        "message": f"工具{tool_name}执行完成，正在整理结果...",
                        "progress": 80,
                        "type": "progress",
                        "step": "progress"
                    },
                    "session_id": session_id
                })
                
                # 发送最终结果
                yield self.format_stream_event(ResponseType.FINAL, {
                    "id": request_id,
                    "success": True,
                    "result": result,
                    "session_id": session_id
                })
                
            except Exception as e:
                logger.error(f"调用工具{tool_name}出错: {str(e)}", exc_info=True)
                # 发送错误事件
                yield self.format_stream_event(ResponseType.ERROR, {
                    "id": request_id,
                    "success": False,
                    "error": {"message": f"工具{tool_name}执行失败: {str(e)}"},
                    "session_id": session_id
                })
        except Exception as e:
            logger.error(f"流式响应处理出错: {str(e)}", exc_info=True)
            # 发送错误事件
            yield self.format_stream_event(ResponseType.ERROR, {
                "id": request_id,
                "success": False,
                "error": {"message": f"流式响应处理失败: {str(e)}"},
                "session_id": session_id
            })
    
    def format_stream_event(self, event_type: str, data: Dict[str, Any]) -> str:
        """格式化流式事件为SSE格式"""
        # 对final事件进行特殊处理，确保包含step和type字段
        if event_type == ResponseType.FINAL and "result" in data:
            # 确保result对象中包含处理阶段信息
            if isinstance(data["result"], dict):
                result_data = data["result"]
                if "step" not in result_data:
                    result_data["step"] = "complete"
                if "type" not in result_data:
                    result_data["type"] = "complete"
                if "progress" not in result_data:
                    result_data["progress"] = 100
        
        # 确保event_data中包含主要的步骤信息
        if "result" in data and isinstance(data["result"], dict):
            result = data["result"]
            # 将关键字段从result复制到data根级别
            if "step" in result:
                data["step"] = result["step"]
            if "type" in result:
                data["type"] = result["type"]
            if "progress" in result:
                data["progress"] = result["progress"]
        
        event_data = json.dumps({"type": event_type, "data": data}, ensure_ascii=False)
        
        # 打印事件数据，便于调试
        if os.environ.get("MCP_DEBUG_ADAPTER", "").lower() == "true":
            if "result" in data and isinstance(data["result"], dict):
                result = data["result"]
                logger.debug(f"发送事件: type={event_type}, step={result.get('step')}, type={result.get('type')}, "
                           f"progress={result.get('progress')}")
                
                # 检查步骤和类型是否一致
                if result.get("step") != result.get("type") and result.get("step") is not None and result.get("type") is not None:
                    logger.warning(f"事件数据中step({result.get('step')})和type({result.get('type')})不一致")
            else:
                logger.debug(f"发送事件: type={event_type}")
        
        return f"data: {event_data}\n\n"

# 初始化中间件
def init():
    # 配置日志
    logging_handler = logging.StreamHandler()
    logging_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(logging_handler)
    
    # 根据环境变量设置日志级别
    if os.environ.get("MCP_DEBUG_ADAPTER", "").lower() == "true":
        logger.setLevel(logging.DEBUG)
        print("\n=== MCP协议适配器已启用调试模式 ===\n")
    else:
        logger.setLevel(logging.INFO)
    
    logger.info("MCP协议适配器已初始化")

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