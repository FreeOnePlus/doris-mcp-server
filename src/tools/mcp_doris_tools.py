#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Doris MCP 工具实现

实现标准的Doris MCP工具，用于注册到MCP工具系统中。
所有工具遵循MCP标准接口。
"""

import os
import time
import json
import logging
from typing import Dict, Any, List, Optional, Callable
import asyncio

# 获取日志记录器
logger = logging.getLogger("doris-mcp-tools")

# 不再导入register_tool装饰器
# 导入NL2SQLService以实现核心功能
from src.nl2sql_service import NL2SQLService

# 注意：所有函数不再使用装饰器，直接作为普通异步函数定义
async def mcp_doris_nl2sql_query(query: str = None) -> Dict[str, Any]:
    """
    将自然语言查询转换为SQL并执行返回结果
    
    Args:
        query: 自然语言查询
        
    Returns:
        Dict[str, Any]: 查询结果
    """
    logger.info(f"MCP工具调用: mcp_doris_nl2sql_query, 查询: {query}")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 检查参数是否为None
        if query is None:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少查询参数",
                            "message": "请提供自然语言查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 处理查询
        result = nl2sql.process_query(query)
        
        # 构建标准格式的返回结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False, default=str)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_nl2sql_query: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "query": query
                    }, ensure_ascii=False)
                }
            ]
        }

# 流式查询暂且关闭
#  async def mcp_doris_nl2sql_query_stream(query: str, callback: Callable = None) -> Dict[str, Any]:
#     """
#     将自然语言查询转换为SQL并使用流式响应返回结果
    
#     Args:
#         query: 自然语言查询
#         callback: 回调函数，用于接收流式结果
        
#     Returns:
#         Dict[str, Any]: 查询结果
#     """
#     logger.info(f"MCP工具调用: mcp_doris_nl2sql_query_stream, 查询: {query}")
    
#     try:
#         # 创建NL2SQL服务实例
#         nl2sql = NL2SQLService()
        
#         # 导入流式处理器
#         from src.nl2sql_stream_processor import StreamNL2SQLProcessor
        
#         # 创建流式处理器
#         processor = StreamNL2SQLProcessor()
        
#         # 定义空的回调函数(不使用回调)
#         async def empty_callback(content, metadata):
#             if callback:
#                 await callback(content, metadata)
        
#         # 处理查询
#         logger.info(f"开始流式处理查询: {query}")
#         result = await processor.process_stream(query, empty_callback)
#         logger.info(f"流式处理查询完成: {query}")
        
#         # 构建标准格式的返回结果
#         return {
#             "content": [
#                 {
#                     "type": "text",
#                     "text": json.dumps(result, ensure_ascii=False, default=str)
#                 }
#             ]
#         }
#     except Exception as e:
#         logger.error(f"MCP工具执行失败 mcp_doris_nl2sql_query_stream: {str(e)}")
#         # 返回错误结果
#         return {
#             "content": [
#                 {
#                     "type": "text",
#                     "text": json.dumps({
#                         "error": str(e),
#                         "query": query
#                     }, ensure_ascii=False)
#                 }
#             ]
#         }

async def mcp_doris_list_database_tables() -> Dict[str, Any]:
    """
    列出数据库中的所有表
    
    Returns:
        Dict[str, Any]: 表列表
    """
    logger.info("MCP工具调用: mcp_doris_list_database_tables")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 获取表列表
        result = nl2sql.list_tables()
        
        # 构建标准格式的返回结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_list_database_tables: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e)
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_explain_table(table_name: str) -> Dict[str, Any]:
    """
    获取表结构的详细信息
    
    Args:
        table_name: 表名
        
    Returns:
        Dict[str, Any]: 表结构详细信息
    """
    logger.info(f"MCP工具调用: mcp_doris_explain_table, 表名: {table_name}")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 检查参数是否为None
        if not table_name:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少表名参数",
                            "message": "请提供表名"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 获取表结构
        result = nl2sql.explain_table(table_name)
        
        # 构建标准格式的返回结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_explain_table: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "table_name": table_name
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_get_business_overview() -> Dict[str, Any]:
    """
    获取数据库业务领域概览
    
    Returns:
        Dict[str, Any]: 业务领域概览
    """
    logger.info("MCP工具调用: mcp_doris_get_business_overview")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 获取业务领域概览
        result = nl2sql.get_business_overview()
        
        # 构建标准格式的返回结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_get_business_overview: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e)
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_get_nl2sql_status() -> Dict[str, Any]:
    """
    获取当前NL2SQL处理状态
    
    Returns:
        Dict[str, Any]: NL2SQL处理状态
    """
    logger.info("MCP工具调用: mcp_doris_get_nl2sql_status")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 获取当前服务器时间
        server_time = nl2sql.get_server_time()
        
        # 获取Doris版本
        try:
            doris_version = nl2sql.get_doris_version()
        except:
            doris_version = "未知"
        
        # 获取缓存的表数量
        cached_table_count = nl2sql.get_cached_table_count()
        cached_tables = nl2sql.get_cached_tables()
        
        # 获取数据库状态
        db_status = nl2sql.get_database_status()
        
        # 组装状态信息
        status_info = {
            "server_time": server_time,
            "doris_version": doris_version,
            "cache": {
                "table_count": cached_table_count,
                "tables": cached_tables,
            },
            "database": db_status,
            "llm": {
                "provider": os.getenv("LLM_PROVIDER", "unknown"),
                "model": os.getenv("LLM_MODEL", "unknown"),
                "available_providers": nl2sql.get_available_llm_providers()
            }
        }
        
        # 构建标准格式的返回结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(status_info, ensure_ascii=False, default=str)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_get_nl2sql_status: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e)
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_refresh_metadata() -> Dict[str, Any]:
    """
    刷新并保存元数据
    
    Returns:
        Dict[str, Any]: 刷新结果
    """
    logger.info("MCP工具调用: mcp_doris_refresh_metadata")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 记录开始时间
        start_time = time.time()
        
        # 刷新元数据
        force_refresh = True  # 强制刷新
        refresh_result = nl2sql._refresh_metadata(force=force_refresh)
        
        # 计算耗时
        elapsed_time = time.time() - start_time
        
        # 组装结果
        result = {
            "success": refresh_result,
            "message": "元数据刷新完成" if refresh_result else "元数据刷新失败",
            "elapsed_time": f"{elapsed_time:.2f}秒",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "tables": nl2sql.get_cached_tables()
        }
        
        # 构建标准格式的返回结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_refresh_metadata: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "刷新元数据时出错",
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_sql_optimize(sql: str = None) -> Dict[str, Any]:
    """
    对SQL语句进行优化分析，提供性能改进建议和业务含义解读
    
    Args:
        sql: 需要优化的SQL语句
        
    Returns:
        Dict[str, Any]: 优化结果
    """
    logger.info(f"MCP工具调用: mcp_doris_sql_optimize, SQL: {sql}")
    
    try:
        # 检查参数是否为None
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少SQL参数",
                            "message": "请提供需要优化的SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 导入SQL优化器
        from src.utils.sql_optimizer import SQLOptimizer
        optimizer = SQLOptimizer()
        
        # 从SQL中提取表信息
        table_info = optimizer.extract_table_info(sql)
        
        # 执行SQL优化
        result = optimizer.optimize(sql, table_info=table_info)
        
        # 构建标准格式的返回结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_sql_optimize: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "sql": sql
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_fix_sql(sql: str = None, error_message: str = "") -> Dict[str, Any]:
    """
    修复SQL语句中的错误
    
    Args:
        sql: 需要修复的SQL语句
        error_message: 错误信息(可选)
        
    Returns:
        Dict[str, Any]: 修复结果
    """
    try:
        from src.utils.sql_optimizer import SQLOptimizer
        
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "未提供SQL语句",
                            "message": "请提供需要修复的SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 创建SQL优化器
        optimizer = SQLOptimizer()
        
        # 优化SQL
        fix_result = optimizer.fix_sql(sql, error_message)
        
        # 包装结果
        result = {
            "success": True,
            "message": "SQL修复成功",
            "original_sql": sql,
            "fixed_sql": fix_result.get("corrected_sql", ""),
            "explanation": fix_result.get("explanation", ""),
            "error_message": error_message,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        error_msg = str(e)
        logger.error(f"修复SQL语句时出错: {error_msg}")
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": "修复SQL失败",
                        "message": error_msg,
                        "original_sql": sql,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_health() -> Dict[str, Any]:
    """
    健康检查工具
    
    Returns:
        Dict[str, Any]: 健康状态
    """
    logger.info("MCP工具调用: mcp_doris_health")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 获取Doris版本
        try:
            doris_version = nl2sql.get_doris_version()
        except:
            doris_version = "未知"
        
        # 获取服务器时间
        server_time = nl2sql.get_server_time()
        
        # 检查数据库连接
        db_status = nl2sql.get_database_status()
        
        # 构建结果
        result = {
            "status": "healthy",
            "database": {
                "status": db_status.get("connection_status", "unknown"),
                "version": doris_version,
                "server_time": server_time
            },
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"健康检查失败: {str(e)}")
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "status": "error",
                        "error": str(e),
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_status() -> Dict[str, Any]:
    """
    获取服务器状态
    
    Returns:
        Dict[str, Any]: 服务器状态
    """
    logger.info("MCP工具调用: mcp_doris_status")
    
    try:
        # 创建NL2SQL服务实例
        nl2sql = NL2SQLService()
        
        # 获取当前服务器时间
        server_time = nl2sql.get_server_time()
        
        # 获取Doris版本
        try:
            doris_version = nl2sql.get_doris_version()
        except:
            doris_version = "未知"
        
        # 获取基本状态信息
        status_data = {
            "status": "running",
            "service_name": "Doris MCP Server",
            "version": "0.1.0",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "server_time": server_time,
            "doris_version": doris_version
        }
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(status_data, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"获取服务器状态失败: {str(e)}")
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "status": "error",
                        "error": str(e),
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        } 