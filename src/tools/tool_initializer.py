#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
工具初始化模块

集中初始化所有工具，确保工具正确注册到MCP
"""

import logging
import os
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import traceback

# 导入Context
from mcp.server.fastmcp import Context

# 导入mcp_doris_tools定义的工具函数
import src.tools.mcp_doris_tools as tools

# 获取日志记录器
logger = logging.getLogger("doris-mcp.tool-init")

async def register_mcp_tools(mcp):
    """注册MCP工具函数
    
    Args:
        mcp: FastMCP实例
    """
    logger = logging.getLogger("tool_initializer")
    logger.info("开始注册MCP工具...")
    
    try:
        # 注册工具: 自然语言查询转SQL
        @mcp.tool("nl2sql_query", description="如果用户输入的是自然语言的业务需求，且非SQL语句及其他工具能力场景，则将自然语言查询转换为SQL并执行返回结果")
        async def nl2sql_query(ctx: Context) -> Dict[str, Any]:
            """将自然语言查询转换为SQL并执行返回结果"""
            query = ctx.params.get("query", "")
            return await tools.mcp_doris_nl2sql_query(query)
        
        # 注册工具: 自然语言查询转SQL (流式)
        # 流式查询暂且关闭
        # @mcp.tool("nl2sql_query_stream", description="将自然语言查询转换为SQL并使用流式响应返回结果")
        # async def nl2sql_query_stream(ctx: Context) -> Dict[str, Any]:
        #     """将自然语言查询转换为SQL并使用流式响应返回结果"""
        #     query = ctx.params.get("query", "")
        #     callback = ctx.params.get("callback")
        #     return await tools.mcp_doris_nl2sql_query_stream(query, callback)
        
        # 注册工具: 列出数据库表
        @mcp.tool("list_database_tables", description="列出数据库中的所有表")
        async def list_database_tables(ctx: Context) -> Dict[str, Any]:
            """列出数据库中的所有表"""
            return await tools.mcp_doris_list_database_tables()
        
        # 注册工具: 获取表结构详情
        @mcp.tool("explain_table", description="获取表结构的详细信息")
        async def explain_table(ctx: Context) -> Dict[str, Any]:
            """获取表结构的详细信息"""
            table_name = ctx.params.get("table_name", "")
            return await tools.mcp_doris_explain_table(table_name)
        
        # 注册工具: 获取业务概览
        @mcp.tool("get_business_overview", description="获取数据库业务领域概览")
        async def get_business_overview(ctx: Context) -> Dict[str, Any]:
            """获取数据库业务领域概览"""
            return await tools.mcp_doris_get_business_overview()
        
        # 注册工具: 获取NL2SQL状态
        @mcp.tool("get_nl2sql_status", description="获取当前NL2SQL处理状态")
        async def get_nl2sql_status(ctx: Context) -> Dict[str, Any]:
            """获取当前NL2SQL处理状态"""
            return await tools.mcp_doris_get_nl2sql_status()
        
        # 注册工具: 刷新元数据
        @mcp.tool("refresh_metadata", description="刷新并保存元数据")
        async def refresh_metadata(ctx: Context) -> Dict[str, Any]:
            """刷新并保存元数据"""
            return await tools.mcp_doris_refresh_metadata()
        
        # 注册工具: SQL优化
        @mcp.tool("sql_optimize", description="如果用户输入的是SQL语句，且希望对SQL语句进行优化分析，则执行该工具对SQL语句进行优化分析，并提供性能改进建议和业务含义解读")
        async def sql_optimize(ctx: Context) -> Dict[str, Any]:
            """对SQL语句进行优化分析，提供性能改进建议和业务含义解读"""
            sql = ctx.params.get("sql", "")
            return await tools.mcp_doris_sql_optimize(sql)
        
        # 注册工具: 修复SQL
        @mcp.tool("fix_sql", description="如果用户输入的是SQL语句，且希望对SQL语句进行修复，则执行该工具对SQL语句进行修复")
        async def fix_sql(ctx: Context) -> Dict[str, Any]:
            """修复SQL语句中的错误"""
            sql = ctx.params.get("sql", "")
            error_message = ctx.params.get("error_message", "")
            return await tools.mcp_doris_fix_sql(sql, error_message)
        
        # 注册工具: 健康检查
        @mcp.tool("health", description="健康检查工具")
        async def health(ctx: Context) -> Dict[str, Any]:
            """健康检查工具"""
            return await tools.mcp_doris_health()
        
        # 注册工具: 获取服务器状态
        @mcp.tool("status", description="获取服务器状态")
        async def status(ctx: Context) -> Dict[str, Any]:
            """获取服务器状态"""
            return await tools.mcp_doris_status()
        
        # 注册工具: 执行SQL查询
        @mcp.tool("exec_query", description="如果用户输入的是SQL语句，且没有其他诉求，则执行该工具执行SQL查询并返回结果")
        async def exec_query(ctx: Context) -> Dict[str, Any]:
            """执行SQL查询并返回结果"""
            sql = ctx.params.get("sql", "")
            return await tools.mcp_doris_exec_query(sql)
        
        # 获取工具数量
        tools_count = len(await mcp.list_tools())
        logger.info(f"已注册所有MCP工具，总计 {tools_count} 个工具")
        return True
    except Exception as e:
        logger.error(f"注册MCP工具时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def init_tools() -> None:
    """
    初始化所有工具
    
    这个函数主要用于非FastMCP场景下的工具初始化，
    但在当前架构下不再需要，因为所有工具都通过FastMCP注册
    """
    logger.info("工具初始化函数被调用，但在当前架构下不再需要执行额外操作")
    logger.info("所有工具都通过FastMCP的@mcp.tool装饰器注册")
    return