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

# 导入doris mcp工具
from src.tools.mcp_doris_tools import (
    mcp_doris_get_schema_list,
    mcp_doris_refresh_metadata,
    mcp_doris_sql_optimize,
    mcp_doris_fix_sql,
    mcp_doris_health,
    mcp_doris_status,
    mcp_doris_exec_query,
    mcp_doris_generate_sql,
    mcp_doris_explain_sql,
    mcp_doris_modify_sql,
    mcp_doris_parse_query,
    mcp_doris_identify_query_type,
    mcp_doris_validate_sql_syntax,
    mcp_doris_check_sql_security,
    mcp_doris_analyze_query_result,
    mcp_doris_find_similar_examples,
    mcp_doris_find_similar_history,
    mcp_doris_calculate_query_similarity,
    mcp_doris_adapt_similar_query,
    mcp_doris_get_metadata,
    mcp_doris_save_metadata
)

# 获取日志记录器
logger = logging.getLogger("doris-mcp.tool-init")

async def register_mcp_tools(mcp):
    """注册MCP工具函数
    
    Args:
        mcp: FastMCP实例
    """
    logger.info("开始注册MCP工具...")
    
    try:
        # 注册工具: 刷新元数据
        @mcp.tool("refresh_metadata", description="[功能描述]：刷新并保存元数据。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- db_name (string) [选填] - 目标数据库名称，默认刷新所有数据库\n- force_refresh (boolean) [选填] - 是否强制全量刷新，默认为false")
        async def refresh_metadata(ctx: Context) -> Dict[str, Any]:
            """刷新并保存元数据"""
            db_name = ctx.params.get("db_name", "")
            force_refresh = ctx.params.get("force_refresh", False)
            return await mcp_doris_refresh_metadata(db_name, force_refresh)
        
        # 注册工具: SQL优化
        @mcp.tool("sql_optimize", description="[功能描述]：对SQL语句进行优化分析。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- sql (string) [必填] - 需要优化的SQL语句\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- optimization_level (string) [选填] - 优化级别，可选值：'basic'、'performance'、'full'，默认为'full'")
        async def sql_optimize(ctx: Context) -> Dict[str, Any]:
            """对SQL语句进行优化分析，提供性能改进建议和执行计划解读"""
            sql = ctx.params.get("sql", "")
            optimization_level = ctx.params.get("optimization_level", "normal")
            return await mcp_doris_sql_optimize(sql, optimization_level)
        
        # 注册工具: 修复SQL
        @mcp.tool("fix_sql", description="[功能描述]：修复SQL语句中的语法错误。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- sql (string) [必填] - 需要修复的SQL语句\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def fix_sql(ctx: Context) -> Dict[str, Any]:
            """修复SQL语句中的错误"""
            sql = ctx.params.get("sql", "")
            error_message = ctx.params.get("error_message", "")
            return await mcp_doris_fix_sql(sql, error_message)
        
        # 注册工具: 健康检查
        @mcp.tool("health", description="[功能描述]：检查服务和数据库的健康状态。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- check_type (string) [选填] - 检查类型，可选值：'basic'、'full'，默认为'basic'")
        async def health(ctx: Context) -> Dict[str, Any]:
            """健康检查工具"""
            return await mcp_doris_health()
        
        # 注册工具: 获取服务器状态
        @mcp.tool("status", description="[功能描述]：获取服务器状态信息。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- include_metrics (boolean) [选填] - 是否包含详细指标，默认为false")
        async def status(ctx: Context) -> Dict[str, Any]:
            """获取服务器状态"""
            return await mcp_doris_status()
        
        # 注册工具: 执行SQL查询
        @mcp.tool("exec_query", description="[功能描述]：执行SQL查询并返回结果。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- sql (string) [必填] - 要执行的SQL语句\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- max_rows (integer) [选填] - 最大返回行数，默认100\n- timeout (integer) [选填] - 查询超时时间（秒），默认30")
        async def exec_query(ctx: Context) -> Dict[str, Any]:
            """执行SQL查询并返回结果"""
            sql = ctx.params.get("sql", "")
            return await mcp_doris_exec_query(sql)
        
        # 注册工具: 根据自然语言生成SQL
        @mcp.tool("generate_sql", description="[功能描述]：根据自然语言生成SQL但不执行，如果需要指定表名，请在tables参数中传入相关表名列表。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- query (string) [必填] - 自然语言查询描述\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- tables (array) [选填] - 相关表名列表")
        async def generate_sql(ctx: Context) -> Dict[str, Any]:
            """根据自然语言生成SQL但不执行"""
            query = ctx.params.get("query", "")
            db_name = ctx.params.get("db_name", "")
            tables = ctx.params.get("tables", None)
            return await mcp_doris_generate_sql(query, db_name, tables)
        
        # 注册工具: 解释SQL
        @mcp.tool("explain_sql", description="[功能描述]：详细解释SQL语句的功能和组成部分。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- sql (string) [必填] - 需要解释的SQL语句\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- explanation_level (string) [选填] - 解释详细程度，可选值：'basic'、'detailed'，默认为'detailed'")
        async def explain_sql(ctx: Context) -> Dict[str, Any]:
            """详细解释SQL语句的功能和组成部分"""
            sql = ctx.params.get("sql", "")
            db_name = ctx.params.get("db_name", "")
            explanation_level = ctx.params.get("explanation_level", "detailed")
            return await mcp_doris_explain_sql(sql, db_name, explanation_level)
        
        # 注册工具: 修改SQL
        @mcp.tool("modify_sql", description="[功能描述]：根据自然语言描述修改SQL。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- sql (string) [必填] - 原始SQL语句\n- modification (string) [必填] - 修改需求描述\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def modify_sql(ctx: Context) -> Dict[str, Any]:
            """根据自然语言描述修改SQL"""
            sql = ctx.params.get("sql", "")
            modification = ctx.params.get("modification", "")
            db_name = ctx.params.get("db_name", "")
            return await mcp_doris_modify_sql(sql, modification, db_name)
        
        # 注册工具: 解析查询
        @mcp.tool("parse_query", description="[功能描述]：解析自然语言查询，提取查询意图、实体和条件。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- query (string) [必填] - 自然语言查询\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def parse_query(ctx: Context) -> Dict[str, Any]:
            """解析自然语言查询，提取查询意图、实体和条件"""
            query = ctx.params.get("query", "")
            db_name = ctx.params.get("db_name", "")
            return await mcp_doris_parse_query(query, db_name)
        
        # 注册工具: 识别查询类型
        @mcp.tool("identify_query_type", description="[功能描述]：识别查询类型，判断查询属于什么类别。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- query (string) [必填] - 自然语言查询\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def identify_query_type(ctx: Context) -> Dict[str, Any]:
            """识别查询类型，判断查询属于什么类别"""
            query = ctx.params.get("query", "")
            return await mcp_doris_identify_query_type(query)
        
        # 注册工具: 验证SQL语法
        @mcp.tool("validate_sql_syntax", description="[功能描述]：验证SQL语法是否正确。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- sql (string) [必填] - 需要验证的SQL语句\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def validate_sql_syntax(ctx: Context) -> Dict[str, Any]:
            """验证SQL语法是否正确"""
            sql = ctx.params.get("sql", "")
            db_name = ctx.params.get("db_name", "")
            return await mcp_doris_validate_sql_syntax(sql, db_name)
        
        # 注册工具: 检查SQL安全性
        @mcp.tool("check_sql_security", description="[功能描述]：检查SQL语句的安全性。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- sql (string) [必填] - 需要检查的SQL语句\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def check_sql_security(ctx: Context) -> Dict[str, Any]:
            """检查SQL语句的安全性"""
            sql = ctx.params.get("sql", "")
            return await mcp_doris_check_sql_security(sql)
        
        # 注册工具: 分析查询结果
        @mcp.tool("analyze_query_result", description="[功能描述]：分析查询结果，提供业务洞察。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- query_result (string) [必填] - 查询结果JSON字符串，必须是字符串类型的JSON，不能是直接的JSON对象\n- analysis_type (string) [选填] - 分析类型，可选值：'summary'、'trend'、'correlation'，默认为'summary'")
        async def analyze_query_result(ctx: Context) -> Dict[str, Any]:
            """分析查询结果，提供业务洞察"""
            query_result = ctx.params.get("query_result", "")
            analysis_type = ctx.params.get("analysis_type", "summary")
            return await mcp_doris_analyze_query_result(query_result, analysis_type)
        
        # 注册工具: 查找相似示例
        @mcp.tool("find_similar_examples", description="[功能描述]：查找与当前查询相似的示例。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- query (string) [必填] - 自然语言查询\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- top_k (integer) [选填] - 返回的最大相似示例数量，默认为3")
        async def find_similar_examples(ctx: Context) -> Dict[str, Any]:
            """查找与当前查询相似的示例"""
            query = ctx.params.get("query", "")
            db_name = ctx.params.get("db_name", "")
            top_k = ctx.params.get("top_k", 3)
            return await mcp_doris_find_similar_examples(query, db_name, top_k)
        
        # 注册工具: 查找相似历史记录
        @mcp.tool("find_similar_history", description="[功能描述]：查找与当前查询相似的历史记录。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- query (string) [必填] - 自然语言查询\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- top_k (integer) [选填] - 返回的最大相似历史记录数量，默认为3")
        async def find_similar_history(ctx: Context) -> Dict[str, Any]:
            """查找与当前查询相似的历史记录"""
            query = ctx.params.get("query", "")
            top_k = ctx.params.get("top_k", 3)
            return await mcp_doris_find_similar_history(query, top_k)
        
        # 注册工具: 计算查询相似度
        @mcp.tool("calculate_query_similarity", description="[功能描述]：计算两个查询之间的相似度。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- query1 (string) [必填] - 第一个查询\n- query2 (string) [必填] - 第二个查询")
        async def calculate_query_similarity(ctx: Context) -> Dict[str, Any]:
            """计算两个查询之间的相似度"""
            query1 = ctx.params.get("query1", "")
            query2 = ctx.params.get("query2", "")
            return await mcp_doris_calculate_query_similarity(query1, query2)
        
        # 注册工具: 调整相似查询
        @mcp.tool("adapt_similar_query", description="[功能描述]：根据当前需求调整相似查询的SQL。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- current_query (string) [必填] - 当前查询\n- similar_query (string) [必填] - 相似查询的SQL\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def adapt_similar_query(ctx: Context) -> Dict[str, Any]:
            """根据当前需求调整相似查询的SQL"""
            current_query = ctx.params.get("current_query", "")
            similar_query = ctx.params.get("similar_query", "")
            db_name = ctx.params.get("db_name", "")
            return await mcp_doris_adapt_similar_query(current_query, similar_query, db_name)
        
        # 注册工具: 获取元数据
        @mcp.tool("get_metadata", description="[功能描述]：获取元数据信息，可以是数据库元数据或表元数据。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- table_name (string) [选填] - 表名，如果提供则获取表元数据，否则获取数据库元数据\n- business_overview_only (boolean) [选填] - 是否只返回业务概览信息，默认为false")
        async def get_metadata(ctx: Context) -> Dict[str, Any]:
            """获取元数据信息，可以是数据库元数据或表元数据"""
            db_name = ctx.params.get("db_name", "")
            table_name = ctx.params.get("table_name", "")
            business_overview_only = ctx.params.get("business_overview_only", False)
            return await mcp_doris_get_metadata(db_name, table_name, business_overview_only)
        
        # 注册工具: 保存元数据
        @mcp.tool("save_metadata", description="[功能描述]：保存元数据到数据库。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- metadata (string) [必填] - 元数据JSON字符串\n- metadata_type (string) [选填] - 元数据类型，默认根据table_name自动确定\n- table_name (string) [选填] - 表名，默认为空表示数据库级元数据\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库")
        async def save_metadata(ctx: Context) -> Dict[str, Any]:
            """保存元数据到数据库"""
            metadata = ctx.params.get("metadata", "")
            metadata_type = ctx.params.get("metadata_type")
            table_name = ctx.params.get("table_name", "")
            db_name = ctx.params.get("db_name", "")
            return await mcp_doris_save_metadata(metadata, metadata_type, table_name, db_name)
        
        # 注册工具: 获取结构信息
        @mcp.tool("get_schema_list", description="[功能描述]：获取数据库或表结构信息，默认仅返回表结构信息，simple_mode 传入参数为 false 时，返回完整库表结构信息和元数据信息。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符\n- table_name (string) [选填] - 表名，如果不提供则获取整个数据库表列表\n- db_name (string) [选填] - 目标数据库名称，默认使用当前数据库\n- simple_mode (boolean) [选填] - 是否使用简化模式（只返回基本信息，不包含提示信息），默认为true")
        async def get_schema_list(ctx: Context) -> Dict[str, Any]:
            """获取数据库或表结构信息"""
            table_name = ctx.params.get("table_name", "")
            db_name = ctx.params.get("db_name", "")
            simple_mode = ctx.params.get("simple_mode", True)
            return await mcp_doris_get_schema_list(table_name, db_name, simple_mode)
        
        # 注册工具: 获取NL2SQL工具流程指南（该工具为测试工具，逻辑应由 Agent 管理，故暂不开放）
        # @mcp.tool("get_nl2sql_prompt", description="[功能描述]：用户有传入自然语言希望进行数据库查询的意图，**必须**先行调用本工具指导如何按照正确流程执行。\n[参数内容]：\n- random_string (string) [必填] - 调用工具的唯一标识符")
        # async def get_nl2sql_prompt(ctx: Context) -> Dict[str, Any]:
        #     """获取NL2SQL工具使用指南的提示词"""
        #     return await mcp_doris_get_nl2sql_prompt()
        
        # 获取工具数量
        tools_count = len(await mcp.list_tools())
        logger.info(f"已注册所有MCP工具，总计 {tools_count} 个工具")
        return True
    except Exception as e:
        logger.error(f"注册MCP工具时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False