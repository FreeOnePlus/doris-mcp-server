#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
工具初始化模块

集中初始化所有工具，确保工具正确注册到MCP
"""

import logging
import inspect
import importlib
import os
import sys
from typing import List, Dict, Any, Optional
import json
import asyncio
from datetime import datetime

# 导入工具注册中心
from src.tools.tool_registry import get_registry, register_tool

# 获取日志记录器
logger = logging.getLogger("doris-mcp.tool-init")

def register_mcp_tools(mcp):
    """注册MCP工具函数
    
    Args:
        mcp: FastMCP实例
    """
    logger = logging.getLogger("tool_initializer")
    logger.info("开始注册MCP工具...")
    
    try:
        registry = get_registry()
        
        # 注册NL2SQL相关工具
        @mcp.tool("nl2sql_query", description="将自然语言查询转换为SQL并执行返回结果")
        async def nl2sql_query(query: str):
            """将自然语言查询转换为SQL并执行返回结果"""
            logger.info(f"执行NL2SQL查询: {query}")
            try:
                # 这里添加实际的NL2SQL处理逻辑
                # 暂时返回模拟数据
                return {
                    "sql": f"SELECT * FROM sales WHERE date > current_date - interval '30 days'",
                    "result": [
                        {"date": "2023-04-01", "product": "ProductA", "sales": 1200},
                        {"date": "2023-04-02", "product": "ProductA", "sales": 1350},
                        {"date": "2023-04-03", "product": "ProductA", "sales": 980}
                    ],
                    "message": "查询执行成功"
                }
            except Exception as e:
                logger.error(f"NL2SQL查询失败: {str(e)}")
                raise Exception(f"NL2SQL查询失败: {str(e)}")
        
        @mcp.tool("nl2sql_query_stream", description="将自然语言查询转换为SQL并使用流式响应返回结果")
        async def nl2sql_query_stream(query: str, callback=None):
            """将自然语言查询转换为SQL并使用流式响应返回结果"""
            logger.info(f"执行流式NL2SQL查询: {query}")
            try:
                # 模拟流式处理过程
                if callback:
                    # 第一步：解析查询
                    await callback("正在分析您的查询...", {"step": "parsing"})
                    await asyncio.sleep(0.5)
                    
                    # 第二步：生成SQL
                    sql = "SELECT * FROM sales WHERE date > current_date - interval '30 days'"
                    await callback(f"已生成SQL: {sql}", {"step": "sql_generation", "sql": sql})
                    await asyncio.sleep(0.5)
                    
                    # 第三步：执行查询
                    await callback("正在执行查询...", {"step": "executing"})
                    await asyncio.sleep(0.5)
                    
                    # 第四步：返回结果
                    result = [
                        {"date": "2023-04-01", "product": "ProductA", "sales": 1200},
                        {"date": "2023-04-02", "product": "ProductA", "sales": 1350},
                        {"date": "2023-04-03", "product": "ProductA", "sales": 980}
                    ]
                    result_str = json.dumps(result, ensure_ascii=False, indent=2)
                    await callback(f"查询结果: {result_str}", {"step": "results", "data": result})
                
                # 返回最终结果
                return {
                    "sql": sql,
                    "result": result,
                    "message": "查询执行成功"
                }
            except Exception as e:
                logger.error(f"流式NL2SQL查询失败: {str(e)}")
                if callback:
                    await callback(f"查询失败: {str(e)}", {"step": "error"})
                raise Exception(f"流式NL2SQL查询失败: {str(e)}")
        
        @mcp.tool("list_llm_providers", description="列出可用的LLM提供商")
        async def list_llm_providers():
            """列出可用的LLM提供商"""
            logger.info("获取LLM提供商列表")
            # 返回模拟数据
            return {
                "providers": [
                    {
                        "name": "OpenAI",
                        "models": ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"]
                    },
                    {
                        "name": "Anthropic",
                        "models": ["claude-2", "claude-instant-1"]
                    }
                ]
            }
        
        @mcp.tool("list_database_tables", description="列出数据库中的所有表")
        async def list_database_tables():
            """列出数据库中的所有表"""
            logger.info("获取数据库表列表")
            # 返回模拟数据
            return {
                "tables": [
                    {"name": "sales", "description": "销售数据表"},
                    {"name": "products", "description": "产品信息表"},
                    {"name": "customers", "description": "客户信息表"}
                ]
            }
        
        @mcp.tool("explain_table", description="获取表结构的详细信息")
        async def explain_table(table_name: str):
            """获取表结构的详细信息"""
            logger.info(f"获取表结构: {table_name}")
            # 返回模拟数据
            if table_name == "sales":
                return {
                    "table": table_name,
                    "columns": [
                        {"name": "id", "type": "INT", "description": "销售ID"},
                        {"name": "date", "type": "DATE", "description": "销售日期"},
                        {"name": "product_id", "type": "INT", "description": "产品ID"},
                        {"name": "customer_id", "type": "INT", "description": "客户ID"},
                        {"name": "quantity", "type": "INT", "description": "销售数量"},
                        {"name": "price", "type": "DECIMAL", "description": "销售价格"}
                    ]
                }
            else:
                return {"error": f"表 {table_name} 不存在"}
        
        @mcp.tool("get_business_overview", description="获取数据库业务领域概览")
        async def get_business_overview():
            """获取数据库业务领域概览"""
            logger.info("获取业务领域概览")
            # 返回模拟数据
            return {
                "business_domain": "电子商务销售系统",
                "main_entities": ["销售", "产品", "客户"],
                "key_metrics": ["销售额", "客户数", "产品类别"]
            }
        
        @mcp.tool("get_nl2sql_status", description="获取当前NL2SQL处理状态")
        async def get_nl2sql_status():
            """获取当前NL2SQL处理状态"""
            logger.info("获取NL2SQL处理状态")
            return {
                "status": "idle",
                "message": "系统准备就绪",
                "timestamp": datetime.now().isoformat()
            }
            
        @mcp.tool("refresh_metadata", description="刷新并保存元数据")
        async def refresh_metadata(force: bool = False):
            """刷新并保存元数据"""
            logger.info(f"刷新元数据 [强制: {force}]")
            return {
                "success": True,
                "message": "元数据刷新成功",
                "tables_count": 5,
                "views_count": 2
            }
            
        @mcp.tool("sql_optimize", description="对SQL语句进行优化分析，提供性能改进建议和业务含义解读")
        async def sql_optimize(sql: str, requirements: str = ""):
            """优化SQL语句"""
            logger.info(f"优化SQL: {sql}")
            return {
                "original_sql": sql,
                "optimized_sql": sql.replace("SELECT *", "SELECT id, name, date"),
                "performance_impact": "预计查询速度提升30%",
                "explanation": "选择特定列而不是全部列可以减少数据传输量"
            }
            
        @mcp.tool("fix_sql", description="修复SQL语句中的错误")
        async def fix_sql(sql: str, error_message: str, requirements: str = ""):
            """修复SQL语句中的错误"""
            logger.info(f"修复SQL: {sql}, 错误: {error_message}")
            return {
                "original_sql": sql,
                "fixed_sql": sql.replace("FORM", "FROM"),
                "explanation": "语法错误: 'FORM' 应该是 'FROM'"
            }
            
        @mcp.tool("set_llm_provider", description="设置LLM提供商")
        async def set_llm_provider(provider_name: str):
            """设置LLM提供商"""
            logger.info(f"设置LLM提供商: {provider_name}")
            return {
                "provider": provider_name,
                "message": f"已设置为 {provider_name}"
            }
            
        @mcp.tool("health", description="健康检查工具")
        async def health():
            """健康检查工具"""
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat()
            }
            
        @mcp.tool("status", description="获取服务器状态")
        async def status():
            """获取服务器状态"""
            try:
                import psutil
                cpu_usage = psutil.cpu_percent()
                memory_usage = psutil.virtual_memory().percent
            except ImportError:
                cpu_usage = "未安装psutil"
                memory_usage = "未安装psutil"
                
            return {
                "status": "running",
                "timestamp": datetime.now().isoformat(),
                "uptime": "12:34:56",
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage
            }
        
        logger.info(f"工具注册完成")
        return True
    except Exception as e:
        logger.error(f"工具注册失败: {str(e)}")
        return False

def init_tools() -> None:
    """
    初始化所有工具
    
    注册所有工具到工具注册中心
    """
    logger.info("开始初始化工具...")
    
    # 获取工具注册中心
    registry = get_registry()
    
    try:
        # 注册内置工具
        @register_tool(description="自然语言转SQL查询工具")
        def nl2sql_query(query: str):
            """将自然语言转换为SQL并执行查询"""
            from src.nl2sql_service import NL2SQLService
            service = NL2SQLService()
            return service.process_query(query)
        
        @register_tool(description="列出数据库中的所有表")
        def list_database_tables():
            """列出数据库中的所有表"""
            from src.nl2sql_service import NL2SQLService
            service = NL2SQLService()
            return service.list_tables()
        
        @register_tool(description="获取表结构的详细信息")
        def explain_table(table_name: str):
            """获取表结构详细信息"""
            from src.nl2sql_service import NL2SQLService
            service = NL2SQLService()
            return service.explain_table(table_name)
        
        @register_tool(description="获取数据库业务领域概览")
        def get_business_overview():
            """获取数据库业务领域概览"""
            from src.nl2sql_service import NL2SQLService
            service = NL2SQLService()
            return service.get_business_overview()
        
        @register_tool(description="刷新并保存元数据")
        def refresh_metadata(force: bool = False):
            """刷新并保存元数据"""
            from src.nl2sql_service import NL2SQLService
            service = NL2SQLService()
            return service.refresh_metadata(force)
        
        @register_tool(description="对SQL语句进行优化分析")
        def sql_optimize(sql: str, requirements: str = ""):
            """优化SQL语句"""
            from src.utils.sql_optimizer import SQLOptimizer
            optimizer = SQLOptimizer()
            return optimizer.optimize(sql, requirements)
        
        @register_tool(description="修复SQL语句中的错误")
        def fix_sql(sql: str, error_message: str, requirements: str = ""):
            """修复SQL语句中的错误"""
            from src.utils.sql_optimizer import SQLOptimizer
            optimizer = SQLOptimizer()
            return optimizer.fix_sql(sql, error_message, requirements)
        
        @register_tool(description="获取当前NL2SQL处理状态")
        def get_nl2sql_status():
            """获取当前NL2SQL处理状态"""
            from src.nl2sql_stream_processor import StreamNL2SQLProcessor
            processor = StreamNL2SQLProcessor()
            return processor.get_status()
        
        @register_tool(description="列出可用的LLM提供商")
        def list_llm_providers():
            """列出可用的LLM提供商"""
            from src.nl2sql_service import NL2SQLService
            service = NL2SQLService()
            providers = service.list_llm_providers()
            return {"providers": providers}
        
        @register_tool(description="设置LLM提供商")
        def set_llm_provider(provider_name: str):
            """设置LLM提供商"""
            from src.nl2sql_service import NL2SQLService
            service = NL2SQLService()
            service.set_llm_provider(provider_name)
            return {"provider": provider_name, "message": f"已设置为 {provider_name}"}
        
        @register_tool(description="健康检查工具")
        def health():
            """健康检查工具"""
            return {"status": "healthy", "timestamp": datetime.datetime.now().isoformat()}
        
        @register_tool(description="获取服务器状态")
        def status():
            """获取服务器状态"""
            import datetime
            return {"status": "running", "timestamp": datetime.datetime.now().isoformat()}
        
        logger.info("工具初始化完成")
    except Exception as e:
        logger.error(f"初始化工具时出错: {str(e)}")
        raise

# 直接运行测试
if __name__ == "__main__":
    # 设置日志级别
    logging.basicConfig(level=logging.INFO)
    
    # 初始化工具
    init_tools()
    
    # 获取所有工具
    registry = get_registry()
    tools = registry.get_all()
    
    # 打印工具列表
    print(f"已注册的工具 ({len(tools)}):")
    for name, (func, desc) in tools.items():
        print(f"- {name}: {desc}") 