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

# 导入工具注册中心
from src.tools.tool_registry import get_registry, register_tool

# 导入NL2SQLService
from src.nl2sql_service import NL2SQLService

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
        
        # 初始化NL2SQL服务
        nl2sql_service = NL2SQLService()
        
        # 注册NL2SQL相关工具
        @mcp.tool("nl2sql_query", description="将自然语言查询转换为SQL并执行返回结果")
        async def nl2sql_query(query: str):
            """将自然语言查询转换为SQL并执行返回结果"""
            logger.info(f"执行NL2SQL查询: {query}")
            try:
                # 记录开始处理请求
                start_time = datetime.now()
                request_id = hash(f"{query}_{start_time.isoformat()}")
                
                # 记录请求到审计日志
                audit_data = {
                    "timestamp": start_time.isoformat(),
                    "request_id": str(request_id),
                    "action": "nl2sql_query",
                    "query": query,
                    "status": "processing"
                }
                audit_logger = logging.getLogger("audit")
                audit_logger.audit(json.dumps(audit_data))
                
                # 调用服务处理查询
                result = nl2sql_service.process_query(query)
                
                # 记录处理结果到审计日志
                end_time = datetime.now()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                audit_data.update({
                    "end_timestamp": end_time.isoformat(),
                    "duration_ms": duration_ms,
                    "status": "completed" if result.get("success", False) else "failed",
                    "sql": result.get("sql", ""),
                    "error": result.get("error", None)
                })
                audit_logger.audit(json.dumps(audit_data))
                
                # 返回结果
                return result
            except Exception as e:
                # 记录错误到审计日志和错误日志
                error_msg = str(e)
                logger.error(f"处理查询时出错: {error_msg}")
                
                # 错误审计
                if 'audit_data' in locals():
                    audit_data.update({
                        "status": "error",
                        "error": error_msg
                    })
                    audit_logger.audit(json.dumps(audit_data))
                
                return {
                    "success": False,
                    "message": f"处理查询时出错: {error_msg}",
                    "query": query
                }
        
        @mcp.tool("nl2sql_query_stream", description="将自然语言查询转换为SQL并使用流式响应返回结果")
        async def nl2sql_query_stream(query: str, callback=None):
            """将自然语言查询转换为SQL并使用流式响应返回结果"""
            logger.info(f"执行流式NL2SQL查询: {query}")
            try:
                # 记录开始处理请求
                start_time = datetime.now()
                request_id = hash(f"{query}_{start_time.isoformat()}")
                
                # 记录请求到审计日志
                audit_data = {
                    "timestamp": start_time.isoformat(),
                    "request_id": str(request_id),
                    "action": "nl2sql_query_stream",
                    "query": query,
                    "status": "processing"
                }
                audit_logger = logging.getLogger("audit")
                audit_logger.audit(json.dumps(audit_data))
                
                # 导入流式处理器
                try:
                    from src.nl2sql_stream_processor import StreamNL2SQLProcessor
                    stream_processor = StreamNL2SQLProcessor()
                    
                    # 创建一个回调函数，如果callback可用则使用，否则使用哑回调
                    async def process_callback(content: str, metadata: dict):
                        if callback:
                            await callback(content, metadata)
                    
                    # 使用流式处理查询
                    result = await stream_processor.process_stream(query, process_callback)
                except ImportError:
                    logger.warning("无法导入StreamNL2SQLProcessor，使用标准处理器")
                    # 使用标准处理器作为后备
                    result = nl2sql_service.process_query(query)
                    
                    # 模拟流式回调
                    if callback:
                        try:
                            await callback("正在处理查询...", {"step": "parsing"})
                            if result.get("sql"):
                                await callback(f"生成SQL: {result.get('sql')}", {"step": "sql_generation"})
                            await callback("查询处理完成", {"step": "complete"})
                        except Exception as cb_error:
                            logger.error(f"回调处理出错: {str(cb_error)}")
                
                # 记录处理结果到审计日志
                end_time = datetime.now()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                audit_data.update({
                    "end_timestamp": end_time.isoformat(),
                    "duration_ms": duration_ms,
                    "status": "completed" if not result.get("error") else "failed",
                    "sql": result.get("sql", ""),
                    "error": result.get("error", None)
                })
                audit_logger.audit(json.dumps(audit_data))
                
                # 返回结果
                return result
            except Exception as e:
                # 记录错误到审计日志和错误日志
                error_msg = str(e)
                logger.error(f"处理流式查询时出错: {error_msg}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                
                # 错误审计
                if 'audit_data' in locals():
                    audit_data.update({
                        "status": "error",
                        "error": error_msg
                    })
                    audit_logger.audit(json.dumps(audit_data))
                
                if callback:
                    try:
                        await callback(f"查询处理出错: {str(e)}", {"step": "error"})
                    except:
                        pass
                
                return {
                    "success": False,
                    "message": f"处理流式查询时出错: {error_msg}",
                    "query": query
                }
        
        @mcp.tool("list_database_tables", description="列出数据库中的所有表")
        async def list_database_tables():
            """列出数据库中的所有表"""
            logger.info("获取数据库表列表")
            try:
                result = nl2sql_service.list_tables()
                return result
            except Exception as e:
                logger.error(f"列出表时出错: {str(e)}")
                return {
                    "success": False,
                    "message": f"列出表时出错: {str(e)}"
                }
        
        @mcp.tool("explain_table", description="获取表结构的详细信息")
        async def explain_table(table_name: str):
            """获取表结构的详细信息"""
            logger.info(f"获取表结构: {table_name}")
            try:
                result = nl2sql_service.explain_table(table_name)
                return result
            except Exception as e:
                logger.error(f"解释表时出错: {str(e)}")
                return {
                    "success": False,
                    "message": f"解释表时出错: {str(e)}"
                }
        
        @mcp.tool("get_business_overview", description="获取数据库业务领域概览")
        async def get_business_overview():
            """获取数据库业务领域概览"""
            logger.info("获取业务领域概览")
            try:
                result = nl2sql_service.get_business_overview()
                return result
            except Exception as e:
                logger.error(f"获取业务概览时出错: {str(e)}")
                return {
                    "success": False,
                    "message": f"获取业务概览时出错: {str(e)}"
                }
        
        @mcp.tool("get_nl2sql_status", description="获取当前NL2SQL处理状态")
        async def get_nl2sql_status():
            """获取当前NL2SQL处理状态"""
            logger.info("获取NL2SQL处理状态")
            try:
                # 使用流式处理器获取当前状态
                try:
                    from src.nl2sql_stream_processor import StreamNL2SQLProcessor
                    stream_processor = StreamNL2SQLProcessor()
                    status = stream_processor.get_current_processing_status()
                    return {
                        "success": True,
                        "current_status": status,
                        "timestamp": datetime.now().isoformat()
                    }
                except ImportError:
                    # 如果无法导入流式处理器，返回基本状态
                    return {
                        "status": "idle",
                        "message": "系统准备就绪",
                        "timestamp": datetime.now().isoformat()
                    }
            except Exception as e:
                logger.error(f"获取处理状态时出错: {str(e)}")
                return {
                    "success": False,
                    "message": f"获取处理状态时出错: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                }
            
        @mcp.tool("refresh_metadata", description="刷新并保存元数据")
        async def refresh_metadata(force: bool = False):
            """刷新并保存元数据"""
            logger.info(f"刷新元数据 [强制: {force}]")
            try:
                nl2sql_service._refresh_metadata(force=force)
                refresh_type = "全量" if force else "增量"
                return {
                    "success": True,
                    "message": f"{refresh_type}元数据刷新完成"
                }
            except Exception as e:
                logger.error(f"刷新元数据时出错: {str(e)}")
                return {
                    "success": False,
                    "message": f"刷新元数据时出错: {str(e)}"
                }
            
        @mcp.tool("sql_optimize", description="对SQL语句进行优化分析，提供性能改进建议和业务含义解读")
        async def sql_optimize(sql: str, requirements: str = ""):
            """优化SQL语句"""
            logger.info(f"优化SQL: {sql}")
            try:
                # 记录开始处理请求
                start_time = datetime.now()
                request_id = hash(f"{sql}_{requirements}_{start_time.isoformat()}")
                
                # 记录请求到审计日志
                audit_data = {
                    "timestamp": start_time.isoformat(),
                    "request_id": str(request_id),
                    "action": "sql_optimize",
                    "sql": sql,
                    "requirements": requirements,
                    "status": "processing"
                }
                audit_logger = logging.getLogger("audit")
                audit_logger.audit(json.dumps(audit_data))
                
                # 导入SQL优化器
                from src.utils.sql_optimizer import SQLOptimizer
                sql_optimizer = SQLOptimizer()
                
                # 调用SQL优化器处理
                result = sql_optimizer.process(sql, requirements)
                
                # 记录处理结果到审计日志
                end_time = datetime.now()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                audit_data.update({
                    "end_timestamp": end_time.isoformat(),
                    "duration_ms": duration_ms,
                    "status": result.get("status", "unknown"),
                    "original_sql": sql,
                })
                audit_logger.audit(json.dumps(audit_data))
                
                # 返回结果
                return result
            except Exception as e:
                # 记录错误到审计日志和错误日志
                error_msg = str(e)
                logger.error(f"处理SQL优化请求时出错: {error_msg}")
                
                # 错误审计
                if 'audit_data' in locals():
                    audit_data.update({
                        "status": "error",
                        "error": error_msg
                    })
                    audit_logger.audit(json.dumps(audit_data))
                
                return {
                    "success": False,
                    "message": f"处理SQL优化请求时出错: {error_msg}",
                    "sql": sql,
                    "requirements": requirements
                }
            
        @mcp.tool("fix_sql", description="修复SQL语句中的错误")
        async def fix_sql(sql: str, error_message: str, requirements: str = ""):
            """修复SQL语句中的错误"""
            logger.info(f"修复SQL: {sql}, 错误: {error_message}")
            try:
                # 导入SQL优化器
                from src.utils.sql_optimizer import SQLOptimizer
                sql_optimizer = SQLOptimizer()
                
                # 提取表信息
                table_info = sql_optimizer.extract_table_info(sql)
                
                # 调用SQL修复功能
                result = sql_optimizer.fix_sql(sql, error_message, requirements, table_info)
                
                return {
                    "success": True,
                    "original_sql": sql,
                    "fix_result": result,
                    "table_info": table_info
                }
            except Exception as e:
                error_msg = str(e)
                logger.error(f"修复SQL时出错: {error_msg}")
                
                return {
                    "success": False,
                    "message": f"修复SQL时出错: {error_msg}",
                    "sql": sql,
                    "error_message": error_message
                }
            
        @mcp.tool("list_llm_providers", description="列出可用的LLM提供商")
        async def list_llm_providers():
            """列出可用的LLM提供商"""
            logger.info("获取LLM提供商列表")
            try:
                providers = nl2sql_service.get_available_llm_providers()
                return {
                    "success": True,
                    "providers": providers,
                    "current_provider": nl2sql_service.llm_provider
                }
            except Exception as e:
                logger.error(f"列出LLM提供商时出错: {str(e)}")
                return {
                    "success": False,
                    "message": f"列出LLM提供商时出错: {str(e)}"
                }
        
        @mcp.tool("set_llm_provider", description="设置LLM提供商")
        async def set_llm_provider(provider_name: str):
            """设置LLM提供商"""
            logger.info(f"设置LLM提供商: {provider_name}")
            try:
                success = nl2sql_service.set_llm_provider(provider_name)
                if success:
                    return {
                        "success": True,
                        "message": f"已切换到LLM提供商: {provider_name}"
                    }
                else:
                    return {
                        "success": False,
                        "message": f"切换LLM提供商失败，{provider_name} 可能不可用"
                    }
            except Exception as e:
                logger.error(f"设置LLM提供商时出错: {str(e)}")
                return {
                    "success": False,
                    "message": f"设置LLM提供商时出错: {str(e)}"
                }
            
        @mcp.tool("health", description="健康检查工具")
        async def health():
            """健康检查工具"""
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "version": "1.0.0"
            }
            
        @mcp.tool("status", description="获取服务器状态")
        async def status():
            """获取服务器状态"""
            try:
                import psutil
                import platform
                
                process = psutil.Process(os.getpid())
                
                # 获取LLM提供商状态
                llm_providers = []
                try:
                    from src.utils.llm_client import get_llm_providers
                    providers = get_llm_providers()
                    llm_providers = list(providers.keys()) if providers else []
                except Exception as e:
                    logger.warning(f"获取LLM提供商信息失败: {str(e)}")
                    # 提供一个默认列表，避免前端错误
                    llm_providers = ["openai", "local"]
                
                return {
                    "service": {
                        "status": "running",
                        "uptime": datetime.now().timestamp() - process.create_time(),
                        "started_at": datetime.fromtimestamp(process.create_time()).isoformat(),
                        "timestamp": datetime.now().isoformat(),
                        "version": "0.1.0"
                    },
                    "system": {
                        "platform": platform.platform(),
                        "python_version": platform.python_version(),
                        "cpu_count": psutil.cpu_count(),
                        "memory_usage": {
                            "percent": psutil.virtual_memory().percent,
                            "used_mb": round(psutil.virtual_memory().used / (1024 * 1024), 2),
                            "total_mb": round(psutil.virtual_memory().total / (1024 * 1024), 2)
                        },
                        "process_memory_mb": round(process.memory_info().rss / (1024 * 1024), 2)
                    },
                    "config": {
                        "host": os.getenv("SERVER_HOST", "0.0.0.0"),
                        "port": int(os.getenv("SERVER_PORT", 8080)),
                        "mcp_port": int(os.getenv("MCP_PORT", 3000)),
                        "log_level": os.getenv("LOG_LEVEL", "INFO"),
                        "llm_provider": os.getenv("LLM_PROVIDER", "openai")
                    },
                    "llm": {
                        "providers": llm_providers,
                        "default_provider": os.getenv("LLM_PROVIDER", "openai"),
                        "default_model": os.getenv(f"{os.getenv('LLM_PROVIDER', 'openai').upper()}_MODEL", "unknown")
                    }
                }
            except Exception as e:
                logger.error(f"获取服务器状态失败: {str(e)}")
                return {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
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