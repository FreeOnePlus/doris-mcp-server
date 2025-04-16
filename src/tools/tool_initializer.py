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

# 导入Context
from mcp.server.fastmcp import Context

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
        async def nl2sql_query(ctx: Context) -> Dict[str, Any]:
            """将自然语言查询转换为SQL并执行返回结果"""
            # 从Context中获取NL2SQL服务和查询参数
            query = ctx.params.get("query", "")
            nl2sql_service = ctx.request_context.lifespan_context.nl2sql_service
            
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
                
                # 确保返回结果格式符合FastMCP期望
                if "success" in result and not result["success"]:
                    # 如果是错误结果，重新格式化为标准结构
                    if "message" in result:
                        error_message = result["message"]
                        logger.error(f"查询处理失败: {error_message}")
                        error = {
                            "code": -32000,
                            "message": error_message,
                            "data": {
                                "query": query,
                                "type": "query_processing_error"
                            }
                        }
                        raise ValueError(json.dumps(error))
                
                # 将结果格式化为客户端期望的格式
                formatted_result = {
                    "content": [
                        {
                            "text": json.dumps(result),
                            "type": "json"
                        }
                    ]
                }
                
                # 返回格式化后的结果
                return formatted_result
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
                
                # 使用标准JSON-RPC错误格式
                error = {
                    "code": -32000,  # 服务器内部错误
                    "message": f"处理查询时出错: {error_msg}",
                    "data": {
                        "type": "query_processing_error",
                        "query": query,
                        "details": error_msg
                    }
                }
                raise ValueError(json.dumps(error))
        
        @mcp.tool("nl2sql_query_stream", description="将自然语言查询转换为SQL并使用流式响应返回结果")
        async def nl2sql_query_stream(ctx: Context) -> Dict[str, Any]:
            """将自然语言查询转换为SQL并使用流式响应返回结果"""
            # 从Context中获取NL2SQL服务和查询参数
            query = ctx.params.get("query", "")
            nl2sql_service = ctx.request_context.lifespan_context.nl2sql_service
            # 获取回调函数
            callback = ctx.params.get("callback")
            
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
                
                # 确保返回结果格式符合FastMCP期望
                if "success" in result and not result["success"]:
                    # 如果是错误结果，重新格式化为标准结构
                    if "message" in result:
                        error_message = result["message"]
                        logger.error(f"查询处理失败: {error_message}")
                        error = {
                            "code": -32000,
                            "message": error_message,
                            "data": {
                                "query": query,
                                "type": "query_processing_error"
                            }
                        }
                        raise ValueError(json.dumps(error))
                
                # 将结果格式化为客户端期望的格式
                formatted_result = {
                    "content": [
                        {
                            "text": json.dumps(result),
                            "type": "json"
                        }
                    ]
                }
                
                # 如果是成功结果，返回标准格式
                return formatted_result
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
                
                # 使用标准JSON-RPC错误格式
                error = {
                    "code": -32000,  # 服务器内部错误
                    "message": f"处理流式查询时出错: {error_msg}",
                    "data": {
                        "type": "stream_processing_error",
                        "query": query,
                        "details": error_msg
                    }
                }
                raise ValueError(json.dumps(error))
        
        @mcp.tool("list_database_tables", description="列出数据库中的所有表")
        async def list_database_tables(ctx: Context) -> Dict[str, Any]:
            """列出数据库中的所有表"""
            logger.info("获取数据库表列表")
            # 从Context中获取NL2SQL服务
            nl2sql_service = ctx.request_context.lifespan_context.nl2sql_service
            
            try:
                result = nl2sql_service.list_tables()
                # 将结果格式化为客户端期望的格式
                formatted_result = {
                    "content": [
                        {
                            "text": json.dumps(result),
                            "type": "json"
                        }
                    ]
                }
                return formatted_result
            except Exception as e:
                logger.error(f"列出表时出错: {str(e)}")
                # 修改错误返回格式，使用标准JSON-RPC错误格式
                error = {
                    "code": -32000,  # 服务器内部错误
                    "message": f"列出表时出错: {str(e)}",
                    "data": {
                        "type": "database_tables_error",
                        "details": str(e)
                    }
                }
                raise ValueError(json.dumps(error))
        
        @mcp.tool("explain_table", description="获取表结构的详细信息")
        async def explain_table(ctx: Context) -> Dict[str, Any]:
            """获取表结构的详细信息"""
            # 从Context中获取NL2SQL服务和表名参数
            table_name = ctx.params.get("table_name", "")
            nl2sql_service = ctx.request_context.lifespan_context.nl2sql_service
            
            logger.info(f"获取表结构: {table_name}")
            try:
                result = nl2sql_service.explain_table(table_name)
                # 将结果格式化为客户端期望的格式
                formatted_result = {
                    "content": [
                        {
                            "text": json.dumps(result),
                            "type": "json"
                        }
                    ]
                }
                return formatted_result
            except Exception as e:
                logger.error(f"解释表时出错: {str(e)}")
                # 修改错误返回格式，使用标准JSON-RPC错误格式
                error = {
                    "code": -32000,  # 服务器内部错误
                    "message": f"解释表时出错: {str(e)}",
                    "data": {
                        "type": "explain_table_error",
                        "details": str(e),
                        "table": table_name
                    }
                }
                raise ValueError(json.dumps(error))
        
        @mcp.tool("get_business_overview", description="获取数据库业务领域概览")
        async def get_business_overview(ctx: Context) -> Dict[str, Any]:
            """获取数据库业务领域概览"""
            logger.info("获取业务领域概览")
            # 从Context中获取NL2SQL服务
            nl2sql_service = ctx.request_context.lifespan_context.nl2sql_service
            
            try:
                result = nl2sql_service.get_business_overview()
                # 将结果格式化为客户端期望的格式
                formatted_result = {
                    "content": [
                        {
                            "text": json.dumps(result),
                            "type": "json"
                        }
                    ]
                }
                return formatted_result
            except Exception as e:
                logger.error(f"获取业务概览时出错: {str(e)}")
                # 修改错误返回格式，使用标准JSON-RPC错误格式
                error = {
                    "code": -32000,  # 服务器内部错误
                    "message": f"获取业务概览时出错: {str(e)}",
                    "data": {
                        "type": "business_overview_error",
                        "details": str(e)
                    }
                }
                raise ValueError(json.dumps(error))
        
        @mcp.tool("get_nl2sql_status", description="获取当前NL2SQL处理状态")
        async def get_nl2sql_status(ctx: Context) -> Dict[str, Any]:
            """获取当前NL2SQL处理状态"""
            logger.info("获取NL2SQL处理状态")
            try:
                # 使用流式处理器获取当前状态
                try:
                    from src.nl2sql_stream_processor import StreamNL2SQLProcessor
                    stream_processor = StreamNL2SQLProcessor()
                    status = stream_processor.get_current_processing_status()
                    formatted_result = {
                        "content": [
                            {
                                "text": json.dumps({
                                    "success": True,
                                    "current_status": status,
                                    "timestamp": datetime.now().isoformat()
                                }),
                                "type": "json"
                            }
                        ]
                    }
                    return formatted_result
                except ImportError:
                    # 如果无法导入流式处理器，返回基本状态
                    formatted_result = {
                        "content": [
                            {
                                "text": json.dumps({
                                    "status": "idle",
                                    "message": "系统准备就绪",
                                    "timestamp": datetime.now().isoformat()
                                }),
                                "type": "json"
                            }
                        ]
                    }
                    return formatted_result
            except Exception as e:
                logger.error(f"获取处理状态时出错: {str(e)}")
                # 修改错误返回格式，使用标准JSON-RPC错误格式
                error = {
                    "code": -32000,  # 服务器内部错误
                    "message": f"获取处理状态时出错: {str(e)}",
                    "data": {
                        "type": "status_error",
                        "details": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                }
                raise ValueError(json.dumps(error))
            
        @mcp.tool("refresh_metadata", description="刷新并保存元数据")
        async def refresh_metadata(ctx: Context) -> Dict[str, Any]:
            """刷新并保存元数据"""
            # 从Context中获取NL2SQL服务和force参数
            force = ctx.params.get("force", False)
            nl2sql_service = ctx.request_context.lifespan_context.nl2sql_service
            
            logger.info(f"刷新元数据 [强制: {force}]")
            try:
                # 注：这里使用了内部方法_refresh_metadata，如果公开了refresh_metadata方法，应该使用公开方法
                success = nl2sql_service._refresh_metadata(force=force)
                refresh_type = "全量" if force else "增量"
                formatted_result = {
                    "content": [
                        {
                            "text": json.dumps({
                                "success": True,
                                "message": f"{refresh_type}元数据刷新完成"
                            }),
                            "type": "json"
                        }
                    ]
                }
                return formatted_result
            except Exception as e:
                logger.error(f"刷新元数据时出错: {str(e)}")
                # 修改错误返回格式，使用标准JSON-RPC错误格式
                error = {
                    "code": -32000,  # 服务器内部错误
                    "message": f"刷新元数据时出错: {str(e)}",
                    "data": {
                        "type": "refresh_metadata_error",
                        "details": str(e),
                        "force": force
                    }
                }
                raise ValueError(json.dumps(error))
        
        # 其他工具函数可以类似地修改为使用Context
            
        @mcp.tool("sql_optimize", description="对SQL语句进行优化分析，提供性能改进建议和业务含义解读")
        async def sql_optimize(ctx: Context) -> Dict[str, Any]:
            """优化SQL语句"""
            # 从Context中获取参数
            sql = ctx.params.get("sql", "")
            requirements = ctx.params.get("requirements", "")
            
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
        async def fix_sql(ctx: Context) -> Dict[str, Any]:
            """修复SQL语句中的错误"""
            # 从Context中获取参数
            sql = ctx.params.get("sql", "")
            error_message = ctx.params.get("error_message", "")
            requirements = ctx.params.get("requirements", "")
            
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
            
        @mcp.tool("health", description="健康检查工具")
        async def health(ctx: Context) -> Dict[str, Any]:
            """健康检查工具"""
            # 获取应用配置
            config = ctx.request_context.lifespan_context.config
            
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "version": config.get("version", "1.0.0"),
                "server_name": config.get("server_name", "Doris MCP Server")
            }
            
        @mcp.tool("status", description="获取服务器状态")
        async def status(ctx: Context) -> Dict[str, Any]:
            """获取服务器状态"""
            # 获取应用配置和服务
            config = ctx.request_context.lifespan_context.config
            nl2sql_service = ctx.request_context.lifespan_context.nl2sql_service
            
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
                        "version": config.get("version", "1.0.0")
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
                        "llm_provider": config.get("llm_provider", "openai")
                    },
                    "llm": {
                        "providers": llm_providers,
                        "default_provider": config.get("llm_provider", "openai"),
                        "default_model": config.get("llm_model", "unknown")
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