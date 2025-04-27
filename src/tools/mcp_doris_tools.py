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
from typing import Dict, Any, List
import traceback
import psutil  # Added import
import datetime # Added import

# 获取日志记录器
logger = logging.getLogger("doris-mcp-tools")

# 导入SimpleContext类，用于传递参数
from src.utils.context import SimpleContext
from src.utils.db import get_doris_version_comment # <-- Import the new function

# 注意：所有函数不再使用装饰器，直接作为普通异步函数定义
# mcp_doris_explain_table函数已被移除，功能已合并到mcp_doris_get_schema_list中

async def mcp_doris_refresh_metadata(db_name: str = None, force_refresh: bool = False) -> Dict[str, Any]:
    """
    刷新并保存元数据
    
    Args:
        db_name: 数据库名称，如果为None则刷新所有数据库
        force_refresh: 是否强制全量刷新，默认为False
        
    Returns:
        Dict[str, Any]: 刷新结果指令，由客户端负责执行具体的刷新操作
    """
    logger.info("MCP工具调用: mcp_doris_refresh_metadata")
    
    try:
        # 使用metadata_tools刷新元数据
        from src.tools.metadata_tools import refresh_metadata
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({"params": {"db_name": db_name, "force_refresh": force_refresh}})
        result = await refresh_metadata(ctx)
        
        # 直接返回结果
        return result
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
                        "message": "准备元数据刷新流程时出错",
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_sql_optimize(sql: str = None, optimization_level: str = "normal", db_name: str = None) -> Dict[str, Any]:
    """
    对SQL语句进行优化分析，提供性能改进建议和业务含义解读
    
    Args:
        sql: 需要优化的SQL语句
        optimization_level: 优化级别，可选值：normal, aggressive
        db_name: 目标数据库名称
        
    Returns:
        Dict[str, Any]: 优化提示词
    """
    logger.info(f"MCP工具调用: mcp_doris_sql_optimize, SQL: {sql}, 优化级别: {optimization_level}")
    
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
        
        # 格式化SQL，移除多余的换行符和反斜杠转义
        import re
        formatted_sql = sql.replace('\\n', '\n')
        formatted_sql = re.sub(r'\s+', ' ', formatted_sql).strip()
        
        # 如果未提供db_name，使用环境变量中的默认数据库
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 从prompts.py中获取SQL优化专用提示词
        from src.prompts.prompts import SQL_VALIDATION_PROMPTS
        
        # 获取系统提示词和用户提示词模板 - 使用专门的优化提示词
        system_prompt = SQL_VALIDATION_PROMPTS["optimization_system"]
        user_prompt_template = SQL_VALIDATION_PROMPTS["optimization_user"]
        
        # 获取表结构信息
        table_info = ""
        try:
            # 从SQL中提取表名
            from src.tools.sql_validator_tools import extract_tables_from_sql
            tables = await extract_tables_from_sql(formatted_sql, db_name)
            
            # 获取表结构
            if tables:
                from src.utils.meta_helper import get_tables_structure
                table_info = get_tables_structure(tables, db_name)
        except Exception as e:
            logger.warning(f"获取表结构信息失败: {str(e)}")
        
        # 获取执行成本估计（如果需要）
        cost_estimate = {}
        try:
            from src.tools.sql_validator_tools import _estimate_execution_cost
            cost_estimate = await _estimate_execution_cost(formatted_sql, db_name)
        except Exception as e:
            logger.warning(f"获取执行成本估计失败: {str(e)}")
            cost_estimate = {"details": "无法获取执行成本估计"}
        
        # 填充用户提示词模板
        user_prompt = user_prompt_template.format(
            sql=formatted_sql,
            db_name=db_name,
            tables_info=table_info,
            cost_estimate=cost_estimate.get("details", ""),
            improvement_type=optimization_level
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词和相关信息
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "prompt": complete_prompt,
                        "sql": formatted_sql,
                        "optimization_level": optimization_level,
                        "db_name": db_name,
                        "message": "请使用提供的完整优化提示词对SQL进行分析和优化"
                    }, ensure_ascii=False)
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
                        "sql": sql,
                        "optimization_level": optimization_level
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_fix_sql(sql: str = None, error_message: str = "", db_name: str = None) -> Dict[str, Any]:
    """
    修复SQL语句中的错误
    
    Args:
        sql: 需要修复的SQL语句
        error_message: 错误信息(可选)
        db_name: 目标数据库名称
        
    Returns:
        Dict[str, Any]: 修复提示词
    """
    logger.info(f"MCP工具调用: mcp_doris_fix_sql, SQL: {sql}, 错误信息: {error_message}")
    
    try:
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
        
        # 如果未提供db_name，使用环境变量中的默认数据库
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
            
        # 从prompts.py中获取SQL修复提示词
        from src.prompts.prompts import SQL_FIX_PROMPTS
        
        # 获取系统提示词和用户提示词模板
        system_prompt = SQL_FIX_PROMPTS["system"]
        user_prompt_template = SQL_FIX_PROMPTS["user"]
        
        # 获取表结构信息
        table_info = ""
        try:
            # 从SQL中提取表名
            from src.tools.sql_validator_tools import extract_tables_from_sql
            tables = await extract_tables_from_sql(sql, db_name)
            
            # 获取表结构
            if tables:
                from src.utils.meta_helper import get_tables_structure
                table_info = get_tables_structure(tables, db_name)
        except Exception as e:
            logger.warning(f"获取表结构信息失败: {str(e)}")
        
        # 填充用户提示词模板
        user_prompt = user_prompt_template.format(
            query=sql,
            error_message=error_message,
            original_question="请修复SQL语句中的错误",
            tables_info=table_info
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词和相关信息
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "prompt": complete_prompt,
                        "sql": sql,
                        "error_message": error_message,
                        "db_name": db_name,
                        "message": "请使用提供的完整修复提示词对SQL进行修复，并返回修复好的SQL语句",
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
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
    健康检查工具, 包含数据库和基本系统资源检查
    
    Returns:
        Dict[str, Any]: 健康状态
    """
    logger.info("MCP工具调用: mcp_doris_health")
    
    db_status = "unknown"
    system_healthy = True
    cpu_percent = -1.0
    memory_percent = -1.0
    
    try:
        # 检查数据库连接和版本
        doris_version = "未知"
        try:
            doris_version = get_doris_version_comment()
            if not doris_version.startswith("未知"):
                db_status = "healthy"
                logger.info(f"数据库连接正常，Doris版本: {doris_version}")
            else:
                db_status = "error"
                logger.warning(f"数据库连接检查失败或无法获取版本: {doris_version}")
        except Exception as db_error:
            logger.warning(f"数据库连接检查失败: {str(db_error)}", exc_info=True)
            doris_version = f"未知 (检查异常: {str(db_error)})"
            db_status = "error"
        
        # 检查基本系统资源
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1) # Use short interval
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            logger.info(f"系统资源检查: CPU={cpu_percent}%, Memory={memory_percent}%")
            # 可以根据阈值判断系统是否健康，例如
            # if cpu_percent > 95 or memory_percent > 90:
            #     system_healthy = False
            #     logger.warning("系统资源使用率过高")
        except Exception as sys_error:
            logger.error(f"获取系统资源信息失败: {str(sys_error)}")
            system_healthy = False # Mark system as unhealthy if resources cannot be checked

        # 获取服务器时间
        server_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 综合状态判断
        overall_status = "healthy"
        if db_status != "healthy" or not system_healthy:
            overall_status = "error"
            
        # 构建结果
        result = {
            "status": overall_status,
            "database": {
                "status": db_status,
                "version": doris_version
            },
            "system": {
                "status": "healthy" if system_healthy else "error",
                "cpu_usage_percent": cpu_percent,
                "memory_usage_percent": memory_percent
            },
            "server_time": server_time, # Moved server_time here
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
        logger.error(f"健康检查失败: {str(e)}", exc_info=True) # Log traceback
        
        # Simplified error response
        error_result = {
             "status": "error",
             "error": f"Health check failed: {str(e)}",
             "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(error_result, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_status(include_metrics: bool = False) -> Dict[str, Any]:
    """
    获取服务器状态, 可选包含详细系统指标
    
    Args:
        include_metrics: 是否包含详细系统指标 (CPU, Memory, Disk, Uptime)

    Returns:
        Dict[str, Any]: 服务器状态
    """
    logger.info(f"MCP工具调用: mcp_doris_status, include_metrics={include_metrics}")
    
    try:
        # 获取服务器时间
        current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 获取Doris版本 (使用新函数)
        doris_version = "未知"
        try:
            doris_version = get_doris_version_comment()
            if doris_version.startswith("未知"):
                 logger.warning(f"获取Doris版本失败或信息不完整: {doris_version}")
            else:
                 logger.info(f"成功获取Doris版本: {doris_version}")
        except Exception as db_error:
            logger.warning(f"获取Doris版本失败: {str(db_error)}", exc_info=True)
            doris_version = f"未知 (获取异常: {str(db_error)})"
        
        # 获取基本状态信息
        status_data = {
            "status": "running",
            "service_name": "Doris MCP Server",
            "version": "0.1.0", # Consider reading from a config or package info
            "timestamp": current_timestamp,
            "doris_version": doris_version
        }

        # 如果请求包含详细指标，则添加系统资源信息
        if include_metrics:
            try:
                logger.info("获取详细系统指标...")
                # CPU
                cpu_percent = psutil.cpu_percent(interval=0.1)
                cpu_count_logical = psutil.cpu_count()
                cpu_count_physical = psutil.cpu_count(logical=False)
                
                # Memory
                memory = psutil.virtual_memory()
                memory_total_gb = memory.total / (1024**3)
                memory_used_gb = memory.used / (1024**3)
                memory_percent = memory.percent
                
                # Disk (Root)
                disk = psutil.disk_usage('/')
                disk_total_gb = disk.total / (1024**3)
                disk_used_gb = disk.used / (1024**3)
                disk_percent = disk.percent
                
                # Uptime
                boot_time_timestamp = psutil.boot_time()
                boot_time = datetime.datetime.fromtimestamp(boot_time_timestamp)
                uptime_seconds = time.time() - boot_time_timestamp
                uptime_str = str(datetime.timedelta(seconds=int(uptime_seconds)))

                status_data["system_metrics"] = {
                    "cpu": {
                        "usage_percent": cpu_percent,
                        "logical_cores": cpu_count_logical,
                        "physical_cores": cpu_count_physical
                    },
                    "memory": {
                        "total_gb": round(memory_total_gb, 2),
                        "used_gb": round(memory_used_gb, 2),
                        "usage_percent": memory_percent
                    },
                    "disk_root": {
                        "total_gb": round(disk_total_gb, 2),
                        "used_gb": round(disk_used_gb, 2),
                        "usage_percent": disk_percent
                    },
                    "uptime": {
                         "seconds": int(uptime_seconds),
                         "readable": uptime_str,
                         "boot_time": boot_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
                logger.info("成功获取详细系统指标")
            except Exception as metrics_error:
                 logger.error(f"获取系统指标时出错: {metrics_error}", exc_info=True)
                 status_data["system_metrics"] = {"error": f"Failed to get metrics: {metrics_error}"}

        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(status_data, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"获取服务器状态失败: {str(e)}", exc_info=True) # Log traceback
        
        # Simplified error response
        error_result = {
             "status": "error",
             "error": f"Failed to get status: {str(e)}",
             "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(error_result, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_exec_query(sql: str = None, db_name: str = None, max_rows: int = 100, timeout: int = 30) -> Dict[str, Any]:
    """
    执行SQL查询并返回结果
    
    Args:
        sql: 需要执行的SQL语句
        db_name: 目标数据库名称
        max_rows: 最大返回行数
        timeout: 查询超时时间（秒）
        
    Returns:
        Dict[str, Any]: 查询结果
    """
    logger.info(f"MCP工具调用: mcp_doris_exec_query, SQL: {sql}, DB: {db_name}, max_rows: {max_rows}, timeout: {timeout}")
    
    try:
        # 检查参数是否为None
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少SQL参数",
                            "message": "请提供需要执行的SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 格式化SQL，处理可能的反斜杠换行符
        import re
        formatted_sql = sql.replace('\\n', '\n')
        # 保留原始换行符，以保持SQL的可读性，但是去除多余的空格
        formatted_sql = re.sub(r'[ \t]+', ' ', formatted_sql).strip()
        
        # 使用sql_executor_tools执行SQL
        from src.tools.sql_executor_tools import execute_sql_query
        
        # 如果未提供db_name，使用环境变量中的默认数据库
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 检查SQL是否只读
        from src.utils.db import is_read_only_query
        if not is_read_only_query(formatted_sql):
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "不允许执行写操作",
                            "message": "出于安全考虑，仅允许执行SELECT、SHOW和DESCRIBE等只读查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({
            "params": {
                "sql": formatted_sql,
                "db_name": db_name,
                "max_rows": max_rows,
                "timeout": timeout
            }
        })
        result = await execute_sql_query(ctx)
        
        # 直接返回结果
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_exec_query: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "sql": sql,
                        "db_name": db_name
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_save_metadata(metadata: str = None, metadata_type: str = None, table_name: str = "", db_name: str = None) -> Dict[str, Any]:
    """
    保存元数据到数据库
    
    Args:
        metadata: 元数据JSON字符串或字典
        metadata_type: 元数据类型，默认根据table_name自动确定（table_summary或business_summary）
        table_name: 表名（可选）
        db_name: 数据库名称（可选）
        
    Returns:
        Dict[str, Any]: 保存结果
    """
    logger.info(f"MCP工具调用: mcp_doris_save_metadata, 表名: {table_name}")
    
    try:
        # 检查必要参数
        if not metadata:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少元数据参数",
                            "message": "请提供元数据内容"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 调用metadata_tools中的save_metadata函数
        from src.tools.metadata_tools import save_metadata
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({
            "params": {
                "db_name": db_name,
                "metadata": metadata,
                "metadata_type": metadata_type,
                "table_name": table_name
            }
        })
        
        result = await save_metadata(ctx)
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_save_metadata: {str(e)}")
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "保存元数据失败",
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_get_schema_list(table_name: str = "", db_name: str = None) -> Dict[str, Any]:
    """
    获取数据库或表结构信息
    
    Args:
        table_name: 表名（可选，如果提供则只返回该表的结构）
        db_name: 目标数据库名称，默认使用当前数据库
        
    Returns:
        Dict[str, Any]: 数据库或表结构信息
    """
    logger.info(f"MCP工具调用: mcp_doris_get_schema_list, 表名: {table_name}, 数据库: {db_name}")
    
    try:
        # 如果未提供db_name，使用环境变量中的默认数据库
        if db_name is None:
            db_name = os.getenv("DB_DATABASE", "")
            logger.info(f"使用默认数据库: {db_name}")
        
        # 验证数据库名称是否有效
        if not db_name:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "未指定数据库名称",
                            "message": "请提供有效的数据库名称参数"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 处理跨库表名格式 (db_name.table_name)
        if table_name and "." in table_name:
            parts = table_name.split(".", 1)
            if len(parts) == 2:
                extracted_db_name, extracted_table_name = parts
                # 使用表名中提取的db_name
                logger.info(f"从表名 {table_name} 中提取数据库名 {extracted_db_name}")
                db_name = extracted_db_name
                table_name = extracted_table_name
        
        # 使用metadata_tools获取结构信息
        from src.tools.metadata_tools import get_schema_list
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({"params": {"table_name": table_name, "db_name": db_name}})
        result = await get_schema_list(ctx)
        
        # 直接返回结果
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_get_schema_list: {str(e)}")
        logger.error(traceback.format_exc())
        # 返回更友好的错误信息
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": "获取结构信息失败",
                        "message": f"获取数据库或表结构时出错: {str(e)}",
                        "db_name": db_name,
                        "table_name": table_name
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_generate_sql(query: str = None, db_name: str = None, tables: List[str] = None) -> Dict[str, Any]:
    """
    根据自然语言生成SQL但不执行
    
    Args:
        query: 自然语言查询描述
        db_name: 目标数据库名称，默认使用当前数据库
        tables: 相关表名列表，可选参数
        
    Returns:
        Dict[str, Any]: 生成的SQL
    """
    logger.info(f"MCP工具调用: mcp_doris_generate_sql, 查询: {query}, 表名: {tables}")
    
    try:
        # 检查参数是否为None
        if not query:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少查询参数",
                            "message": "请提供自然语言查询描述"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 导入提示词模板
        from src.prompts.prompts import SQL_GENERATION_PROMPTS
        
        # 获取数据库表结构信息
        tables_info = ""
        try:
            # 导入元数据提取器
            from src.utils.metadata_extractor import MetadataExtractor
            extractor = MetadataExtractor(db_name)
            
            # 如果提供了特定表列表，只获取这些表的结构
            if tables and len(tables) > 0:
                logger.info(f"使用指定表列表: {tables}")
                target_tables = []
            
                # 处理可能的"db_name.table_name"格式
                for table_spec in tables:
                    if "." in table_spec:
                        parts = table_spec.split(".", 1)
                        if len(parts) == 2:
                            table_db, table_name = parts
                            # 如果数据库名与当前不同，需要处理跨库表名
                            if db_name != table_db:
                                logger.info(f"处理跨库表名: {table_spec}")
                            target_tables.append((table_db, table_name))
                    else:
                        target_tables.append((db_name, table_spec))
                
                # 获取指定表的结构
                for table_db, table_name in target_tables:
                    schema = extractor.get_table_schema(table_name, table_db)
                    if schema:
                        tables_info += f"\n表名: {table_db}.{table_name}\n"
                        tables_info += f"表说明: {schema.get('comment', '')}\n"
                        tables_info += "字段:\n"
                        
                        for col in schema.get('columns', []):
                            col_name = col.get('name', '')
                            col_type = col.get('type', '')
                            col_comment = col.get('comment', '')
                            tables_info += f"  - {col_name} ({col_type}): {col_comment}\n"
                        
                        tables_info += "\n"
            else:
                # 获取所有表
                db_tables = extractor.get_database_tables(db_name)
                
                # 收集所有表的结构信息
                for table_name in db_tables:
                    schema = extractor.get_table_schema(table_name, db_name)
                    if schema:
                        tables_info += f"\n表名: {table_name}\n"
                        tables_info += f"表说明: {schema.get('comment', '')}\n"
                        tables_info += "字段:\n"
                        
                        for col in schema.get('columns', []):
                            col_name = col.get('name', '')
                            col_type = col.get('type', '')
                            col_comment = col.get('comment', '')
                            tables_info += f"  - {col_name} ({col_type}): {col_comment}\n"
                        
                        tables_info += "\n"
        except Exception as e:
            logger.warning(f"获取表结构信息失败: {str(e)}")
        
        # 构建提示词
        system_prompt = SQL_GENERATION_PROMPTS["system"]
        user_prompt = SQL_GENERATION_PROMPTS["user"].format(
            query=query,
            db_name=db_name,
            tables_info=tables_info
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词和相关信息
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "query": query,
                        "db_name": db_name,
                        "tables": tables,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词生成SQL，仅生成SQL返回即可，无需考虑执行"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_generate_sql: {str(e)}")
        logger.error(traceback.format_exc())
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "query": query,
                        "tables": tables if tables else []
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_explain_sql(sql: str = None, db_name: str = None, explanation_level: str = "detailed") -> Dict[str, Any]:
    """
    详细解释SQL语句的功能和组成部分
    
    Args:
        sql: 需要解释的SQL语句
        db_name: 目标数据库名称，默认使用当前数据库
        explanation_level: 解释详细程度，可选值：'basic'、'detailed'，默认为'detailed'
        
    Returns:
        Dict[str, Any]: 解释结果
    """
    logger.info(f"MCP工具调用: mcp_doris_explain_sql, SQL: {sql}")
    
    try:
        # 检查参数是否为None
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少SQL参数",
                            "message": "请提供需要解释的SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用sql_generator_tools解释SQL
        from src.tools.sql_generator_tools import explain_sql
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({"params": {"sql": sql, "db_name": db_name, "explanation_level": explanation_level}})
        result = await explain_sql(ctx)
        
        # 直接返回结果
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_explain_sql: {str(e)}")
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

async def mcp_doris_modify_sql(sql: str = None, modification: str = None, db_name: str = None) -> Dict[str, Any]:
    """
    根据自然语言描述修改SQL
    
    Args:
        sql: 原始SQL语句
        modification: 修改需求描述
        db_name: 目标数据库名称，默认使用当前数据库
        
    Returns:
        Dict[str, Any]: 修改结果
    """
    logger.info(f"MCP工具调用: mcp_doris_modify_sql, SQL: {sql}, 修改描述: {modification}")
    
    try:
        # 检查参数是否为None
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少SQL参数",
                            "message": "请提供原始SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        if not modification:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少修改描述参数",
                            "message": "请提供修改需求描述"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
            
        # 获取表结构信息
        tables_info = ""
        try:
            # 导入元数据提取器
            from src.utils.metadata_extractor import MetadataExtractor
            extractor = MetadataExtractor(db_name)
            
            # 获取所有表
            tables = extractor.get_database_tables(db_name)
            
            # 收集所有表的结构信息
            for table_name in tables:
                schema = extractor.get_table_schema(table_name, db_name)
                if schema:
                    tables_info += f"\n表名: {table_name}\n"
                    tables_info += f"表说明: {schema.get('table_comment', '')}\n"
                    tables_info += "字段:\n"
                    
                    for col in schema.get('columns', []):
                        col_name = col.get('name', '')
                        col_type = col.get('type', '')
                        col_comment = col.get('comment', '')
                        tables_info += f"  - {col_name} ({col_type}): {col_comment}\n"
                    
                    tables_info += "\n"
        except Exception as e:
            logger.warning(f"获取表结构信息失败: {str(e)}")
            
        # 导入SQL修改提示词
        from src.prompts.prompts import SQL_MODIFICATION_PROMPTS
        
        # 构建提示词
        system_prompt = SQL_MODIFICATION_PROMPTS["system"]
        user_prompt = SQL_MODIFICATION_PROMPTS["user"].format(
            sql=sql,
            modification=modification,
            db_name=db_name,
            tables_info=tables_info
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词和相关信息
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "sql": sql,
                        "modification": modification,
                        "db_name": db_name,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词修改SQL查询"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_modify_sql: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "sql": sql,
                        "modification": modification
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_parse_query(query: str = None, db_name: str = None, table_names: List[str] = None) -> Dict[str, Any]:
    """
    解析自然语言查询，提取查询意图、实体和条件
    
    Args:
        query: 自然语言查询
        db_name: 目标数据库名称，默认使用当前数据库
        table_names: 表名列表，如果提供则获取指定表的元数据
        
    Returns:
        Dict[str, Any]: 解析结果的提示词
    """
    logger.info(f"MCP工具调用: mcp_doris_parse_query, 查询: {query}, 表名: {table_names}")
    
    try:
        # 检查参数是否为None
        if not query:
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
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 从环境变量获取跨库查询设置
        enable_multi_database = os.getenv("ENABLE_MULTI_DATABASE", "false").lower() == "true"
            
        # 导入提示词模板
        from src.prompts.prompts import QUERY_PARSE_PROMPTS
        
        # 导入元数据提取器
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        
        # 根据调用方式选择不同的处理逻辑
        if table_names:
            # 方式1：传入了表名列表，获取指定表的元数据
            logger.info(f"解析查询使用指定表: {table_names}")
            
            # 获取指定表的结构信息
            tables_info = ""
            for table_name in table_names:
                schema = extractor.get_table_schema(table_name, db_name)
                if schema:
                    tables_info += f"\n表名: {table_name}\n"
                    tables_info += f"表说明: {schema.get('table_comment', '')}\n"
                    tables_info += "字段:\n"
                    
                    for col in schema.get('columns', []):
                        col_name = col.get('name', '')
                        col_type = col.get('type', '')
                        col_comment = col.get('comment', '')
                        tables_info += f"  - {col_name} ({col_type}): {col_comment}\n"
                    
                    tables_info += "\n"
            
            # 构建带有表元数据的提示词
            system_prompt = QUERY_PARSE_PROMPTS["system"]
            user_prompt = QUERY_PARSE_PROMPTS["user"].format(
                query=query,
                db_name=db_name,
                tables_info=tables_info
            )
            
            # 构建完整的提示词
            complete_prompt = {
                "system": system_prompt,
                "user": user_prompt
            }
            
            # 返回提示词和相关信息
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": True,
                            "query": query,
                            "db_name": db_name,
                            "tables": table_names,
                            "prompt": complete_prompt,
                            "message": "请使用提供的完整提示词解析查询，提取用于生成SQL的实体",
                            "mode": "table_analysis"
                        }, ensure_ascii=False)
                    }
                ]
            }
        else:
            # 方式2：没有传入表名列表，返回数据库总体业务分析和表列表
            logger.info("解析查询使用数据库总体业务分析")
            
            # 获取所有表
            all_tables = []
            
            if enable_multi_database:
                # 开启跨库查询，获取所有数据库
                # 实现跨库查询逻辑需要增加获取所有数据库的功能
                # 这里简化示例，实际需要根据系统设计调整
                all_db_tables = {}
                try:
                    from src.utils.db import get_all_databases
                    dbs = get_all_databases()
                    for db in dbs:
                        tables = extractor.get_database_tables(db)
                        all_db_tables[db] = tables
                        for table in tables:
                            all_tables.append(f"{db}.{table}")
                except Exception as e:
                    logger.warning(f"获取所有数据库表失败: {str(e)}")
                    # 退回到使用当前数据库
                    tables = extractor.get_database_tables(db_name)
                    all_tables = tables
            else:
                # 不开启跨库查询，只使用当前数据库
                tables = extractor.get_database_tables(db_name)
                all_tables = tables
            
            # 获取数据库业务概览
            try:
                from src.tools.metadata_tools import get_business_overview_data
                business_overview = await get_business_overview_data(db_name)
                
                # 格式化业务概览
                business_summary = f"""
业务领域: {business_overview.get('business_domain', '未知')}

核心业务实体:
"""
                for entity in business_overview.get('core_entities', []):
                    business_summary += f"- {entity.get('name', '')}: {entity.get('description', '')}\n"
                
                business_summary += "\n业务流程:\n"
                for process in business_overview.get('business_processes', []):
                    business_summary += f"- {process.get('name', '')}: {process.get('description', '')}\n"
                
            except Exception as e:
                logger.warning(f"获取业务概览失败: {str(e)}")
                business_summary = "未能获取业务概览"
            
            # 构建表列表信息
            tables_list = "\n".join([f"- {table}" for table in all_tables])
            
            # 导入表选择提示词模板
            from src.prompts.prompts import TABLE_SELECTION_PROMPTS
            
            # 构建表选择提示词
            system_prompt = TABLE_SELECTION_PROMPTS["system"]
            user_prompt = TABLE_SELECTION_PROMPTS["user"].format(
                query=query,
                db_name=db_name,
                cross_db=str(enable_multi_database).lower(),
                business_summary=business_summary,
                tables_list=tables_list
            )
            
            # 构建完整的提示词
            complete_prompt = {
                "system": system_prompt,
                "user": user_prompt
            }
            
            # 返回提示词和相关信息
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": True,
                            "query": query,
                            "db_name": db_name,
                            "all_tables": all_tables,
                            "prompt": complete_prompt,
                            "message": "请使用提供的完整提示词分析查询，选择出相关表名列表，再次调用mcp_doris_parse_query工具，传入查询的自然语言和表名列表",
                            "mode": "table_selection"
                        }, ensure_ascii=False)
                    }
                ]
            }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_parse_query: {str(e)}")
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

async def mcp_doris_identify_query_type(query: str = None, db_name: str = None) -> Dict[str, Any]:
    """
    识别查询类型，判断查询属于什么类别
    
    Args:
        query: 自然语言查询
        db_name: 目标数据库名称，默认使用当前数据库
        
    Returns:
        Dict[str, Any]: 查询类型识别的提示词
    """
    logger.info(f"MCP工具调用: mcp_doris_identify_query_type, 查询: {query}")
    
    try:
        # 检查参数是否为None
        if not query:
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
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 导入提示词模板
        from src.prompts.prompts import QUERY_TYPE_IDENTIFICATION_PROMPTS
        
        # 获取数据库表结构信息
        tables_info = ""
        try:
            # 导入元数据提取器
            from src.utils.metadata_extractor import MetadataExtractor
            extractor = MetadataExtractor(db_name)
            
            # 获取所有表
            tables = extractor.get_database_tables(db_name)
            
            # 收集所有表的结构信息
            for table_name in tables:
                schema = extractor.get_table_schema(table_name, db_name)
                if schema:
                    tables_info += f"\n表名: {table_name}\n"
                    tables_info += f"表说明: {schema.get('table_comment', '')}\n"
                    tables_info += "字段:\n"
                    
                    for col in schema.get('columns', []):
                        col_name = col.get('name', '')
                        col_type = col.get('type', '')
                        col_comment = col.get('comment', '')
                        tables_info += f"  - {col_name} ({col_type}): {col_comment}\n"
                    
                    tables_info += "\n"
        except Exception as e:
            logger.warning(f"获取表结构信息失败: {str(e)}")
        
        # 构建提示词
        system_prompt = QUERY_TYPE_IDENTIFICATION_PROMPTS["system"]
        user_prompt = QUERY_TYPE_IDENTIFICATION_PROMPTS["user"].format(
            query=query,
            db_name=db_name,
            tables_info=tables_info
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词和相关信息
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "query": query,
                        "db_name": db_name,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词识别查询类型"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_identify_query_type: {str(e)}")
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

async def mcp_doris_validate_sql_syntax(sql: str = None, db_name: str = None) -> Dict[str, Any]:
    """
    验证SQL语法是否正确
    
    Args:
        sql: 需要验证的SQL语句
        db_name: 目标数据库名称，默认使用当前数据库
        
    Returns:
        Dict[str, Any]: 验证结果
    """
    logger.info(f"MCP工具调用: mcp_doris_validate_sql_syntax, SQL: {sql}")
    
    try:
        # 检查参数是否为None
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少SQL参数",
                            "message": "请提供需要验证的SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 导入验证提示词
        from src.prompts.prompts import SQL_VALIDATION_PROMPTS
        
        # 构建提示词
        system_prompt = SQL_VALIDATION_PROMPTS["syntax_system"]
        user_prompt = SQL_VALIDATION_PROMPTS["syntax_user"].format(sql=sql)
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "sql": sql,
                        "db_name": db_name,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词验证SQL语法"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_validate_sql_syntax: {str(e)}")
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

async def mcp_doris_check_sql_security(sql: str = None) -> Dict[str, Any]:
    """
    检查SQL语句的安全性
    
    Args:
        sql: 需要检查的SQL语句
        
    Returns:
        Dict[str, Any]: 安全检查结果
    """
    logger.info(f"MCP工具调用: mcp_doris_check_sql_security, SQL: {sql}")
    
    try:
        # 检查参数是否为None
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少SQL参数",
                            "message": "请提供需要检查的SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 导入验证提示词
        from src.prompts.prompts import SQL_VALIDATION_PROMPTS
        
        # 构建提示词
        system_prompt = SQL_VALIDATION_PROMPTS["security_system"]
        user_prompt = SQL_VALIDATION_PROMPTS["security_user"].format(sql=sql)
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "sql": sql,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词检查SQL安全性"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_check_sql_security: {str(e)}")
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

async def mcp_doris_analyze_query_result(query_result: str = None, analysis_type: str = "summary") -> Dict[str, Any]:
    """
    分析查询结果，提供业务洞察
    
    Args:
        query_result: 查询结果JSON字符串
        analysis_type: 分析类型，可选值：'summary'、'trend'、'correlation'，默认为'summary'
        
    Returns:
        Dict[str, Any]: 分析结果
    """
    logger.info(f"MCP工具调用: mcp_doris_analyze_query_result, 分析类型: {analysis_type}")
    
    try:
        # 检查参数是否为None
        if not query_result:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少查询结果参数",
                            "message": "请提供查询结果JSON字符串"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 如果query_result是字典类型，先将其转换为JSON字符串
        if isinstance(query_result, dict):
            query_result = json.dumps(query_result, ensure_ascii=False)
        
        # 解析查询结果
        try:
            result_obj = json.loads(query_result)
            sql = result_obj.get("sql", "")
            data = result_obj.get("data", [])
            query = result_obj.get("query", "")
            
            if not data:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "error": "查询结果为空",
                                "message": "无法分析空的查询结果"
                            }, ensure_ascii=False)
                        }
                    ]
                }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "查询结果解析失败",
                            "message": f"JSON解析错误: {str(e)}"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 导入业务分析提示词
        from src.prompts.prompts import BUSINESS_ANALYSIS_PROMPTS
        
        # 构建提示词
        system_prompt = BUSINESS_ANALYSIS_PROMPTS["system"]
        user_prompt = BUSINESS_ANALYSIS_PROMPTS["user"].format(
            query=query,
            sql=sql,
            result=json.dumps(data, ensure_ascii=False),
            tables_info=""  # 这里可以选择是否添加表结构信息
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "query_result": query_result,
                        "analysis_type": analysis_type,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词分析查询结果，生成业务洞察"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_analyze_query_result: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "analysis_type": analysis_type
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_find_similar_examples(query: str = None, db_name: str = None, top_k: int = 3) -> Dict[str, Any]:
    """
    查找与当前查询相似的示例
    
    Args:
        query: 自然语言查询
        db_name: 目标数据库名称，默认使用当前数据库
        top_k: 返回的最大相似示例数量，默认为3
        
    Returns:
        Dict[str, Any]: 相似示例
    """
    logger.info(f"MCP工具调用: mcp_doris_find_similar_examples, 查询: {query}, top_k: {top_k}")
    
    try:
        # 检查参数是否为None
        if not query:
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
        
        # 使用similarity_tools查找相似示例
        from src.tools.similarity_tools import find_similar_examples
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({"params": {"query": query, "db_name": db_name, "top_k": top_k}})
        result = await find_similar_examples(ctx)
        
        # 直接返回结果
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_find_similar_examples: {str(e)}")
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

async def mcp_doris_find_similar_history(query: str = None, top_k: int = 3) -> Dict[str, Any]:
    """
    查找与当前查询相似的历史记录
    
    Args:
        query: 自然语言查询
        top_k: 返回的最大相似历史记录数量，默认为3
        
    Returns:
        Dict[str, Any]: 相似历史记录
    """
    logger.info(f"MCP工具调用: mcp_doris_find_similar_history, 查询: {query}, top_k: {top_k}")
    
    try:
        # 检查参数是否为None
        if not query:
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
        
        # 使用similarity_tools查找相似历史记录
        from src.tools.similarity_tools import find_similar_history
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({"params": {"query": query, "top_k": top_k}})
        result = await find_similar_history(ctx)
        
        # 直接返回结果
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_find_similar_history: {str(e)}")
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

async def mcp_doris_calculate_query_similarity(query1: str = None, query2: str = None) -> Dict[str, Any]:
    """
    计算两个查询之间的相似度
    
    Args:
        query1: 第一个查询
        query2: 第二个查询
        
    Returns:
        Dict[str, Any]: 相似度计算结果
    """
    logger.info(f"MCP工具调用: mcp_doris_calculate_query_similarity")
    
    try:
        # 检查参数是否为None
        if not query1 or not query2:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少查询参数",
                            "message": "请提供两个查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用similarity_tools计算查询相似度
        from src.tools.similarity_tools import calculate_query_similarity
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({"params": {"query1": query1, "query2": query2}})
        result = await calculate_query_similarity(ctx)
        
        # 直接返回结果
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_calculate_query_similarity: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "query1": query1,
                        "query2": query2
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_adapt_similar_query(current_query: str = None, similar_query: str = None, db_name: str = None) -> Dict[str, Any]:
    """
    根据当前需求调整相似查询的SQL
    
    Args:
        current_query: 当前查询
        similar_query: 相似查询的SQL
        db_name: 目标数据库名称，默认使用当前数据库
        
    Returns:
        Dict[str, Any]: 调整结果
    """
    logger.info(f"MCP工具调用: mcp_doris_adapt_similar_query, 当前查询: {current_query}, 相似查询SQL: {similar_query}")
    
    try:
        # 检查参数是否为None
        if not current_query:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少当前查询参数",
                            "message": "请提供当前查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        if not similar_query:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "error": "缺少相似查询SQL参数",
                            "message": "请提供相似查询的SQL"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 导入查询适配提示词
        from src.prompts.prompts import QUERY_ADAPTATION_PROMPTS
        
        # 构建提示词
        system_prompt = QUERY_ADAPTATION_PROMPTS["system"]
        user_prompt = QUERY_ADAPTATION_PROMPTS["user"].format(
            current_query=current_query,
            similar_query="", # 这里是对应相似查询的自然语言描述，但我们参数中没有，可以留空
            similar_sql=similar_query
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "current_query": current_query,
                        "similar_sql": similar_query,
                        "db_name": db_name,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词调整相似查询的SQL，使其符合当前查询需求"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_adapt_similar_query: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "current_query": current_query,
                        "similar_query": similar_query
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_get_metadata(db_name: str = None, table_name: str = None, business_overview_only: bool = False) -> Dict[str, Any]:
    """
    获取元数据信息，可以是数据库元数据或表元数据
    
    Args:
        db_name: 目标数据库名称，默认使用当前数据库
        table_name: 表名称（可选，如果提供则获取表元数据）
        business_overview_only: 是否只返回业务概览信息，默认为False
        
    Returns:
        Dict[str, Any]: 元数据信息
    """
    logger.info(f"MCP工具调用: mcp_doris_get_metadata, 数据库: {db_name}, 表: {table_name}, 仅概览: {business_overview_only}")
    
    try:
        # 使用默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 如果只需要业务概览信息，并且没有指定表名
        if business_overview_only and not table_name:
            # 使用metadata_tools获取业务概览
            from src.tools.metadata_tools import get_business_overview
            
            # 使用SimpleContext传递参数
            ctx = SimpleContext({"params": {"db_name": db_name}})
            return await get_business_overview(ctx)
        
        # 否则使用metadata_tools获取完整元数据
        from src.tools.metadata_tools import get_metadata
        
        # 使用SimpleContext传递参数
        ctx = SimpleContext({"params": {"db_name": db_name, "table_name": table_name or ""}})
        result = await get_metadata(ctx)
        
        # 直接返回结果
        return result
    except Exception as e:
        logger.error(f"MCP工具执行失败 mcp_doris_get_metadata: {str(e)}")
        # 返回错误结果
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "error": str(e),
                        "db_name": db_name,
                        "table_name": table_name
                    }, ensure_ascii=False)
                }
            ]
        }

async def mcp_doris_get_nl2sql_prompt() -> Dict[str, Any]:
    """
    获取NL2SQL工具使用指南的提示词，用于指导ClientLLM如何按照正确流程调用相关工具
    
    Returns:
        Dict[str, Any]: 包含NL2SQL工具使用指南的提示词
    """
    logger.info("MCP工具调用: mcp_doris_get_nl2sql_prompt")
    
    # 导入NL2SQL工具流程指南提示词
    from src.prompts.prompts import NL2SQL_WORKFLOW_GUIDE
    
    # 返回指南
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps({
                    "success": True,
                    "guide": NL2SQL_WORKFLOW_GUIDE,
                    "message": "成功获取NL2SQL工具流程指南"
                }, ensure_ascii=False)
            }
        ]
    } 