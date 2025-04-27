#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL执行工具

负责执行SQL查询并处理结果
"""

import os
import json
import logging
import traceback
import time
from typing import Dict, Any, List, Optional, Tuple, Union
import re
import datetime
from decimal import Decimal

# 导入提示词
from src.prompts.prompts import SQL_EXECUTOR_PROMPTS, SQL_FIX_PROMPTS

# 获取日志记录器
logger = logging.getLogger("doris-mcp.sql-executor")

# 添加环境变量控制是否进行SQL安全校验
ENABLE_SQL_SECURITY_CHECK = os.environ.get('ENABLE_SQL_SECURITY_CHECK', 'false').lower() == 'true'

async def execute_sql_query(ctx) -> Dict[str, Any]:
    """
    执行SQL查询并返回结果
    
    Args:
        ctx: Context对象或字典，包含请求参数
        
    Returns:
        Dict[str, Any]: 执行结果
    """
    try:
        # 支持传入的参数是字典的情况
        if isinstance(ctx, dict) and 'params' in ctx:
            params = ctx['params']
        else:
            params = ctx.params

        sql = params.get("sql")
        db_name = params.get("db_name", os.getenv("DB_DATABASE", ""))
        max_rows = params.get("max_rows", 1000)  # 最大返回行数
        timeout = params.get("timeout", 30)  # 超时时间，单位秒
        
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少SQL参数",
                            "message": "请提供需要执行的SQL查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 先检查SQL安全性
        security_result = await _check_sql_security(sql)
        if not security_result.get("is_safe", False):
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "SQL安全性检查失败",
                            "message": "查询包含不安全的操作，无法执行",
                            "security_issues": security_result.get("security_issues", [])
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 导入数据库连接工具
        from src.utils.db import execute_query
        
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少SQL参数",
                            "message": "请提供需要执行的SQL查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 确保SELECT语句包含LIMIT子句
        sql_lower = sql.lower().strip()
        if sql_lower.startswith("select") and "limit" not in sql_lower:
            sql = sql.rstrip(";") + f" LIMIT {max_rows};"
        
        # 开始计时
        start_time = time.time()
        
        # 执行查询
        try:
            result = execute_query(sql, db_name)
            
            # 计算执行时间
            execution_time = time.time() - start_time
            
            # 构建返回结果
            if isinstance(result, list):
                # 处理查询结果列表
                row_count = len(result)
                
                # 如果结果为空，尝试优化SQL以返回有意义的结果
                if row_count == 0:
                    # 获取表结构信息用于优化
                    table_info = ""
                    try:
                        # 从SQL中提取表名
                        from src.tools.sql_validator_tools import extract_tables_from_sql
                        tables = await extract_tables_from_sql(sql, db_name)
                        
                        # 获取表结构
                        if tables:
                            from src.utils.meta_helper import get_tables_structure
                            table_info = get_tables_structure(tables, db_name)
                    except Exception as table_error:
                        logger.warning(f"获取表结构信息失败: {str(table_error)}")
                    
                
                    # 创建带有优化建议的响应
                    empty_response = {
                        "success": True,
                        "sql": sql,
                        "row_count": 0,
                        "columns": [],
                        "data": [],
                        "execution_time": execution_time,
                        "message": "查询执行成功，但没有返回任何数据",
                        "optimized_sql": None
                    }
                    empty_response = _serialize_row_data(empty_response)
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(empty_response, ensure_ascii=False)
                            }
                        ]
                    }
                
                # 提取列名
                if hasattr(result[0], "_fields"):
                    # 如果是命名元组
                    columns = list(result[0]._fields)
                else:
                    # 否则假设是字典
                    columns = list(result[0].keys()) if isinstance(result[0], dict) else []
                
                # 转换结果为可序列化的格式
                data = []
                for row in result:
                    row_dict = {}
                    if hasattr(row, "_asdict"):
                        # 如果是命名元组
                        row_dict = row._asdict()
                    elif isinstance(row, dict):
                        # 如果是字典
                        row_dict = row
                    else:
                        # 如果是列表或元组
                        row_dict = dict(zip(columns, row)) if columns else row
                    
                    # 处理特殊类型，使其可JSON序列化
                    serialized_row = _serialize_row_data(row_dict)
                    data.append(serialized_row)
                
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": True,
                                "sql": sql,
                                "row_count": row_count,
                                "columns": columns,
                                "data": data[:max_rows],  # 限制返回行数
                                "execution_time": execution_time,
                                "truncated": row_count > max_rows
                            }, ensure_ascii=False)
                        }
                    ]
                }
            else:
                # 处理其他类型的结果
                other_response = {
                    "success": True,
                    "sql": sql,
                    "result": str(result),
                    "execution_time": execution_time
                }
                other_response = _serialize_row_data(other_response)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(other_response, ensure_ascii=False)
                        }
                    ]
                }
                
        except Exception as db_error:
            error_message = str(db_error)
            
            # 尝试获取更详细的错误信息
            error_details = {}
            if "timeout" in error_message.lower():
                error_details["type"] = "timeout"
                error_details["suggestion"] = "查询执行超时，请优化SQL或增加超时时间"
            elif "syntax" in error_message.lower():
                error_details["type"] = "syntax"
                error_details["suggestion"] = "SQL语法错误，请检查语法"
            elif "not found" in error_message.lower() or "doesn't exist" in error_message.lower():
                error_details["type"] = "not_found"
                error_details["suggestion"] = "表或列不存在，请检查表名和列名"
            else:
                error_details["type"] = "unknown"
                error_details["suggestion"] = "请检查SQL语句并尝试简化查询"
            # 确保错误响应也是可序列化的
            error_response = _serialize_row_data(error_response)
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(error_response, ensure_ascii=False)
                    }
                ]
            }
        
    except Exception as e:
        logger.error(f"执行SQL查询失败: {str(e)}")
        logger.error(traceback.format_exc())
        
        error_response = {
            "success": False,
            "error": str(e),
            "message": "执行SQL查询时出错"
        }
        
        # 确保错误响应也是可序列化的
        error_response = _serialize_row_data(error_response)
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(error_response, ensure_ascii=False)
                }
            ]
        }

# 辅助函数
async def _check_sql_security(sql: str) -> Dict[str, Any]:
    """检查SQL安全性"""
    # 如果环境变量设置为关闭安全检查，直接返回安全
    if not ENABLE_SQL_SECURITY_CHECK:
        return {
            "is_safe": True,
            "security_issues": []
        }
        
    # 检查SQL是否包含危险操作
    sql_lower = sql.lower()
    
    # 检查是否是只读查询类型
    is_read_only = sql_lower.strip().startswith(("select ", "show ", "desc ", "describe ", "explain "))
    
    # 定义危险操作列表（只读和非只读查询都检查的部分）
    dangerous_operations = [
        (r'\bdelete\b', "删除操作"),
        (r'\bdrop\b', "删除表/数据库操作"),
        (r'\btruncate\b', "清空表操作"),
        (r'\bupdate\b', "更新操作"),
        (r'\binsert\b', "插入操作"),
        (r'\balter\b', "修改表结构操作"),
        (r'\bcreate\b', "创建表/数据库操作"),
        (r'\bgrant\b', "授权操作"),
        (r'\brevoke\b', "撤销权限操作"),
        (r'\bexec\b', "执行存储过程"),
        (r'\bxp_', "扩展存储过程，可能存在安全风险"),
        (r'\bshutdown\b', "关闭数据库操作"),
        (r'\bunion\s+all\s+select\b', "UNION语句，可能用于SQL注入"),
        (r'\bunion\s+select\b', "UNION语句，可能用于SQL注入"),
        (r'\binto\s+outfile\b', "写入文件操作"),
        (r'\bload_file\b', "加载文件操作")
    ]
    
    # 只有非只读查询才检查的危险操作
    non_readonly_operations = []
    if not is_read_only:
        non_readonly_operations = [
            (r'--', "SQL注释，可能用于SQL注入"),
            (r'/\*', "SQL块注释，可能用于SQL注入")
        ]
    
    # 检查是否包含危险操作
    security_issues = []
    
    # 检查所有查询都需检查的危险操作
    for operation, description in dangerous_operations:
        if re.search(operation, sql_lower):
            # 对于只读查询中的特定关键词，我们需要区分是否作为独立操作使用
            if is_read_only and operation in [r'\bcreate\b', r'\bdrop\b', r'\bdelete\b', r'\binsert\b', r'\bupdate\b', r'\balter\b']:
                # 检查是否作为DDL/DML关键字使用，如 CREATE TABLE, DROP DATABASE 等
                pattern = operation + r'\s+(?:table|database|view|index|procedure|function|trigger|event)'
                if re.search(pattern, sql_lower):
                    security_issues.append({
                        "operation": operation.replace(r'\b', '').replace(r'\s+', ' '),
                        "description": description,
                        "severity": "高"
                    })
            else:
                security_issues.append({
                    "operation": operation.replace(r'\b', '').replace(r'\s+', ' '),
                    "description": description,
                    "severity": "高"
                })
    
    # 检查非只读查询特有的危险操作
    for operation, description in non_readonly_operations:
        if re.search(operation, sql_lower):
            security_issues.append({
                "operation": operation.replace(r'\b', '').replace(r'\s+', ' '),
                "description": description,
                "severity": "中"
            })
    
    return {
        "is_safe": len(security_issues) == 0,
        "security_issues": security_issues
    }

def _serialize_row_data(row_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    将行数据中的特殊类型（如日期、时间、Decimal等）转换为可JSON序列化的格式
    
    Args:
        row_data: 行数据字典
        
    Returns:
        Dict[str, Any]: 处理后的可序列化字典
    """
    serialized_data = {}
    for key, value in row_data.items():
        if value is None:
            serialized_data[key] = None
        elif isinstance(value, (datetime.date, datetime.datetime)):
            # 将日期和时间类型转换为ISO格式字符串
            serialized_data[key] = value.isoformat()
        elif isinstance(value, Decimal):
            # 将Decimal类型转换为浮点数
            serialized_data[key] = float(value)
        elif isinstance(value, (list, tuple)):
            # 递归处理列表或元组中的元素
            serialized_data[key] = [
                _serialize_row_data(item) if isinstance(item, dict) else item
                for item in value
            ]
        elif isinstance(value, dict):
            # 递归处理嵌套字典
            serialized_data[key] = _serialize_row_data(value)
        else:
            serialized_data[key] = value
    return serialized_data 