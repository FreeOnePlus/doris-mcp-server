#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL生成工具

根据自然语言查询生成SQL语句，包括生成、改进、解释和修改SQL
"""

import os
import json
import logging
import traceback
from typing import Dict, Any, Tuple

# 获取日志记录器
logger = logging.getLogger("doris-mcp.sql-generator")

# 从环境变量获取数据库名称
DEFAULT_DB = os.getenv("DB_DATABASE", "")

async def explain_sql(ctx) -> Dict[str, Any]:
    """
    解释SQL查询，提供每个部分的功能说明
    
    Args:
        ctx: Context对象，包含请求参数
        
    Returns:
        Dict[str, Any]: SQL解释的提示词
    """
    try:
        # 获取参数
        sql = ctx.params.get("sql", "")
        db_name = ctx.params.get("db_name", DEFAULT_DB)
        
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少SQL参数",
                            "message": "请提供要解释的SQL语句"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 获取数据库表结构
        tables_info = await _get_table_info(db_name)
        
        # 格式化表结构信息
        formatted_tables_info = _format_tables_info(tables_info) if tables_info else ""
        
        # 导入提示词
        from src.prompts.prompts import SQL_EXPLAIN_PROMPTS
        
        # 构建提示
        system_prompt = SQL_EXPLAIN_PROMPTS["system"]
        user_prompt = SQL_EXPLAIN_PROMPTS["user"].format(
            sql=sql,
            table_info=formatted_tables_info
        )
        
        # 构建完整的提示词
        complete_prompt = {
            "system": system_prompt,
            "user": user_prompt
        }
        
        # 返回提示词和SQL信息
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "sql": sql,
                        "db_name": db_name,
                        "prompt": complete_prompt,
                        "message": "请使用提供的完整提示词解释SQL语句"
                    }, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"生成SQL解释提示词失败: {str(e)}", exc_info=True)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": "生成SQL解释提示词失败",
                        "message": str(e)
                    }, ensure_ascii=False)
                }
            ]
        }

# 辅助函数: 格式化表结构信息
def _format_tables_info(tables_info: Dict) -> str:
    """格式化表结构信息为字符串

    Args:
        tables_info: 表结构信息字典

    Returns:
        str: 格式化后的表结构信息
    """
    formatted_info = []
    
    for table_name, columns in tables_info.items():
        table_info = f"表名: {table_name}\n列:"
        
        for col in columns:
            col_name = col.get("name", "")
            col_type = col.get("type", "")
            is_key = col.get("is_key", False)
            comment = col.get("comment", "")
            
            key_str = " (主键)" if is_key else ""
            comment_str = f" - {comment}" if comment else ""
            
            table_info += f"\n  - {col_name}: {col_type}{key_str}{comment_str}"
        
        formatted_info.append(table_info)
    
    return "\n\n".join(formatted_info)

# 辅助函数: 从响应中提取SQL
def _extract_sql(response_text: str) -> Tuple[str, str]:
    """从LLM响应文本中提取SQL和解释

    Args:
        response_text: LLM响应文本

    Returns:
        Tuple[str, str]: (SQL, 解释)
    """
    # 尝试提取SQL代码块
    sql_pattern = r"```sql\s*([\s\S]*?)\s*```"
    sql_matches = re.findall(sql_pattern, response_text)
    
    if sql_matches:
        sql = sql_matches[0].strip()
        
        # 移除响应中的SQL代码块，剩余部分作为解释
        explanation = re.sub(sql_pattern, "", response_text).strip()
        return sql, explanation
    
    # 如果没有SQL代码块，尝试提取一般代码块
    code_pattern = r"```\s*([\s\S]*?)\s*```"
    code_matches = re.findall(code_pattern, response_text)
    
    if code_matches:
        sql = code_matches[0].strip()
        
        # 移除响应中的代码块，剩余部分作为解释
        explanation = re.sub(code_pattern, "", response_text).strip()
        return sql, explanation
    
    # 如果没有代码块，尝试提取SELECT, INSERT, UPDATE, DELETE等关键字开头的行
    sql_keywords = r"(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|SHOW|DESCRIBE|USE)"
    query_pattern = r"(?:^|\n)(" + sql_keywords + r"[\s\S]*?)((?=\n\n)|$)"
    query_matches = re.findall(query_pattern, response_text, re.IGNORECASE)
    
    if query_matches:
        sql = query_matches[0][0].strip()
        
        # 移除提取的SQL，剩余部分作为解释
        explanation = re.sub(re.escape(sql), "", response_text).strip()
        return sql, explanation
    
    # 如果以上都失败，返回空
    return "", response_text

# 辅助函数: 获取数据库表结构
async def _get_table_info(db_name: str) -> Dict:
    """获取数据库表结构信息
    
    Args:
        db_name: 数据库名称

    Returns:
        Dict: 表结构信息 {表名: [列信息]}
    """
    try:
        from src.utils.db import get_tables_in_database, execute_query
        
        # 获取数据库中的表
        tables = get_tables_in_database(db_name)
        if not tables:
            logger.warning(f"数据库 {db_name} 中没有找到表")
            return {}
        
        # 获取每个表的结构
        tables_info = {}
        for table in tables:
            # 使用execute_query代替get_table_schema
            columns = execute_query(f"DESCRIBE `{db_name}`.`{table}`")
            if columns:
                tables_info[table] = columns
        
        return tables_info
    except Exception as e:
        logger.error(f"获取数据库表结构失败: {str(e)}", exc_info=True)
        return {}

    """基于错误信息分析和修复SQL

    Args:
        sql: 原始SQL
        error_message: 错误信息
        tables_info: 可选的表结构信息

    Returns:
        Tuple[str, str]: (修复后的SQL, 修复说明)
    """
    try:
        from src.utils.llm_client import get_llm_client, Message
        from src.prompts.prompts import SQL_REPAIR_PROMPTS
        
        # 获取LLM客户端
        llm_client = get_llm_client()
        
        # 格式化表结构信息
        formatted_tables_info = _format_tables_info(tables_info) if tables_info else ""
        
        # 构建提示
        system_prompt = SQL_REPAIR_PROMPTS["system"]
        user_prompt = SQL_REPAIR_PROMPTS["user"].format(
            sql=sql,
            error=error_message,
            tables=formatted_tables_info
        )
        
        messages = [
            Message(role="system", content=system_prompt),
            Message(role="user", content=user_prompt)
        ]
        
        # 调用LLM修复SQL
        response = llm_client.chat(messages)
        
        # 从响应中提取修复后的SQL和修复说明
        fixed_sql, explanation = _extract_sql(response.content)
        
        return fixed_sql, explanation
    except Exception as e:
        logger.error(f"修复SQL失败: {str(e)}", exc_info=True)
        return "", f"修复SQL时发生错误: {str(e)}" 