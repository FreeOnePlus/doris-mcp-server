#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL验证工具

负责验证SQL语句的语法正确性和语义正确性，以及安全性检查等
"""

import os
import re
import json
import logging
from typing import List

# 获取日志记录器
logger = logging.getLogger("doris-mcp.sql-validator")

# 从环境变量获取数据库名称
DEFAULT_DB = os.getenv("DB_DATABASE", "")
DEFAULT_CHECK_TIMEOUT = 30  # 默认检查超时时间（秒）

async def _extract_tables_from_sql(sql: str) -> List[str]:
    """
    从SQL查询中提取表名
    
    Args:
        sql: SQL查询
        
    Returns:
        List[str]: 表名列表
    """
    try:
        # 使用LLM提取表名
        from src.utils.llm_client import get_llm_client, Message
        from src.prompts.prompts import SQL_VALIDATION_PROMPTS
        
        llm_client = get_llm_client()
        
        system_prompt = SQL_VALIDATION_PROMPTS["extract_tables_system"]
        user_prompt = SQL_VALIDATION_PROMPTS["extract_tables_user"].format(sql=sql)
        
        messages = [
            Message(role="system", content=system_prompt),
            Message(role="user", content=user_prompt)
        ]
        
        response = llm_client.chat(messages)
        
        # 从响应中提取表名
        tables = []
        
        try:
            # 尝试解析JSON格式响应
            tables_json_match = re.search(r'\{[\s\S]*\}', response.content)
            if tables_json_match:
                tables_json = json.loads(tables_json_match.group(0))
                if "tables" in tables_json and isinstance(tables_json["tables"], list):
                    return tables_json["tables"]
        except Exception as json_error:
            logger.warning(f"无法解析表名JSON: {str(json_error)}")
        
        # 如果JSON解析失败，直接按行解析
        table_pattern = r'[\-\*]\s+(\w+)'
        table_matches = re.findall(table_pattern, response.content)
        
        if table_matches:
            tables = table_matches
        else:
            # 尝试从行文本中提取
            lines = response.content.strip().split('\n')
            for line in lines:
                line = line.strip()
                if line and not line.startswith(('表名', '提取', '表格', 'Table', 'SQL')):
                    tables.append(line)
        
        return tables
        
    except Exception as e:
        logger.error(f"提取表名失败: {str(e)}", exc_info=True)
        
        # 使用正则表达式作为备用方法
        pattern = r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        return list(set(matches))

# 公共API函数
async def extract_tables_from_sql(sql: str, db_name: str = None) -> List[str]:
    """
    从SQL查询中提取表名的公共函数
    
    Args:
        sql: SQL查询
        db_name: 数据库名称（可选）
        
    Returns:
        List[str]: 表名列表
    """
    tables = await _extract_tables_from_sql(sql)
    
    # 如果提供了数据库名称，尝试添加完整表名
    if db_name and tables:
        # 检查表名是否已包含数据库前缀
        processed_tables = []
        for table in tables:
            if '.' not in table:
                processed_tables.append(f"{db_name}.{table}")
            else:
                processed_tables.append(table)
        return processed_tables
    
    return tables 