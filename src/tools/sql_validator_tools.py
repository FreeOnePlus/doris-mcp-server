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
        # 使用正则表达式提取表名
        # 模式匹配 FROM、JOIN、UPDATE、INSERT INTO 后面的表名
        # 同时处理带数据库前缀的表名 (db.table) 和不带前缀的表名
        table_patterns = [
            # FROM 子句
            r'FROM\s+([`"]?([a-zA-Z0-9_]+)\.)?([`"]?[a-zA-Z0-9_]+[`"]?)', 
            # JOIN 子句
            r'JOIN\s+([`"]?([a-zA-Z0-9_]+)\.)?([`"]?[a-zA-Z0-9_]+[`"]?)',
            # INSERT INTO 语句
            r'INSERT\s+INTO\s+([`"]?([a-zA-Z0-9_]+)\.)?([`"]?[a-zA-Z0-9_]+[`"]?)',
            # UPDATE 语句
            r'UPDATE\s+([`"]?([a-zA-Z0-9_]+)\.)?([`"]?[a-zA-Z0-9_]+[`"]?)'
        ]
        
        tables = set()
        
        # 移除SQL中的字符串字面量，以避免在字符串中提取到表名
        # 先替换单引号字符串
        no_strings_sql = re.sub(r"'[^']*'", "''", sql)
        # 再替换双引号字符串
        no_strings_sql = re.sub(r'"[^"]*"', '""', no_strings_sql)
        
        # 使用多个模式匹配表名
        for pattern in table_patterns:
            matches = re.finditer(pattern, no_strings_sql, re.IGNORECASE)
            for match in matches:
                if match.group(2) and match.group(3):  # 有数据库前缀
                    db_name = match.group(2)
                    table_name = match.group(3).strip('`"')
                    tables.add(f"{db_name}.{table_name}")
                elif match.group(3):  # 只有表名
                    table_name = match.group(3).strip('`"')
                    tables.add(table_name)
        
        # 处理子查询中的表名
        # 移除括号内容再进行匹配
        sub_queries = re.findall(r'\([^()]*\)', no_strings_sql)
        for subquery in sub_queries:
            # 递归调用自身提取子查询中的表名
            if len(subquery) > 2:  # 不处理空括号
                sub_tables = await _extract_tables_from_sql(subquery[1:-1])  # 移除括号
                tables.update(sub_tables)
        
        # 处理WITH语句中的公用表表达式(CTE)
        cte_pattern = r'WITH\s+([a-zA-Z0-9_]+)\s+AS\s*\('
        cte_matches = re.finditer(cte_pattern, no_strings_sql, re.IGNORECASE)
        for match in cte_matches:
            cte_name = match.group(1)
            tables.add(cte_name)  # 将CTE名称也作为表名
        
        # 如果未找到表名，可能是SQL格式不标准或是其他语句类型
        if not tables:
            # 使用更宽松的模式
            fallback_pattern = r'(?:FROM|JOIN|INTO|UPDATE)\s+([a-zA-Z0-9_\.]+)'
            fallback_matches = re.findall(fallback_pattern, no_strings_sql, re.IGNORECASE)
            tables.update(fallback_matches)
        
        return list(tables)
        
    except Exception as e:
        logger.error(f"提取表名失败: {str(e)}", exc_info=True)
        
        # 使用简单正则表达式作为最后的备用方法
        pattern = r'(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)'
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