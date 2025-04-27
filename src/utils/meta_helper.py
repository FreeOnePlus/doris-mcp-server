#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
元数据辅助工具

提供元数据相关的辅助功能，如获取表结构等
"""

import os
import json
import logging
from typing import List, Dict, Any

# 获取日志记录器
logger = logging.getLogger("doris-mcp.meta-helper")

def get_tables_structure(tables: List[str], db_name: str = None) -> str:
    """
    获取表结构信息
    
    Args:
        tables: 表名列表
        db_name: 数据库名（可选）
        
    Returns:
        str: 表结构信息字符串
    """
    try:
        if not tables:
            return ""
        
        result = []
        
        for table in tables:
            # 处理表名
            if '.' in table:
                parts = table.split('.')
                current_db = parts[0]
                table_name = parts[1]
            else:
                current_db = db_name or os.getenv("DB_DATABASE", "")
                table_name = table
            
            # 查询表结构
            try:
                from src.utils.db import execute_query
                
                # 获取表结构
                columns_query = f"DESC {current_db}.{table_name}"
                columns_result = execute_query(columns_query)
                
                if not columns_result:
                    continue
                
                # 获取表注释
                comment_query = f"SHOW CREATE TABLE {current_db}.{table_name}"
                comment_result = execute_query(comment_query)
                table_comment = ""
                
                if comment_result and len(comment_result) > 0 and len(comment_result[0]) > 1:
                    create_stmt = comment_result[0][1]
                    import re
                    comment_match = re.search(r"COMMENT\s+'([^']*)'", create_stmt)
                    if comment_match:
                        table_comment = comment_match.group(1)
                
                # 构建表结构信息
                table_info = f"表名: {current_db}.{table_name}\n"
                if table_comment:
                    table_info += f"表注释: {table_comment}\n"
                
                table_info += "字段列表:\n"
                
                for col in columns_result:
                    col_name = col[0]
                    col_type = col[1]
                    col_null = "可空" if col[2].upper() == "YES" else "非空"
                    col_key = col[3] if col[3] else ""
                    col_default = f"默认值: {col[4]}" if col[4] else ""
                    col_extra = col[5] if col[5] else ""
                    
                    table_info += f"  - {col_name} ({col_type}, {col_null}"
                    if col_key:
                        table_info += f", {col_key}"
                    if col_default:
                        table_info += f", {col_default}"
                    if col_extra:
                        table_info += f", {col_extra}"
                    table_info += ")\n"
                
                result.append(table_info)
                
            except Exception as e:
                logger.warning(f"获取表 {current_db}.{table_name} 结构失败: {str(e)}")
                continue
        
        return "\n".join(result)
        
    except Exception as e:
        logger.error(f"获取表结构信息失败: {str(e)}")
        return "" 