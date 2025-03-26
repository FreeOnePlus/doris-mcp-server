#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
模式和元数据资源处理函数
"""

import os
import logging
import json
from typing import Dict, Any, Optional

# 导入NL2SQL服务以获取元数据
from src.nl2sql_service import NL2SQLService

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 初始化NL2SQL服务
service = NL2SQLService()

def get_schema(database: str, table: str) -> str:
    """
    获取表的结构信息
    
    Args:
        database: 数据库名称
        table: 表名称
    
    Returns:
        str: 表结构信息（HTML格式）
    """
    try:
        # 如果数据库名为空，使用默认数据库
        if not database:
            database = service.db_name
        
        # 获取表结构
        schema = service.metadata_extractor.get_table_schema(table, database)
        
        if not schema:
            logger.warning(f"表不存在: {database}.{table}")
            return f"<h1>404 - 表不存在</h1><p>找不到表 '{database}.{table}'</p>"
        
        # 构建HTML表格
        html = f"<h1>表 {database}.{table} 的结构</h1>"
        
        # 表注释
        if schema.get('table_comment'):
            html += f"<p><strong>表说明:</strong> {schema.get('table_comment')}</p>"
        
        # 列信息
        html += "<h2>列信息</h2>"
        html += "<table border='1'>"
        html += "<tr><th>列名</th><th>类型</th><th>可空</th><th>主键</th><th>默认值</th><th>说明</th></tr>"
        
        for column in schema.get('columns', []):
            html += "<tr>"
            html += f"<td>{column.get('name', '')}</td>"
            html += f"<td>{column.get('type', '')}</td>"
            html += f"<td>{'是' if column.get('nullable') else '否'}</td>"
            html += f"<td>{'是' if column.get('primary_key') else '否'}</td>"
            html += f"<td>{column.get('default', '')}</td>"
            html += f"<td>{column.get('comment', '')}</td>"
            html += "</tr>"
        
        html += "</table>"
        
        # 添加索引信息（如果有）
        if schema.get('indexes'):
            html += "<h2>索引信息</h2>"
            html += "<table border='1'>"
            html += "<tr><th>索引名</th><th>列</th><th>类型</th></tr>"
            
            for index in schema.get('indexes', []):
                html += "<tr>"
                html += f"<td>{index.get('name', '')}</td>"
                html += f"<td>{', '.join(index.get('columns', []))}</td>"
                html += f"<td>{index.get('type', '')}</td>"
                html += "</tr>"
            
            html += "</table>"
        
        logger.info(f"成功获取表结构: {database}.{table}")
        return html
    except Exception as e:
        logger.error(f"获取表结构时出错: {str(e)}")
        return f"<h1>Error</h1><p>获取表结构时出错: {str(e)}</p>"

def get_metadata(database: str, table: str) -> str:
    """
    获取表的元数据和业务含义
    
    Args:
        database: 数据库名称
        table: 表名称
    
    Returns:
        str: 表元数据信息（HTML格式）
    """
    try:
        # 如果数据库名为空，使用默认数据库
        if not database:
            database = service.db_name
        
        # 获取表结构详细信息
        result = service.explain_table(table)
        
        if not result.get('success', False):
            logger.warning(f"表不存在或元数据不可用: {database}.{table}")
            return f"<h1>404 - 表元数据不可用</h1><p>{result.get('message', '')}</p>"
        
        # 构建HTML内容
        html = f"<h1>表 {database}.{table} 的元数据</h1>"
        
        # 表说明
        if result.get('table_comment'):
            html += f"<p><strong>表说明:</strong> {result.get('table_comment')}</p>"
        
        # 业务描述
        if result.get('business_description'):
            html += f"<h2>业务含义</h2>"
            html += f"<p>{result.get('business_description')}</p>"
        
        # 列信息和业务含义
        html += "<h2>列信息和业务含义</h2>"
        html += "<table border='1'>"
        html += "<tr><th>列名</th><th>类型</th><th>业务含义</th></tr>"
        
        for column in result.get('columns', []):
            html += "<tr>"
            html += f"<td>{column.get('name', '')}</td>"
            html += f"<td>{column.get('type', '')}</td>"
            html += f"<td>{column.get('comment', '')}</td>"
            html += "</tr>"
        
        html += "</table>"
        
        # 表关系
        if result.get('relationships'):
            html += "<h2>表关系</h2>"
            html += "<ul>"
            
            for relation in result.get('relationships', []):
                if relation.get('table') == table:
                    html += f"<li>字段 <strong>{relation.get('column')}</strong> 引用 <strong>{relation.get('references_table')}.{relation.get('references_column')}</strong></li>"
                else:
                    html += f"<li>被 <strong>{relation.get('table')}.{relation.get('column')}</strong> 引用</li>"
            
            html += "</ul>"
        
        logger.info(f"成功获取表元数据: {database}.{table}")
        return html
    except Exception as e:
        logger.error(f"获取表元数据时出错: {str(e)}")
        return f"<h1>Error</h1><p>获取表元数据时出错: {str(e)}</p>" 