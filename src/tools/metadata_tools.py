#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
元数据管理工具

负责数据库元数据的加载、刷新和查询功能
"""

import os
import time
import json
import logging
import traceback
from typing import Dict, Any, List, Optional

# 导入数据库查询函数
from src.utils.db import execute_query

# 获取日志记录器
logger = logging.getLogger("doris-mcp.metadata-tools")

async def refresh_metadata(ctx) -> Dict[str, Any]:
    """
    刷新数据库元数据
    
    Args:
        ctx: Context对象，包含请求参数
        
    Returns:
        Dict[str, Any]: 刷新结果
    """
    try:
        force = ctx.params.get("force_refresh", False)
        db_name = ctx.params.get("db_name", os.getenv("DB_DATABASE", ""))
        
        # 导入元数据提取器
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        
        # 获取所有目标数据库
        if db_name:
            # 如果指定了数据库，只刷新该数据库
            target_databases = [db_name]
        else:
            # 否则获取所有目标数据库
            target_databases = extractor.get_all_target_databases()
            
        if not target_databases:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "未获取到任何目标数据库",
                            "message": "请检查数据库配置",
                            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 构建需要客户端执行的刷新流程说明
        refresh_instructions = {
            "success": True,
            "message": "元数据刷新流程已启动，请按照以下步骤在客户端执行",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "refresh_flow": {
                "target_databases": target_databases,
                "force_refresh": force,
                "steps": [
                    {
                        "step": 1,
                        "action": "获取数据库结构信息",
                        "tool": "get_schema_list",
                        "params": {
                            "db_name": "${db_name}"  # 使用模板变量，客户端需要替换
                        }
                    },
                    {
                        "step": 2,
                        "action": "保存业务领域元数据",
                        "tool": "save_metadata",
                        "params": {
                            "db_name": "${db_name}",
                            "metadata_type": "business_summary",
                            "metadata": "${business_metadata}"  # 客户端需要替换为生成的元数据
                        }
                    },
                    {
                        "step": 3,
                        "action": "获取每个表的结构信息并保存表元数据",
                        "description": "循环处理数据库中的每个表",
                        "sub_steps": [
                            {
                                "step": 3.1,
                                "action": "获取表结构信息",
                                "tool": "get_schema_list",
                                "params": {
                                    "db_name": "${db_name}",
                                    "table_name": "${table_name}"  # 客户端需要替换
                                }
                            },
                            {
                                "step": 3.2,
                                "action": "保存表元数据",
                                "tool": "save_metadata",
                                "params": {
                                    "db_name": "${db_name}",
                                    "table_name": "${table_name}",
                                    "metadata_type": "table_summary",
                                    "metadata": "${table_metadata}"  # 客户端需要替换为生成的元数据
                                }
                            }
                        ]
                    }
                ]
            }
        }
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(refresh_instructions, ensure_ascii=False)
                }
            ]
        }
    except Exception as e:
        logger.error(f"准备元数据刷新流程失败: {str(e)}")
        logger.error(traceback.format_exc())
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

async def get_table_info(ctx) -> Dict[str, Any]:
    """
    获取表结构信息
    
    Args:
        ctx: Context对象，包含请求参数
        
    Returns:
        Dict[str, Any]: 表结构信息
    """
    try:
        table_name = ctx.params.get("table_name")
        db_name = ctx.params.get("db_name", os.getenv("DB_DATABASE", ""))
        
        if not table_name:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少表名参数",
                            "message": "请提供表名"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 处理跨库表名格式 (db_name.table_name)
        if table_name and "." in table_name:
            parts = table_name.split(".", 1)
            if len(parts) == 2:
                extracted_db_name, extracted_table_name = parts
                # 如果参数中没有显式指定db_name，或者指定的db_name与表名中提取的不同，则使用表名中提取的db_name
                if not db_name or db_name != extracted_db_name:
                    logger.info(f"从表名 {table_name} 中提取数据库名 {extracted_db_name}")
                    db_name = extracted_db_name
                    table_name = extracted_table_name
        
        # 导入元数据提取器
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        
        # 获取表结构
        schema = extractor.get_table_schema(table_name, db_name)
        
        if not schema:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "message": f"未找到表 {db_name}.{table_name}"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 获取表关系
        relationships = extractor.get_table_relationships()
        table_relations = [r for r in relationships if r.get('table') == table_name or r.get('references_table') == table_name]
        
        # 获取业务元数据
        business_metadata = extractor.summarize_business_metadata(db_name)
        
        # 查找表的业务描述
        table_description = ""
        if business_metadata and 'tables_summary' in business_metadata:
            for table_info in business_metadata['tables_summary']:
                if table_info.get('name') == table_name:
                    table_description = table_info.get('description', '')
                    break
        
        # 构建结果
        result = {
            "success": True,
            "table_name": table_name,
            "db_name": db_name,
            "table_comment": schema.get("table_comment", ""),
            "business_description": table_description,
            "columns": schema.get("columns", []),
            "relationships": table_relations
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
        logger.error(f"获取表信息失败: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "获取表信息时出错",
                        "table_name": ctx.params.get("table_name", "")
                    }, ensure_ascii=False)
                }
            ]
        }

async def get_business_overview(ctx) -> Dict[str, Any]:
    """
    获取业务概览
    
    Args:
        ctx: Context对象，包含请求参数
        
    Returns:
        Dict[str, Any]: 业务概览信息或生成业务概览的提示词
    """
    try:
        db_name = ctx.params.get("db_name", os.getenv("DB_DATABASE", ""))
        client_mode = ctx.params.get("client_mode", True)  # 默认使用客户端模式
        
        # 导入元数据提取器
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        
        # 导入提示词模板
        from src.prompts.prompts import CLIENT_METADATA_PROMPT
        
        # 先从元数据库中查询业务概览信息
        business_metadata = extractor.get_business_metadata_from_database(db_name)
        
        # 获取表数量和表信息
        tables = extractor.get_database_tables(db_name)
        table_count = len(tables)
        
        # 收集所有表的结构信息用于生成提示词
        tables_info = []
        formatted_tables_info = ""
        
        for table_name in tables:
            schema = extractor.get_table_schema(table_name, db_name)
            if schema:
                # 构建JSON对象用于返回给客户端
                table_info = {
                    "name": table_name,
                    "comment": schema.get("table_comment", ""),
                    "columns": [
                        {
                            "name": col.get("name", ""),
                            "type": col.get("type", ""),
                            "comment": col.get("comment", "")
                        }
                        for col in schema.get("columns", [])
                    ]
                }
                tables_info.append(table_info)
                
                # 构建格式化文本用于提示词
                formatted_tables_info += f"\n表名: {table_name}\n"
                formatted_tables_info += f"表说明: {schema.get('table_comment', '')}\n"
                formatted_tables_info += "字段:\n"
                
                for col in schema.get("columns", []):
                    formatted_tables_info += f"  - {col.get('name', '')} ({col.get('type', '')}): {col.get('comment', '')}\n"
                
                formatted_tables_info += "\n"
        
        # 如果元数据库中存在业务概览信息，则直接返回
        if business_metadata and isinstance(business_metadata, dict) and 'business_domain' in business_metadata:
            result = {
                "success": True,
                "database": db_name,
                "source": "metadata_db",
                "table_count": table_count,
                "business_domain": business_metadata.get("business_domain", "未知业务领域"),
                "core_entities": business_metadata.get("core_entities", []),
                "business_processes": business_metadata.get("business_processes", [])
            }
        else:
            # 使用提示词模板
            prompt = CLIENT_METADATA_PROMPT["business_overview"].format(
                db_name=db_name,
                table_count=table_count,
                tables_info=formatted_tables_info
            )
            
            # 返回提示词和表结构信息
            result = {
                "success": True,
                "database": db_name,
                "source": "client_prompt",
                "table_count": table_count,
                "tables_info": tables_info,
                "prompt": prompt,
                "metadata_type": "business_summary"
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
        logger.error(f"获取业务概览失败: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "获取业务概览时出错"
                    }, ensure_ascii=False)
                }
            ]
        }

async def save_metadata(ctx) -> Dict[str, Any]:
    """
    保存元数据到数据库
    
    Args:
        ctx: Context对象，包含请求参数
        - db_name: 数据库名称
        - metadata: 元数据JSON字符串或字典
        - metadata_type: 元数据类型，例如'business_summary'、'table_summary'等，如果未提供则根据table_name自动确定
        - table_name: 表名（可选，默认为空字符串，表示数据库级元数据）
        
    Returns:
        Dict[str, Any]: 保存结果
    """
    try:
        db_name = ctx.params.get("db_name", os.getenv("DB_DATABASE", ""))
        metadata = ctx.params.get("metadata", {})
        table_name = ctx.params.get("table_name", "")
        
        # 根据是否提供table_name自动确定metadata_type
        metadata_type = ctx.params.get("metadata_type")
        if not metadata_type:
            metadata_type = "table_summary" if table_name else "business_summary"
        
        # 如果没有提供元数据或元数据为空，返回错误
        if not metadata:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "未提供元数据",
                            "message": "请提供有效的元数据内容"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 如果metadata是字符串，尝试解析为JSON
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError as e:
                logger.error(f"解析元数据JSON失败: {str(e)}")
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": False,
                                "error": f"元数据JSON解析失败: {str(e)}",
                                "message": "请提供有效的JSON格式元数据"
                            }, ensure_ascii=False)
                        }
                    ]
                }
        
        # 对元数据进行基本验证
        if metadata_type == "table_summary" and table_name:
            # 验证表级元数据
            required_fields = ["table_name", "business_description"]
            missing_fields = [field for field in required_fields if field not in metadata]
            
            if missing_fields:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": False,
                                "error": f"元数据缺少必要字段: {', '.join(missing_fields)}",
                                "message": "请提供完整的表元数据信息"
                            }, ensure_ascii=False)
                        }
                    ]
                }
            
            # 确保表名一致
            if metadata.get("table_name") != table_name:
                logger.warning(f"元数据中的表名 '{metadata.get('table_name')}' 与参数表名 '{table_name}' 不一致，使用参数表名")
                metadata["table_name"] = table_name
                
        elif metadata_type == "business_summary":
            # 验证数据库级元数据
            required_fields = ["business_domain"]
            missing_fields = [field for field in required_fields if field not in metadata]
            
            if missing_fields:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": False,
                                "error": f"元数据缺少必要字段: {', '.join(missing_fields)}",
                                "message": "请提供完整的数据库业务元数据信息"
                            }, ensure_ascii=False)
                        }
                    ]
                }
        
        # 导入元数据模式定义
        from src.prompts.metadata_schema import METADATA_DB_NAME, METADATA_TABLES, CREATE_DATABASE_SQL
        
        # 添加时间戳
        if "timestamp" not in metadata:
            metadata["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
        # 添加版本和来源信息
        if "metadata_version" not in metadata:
            metadata["metadata_version"] = "1.0"
            
        if "source" not in metadata:
            metadata["source"] = "client_generated"
        
        # 将元数据转换为JSON字符串
        metadata_json = json.dumps(metadata, ensure_ascii=False)
        
        # 确保元数据值使用UTF-8编码
        def ensure_utf8_encoding(text: str) -> str:
            """确保文本使用UTF-8编码"""
            try:
                if isinstance(text, bytes):
                    return text.decode('utf-8')
                elif isinstance(text, str):
                    return text.encode('utf-8').decode('utf-8')
                else:
                    return str(text)
            except Exception as e:
                logger.warning(f"转换UTF-8编码时出错: {str(e)}")
                return str(text)
        
        metadata_json = ensure_utf8_encoding(metadata_json)
        
        # 记录保存的元数据内容
        logger.info(f"正在保存元数据: 类型={metadata_type}, 数据库={db_name}, 表={table_name or '无'}")
        logger.debug(f"元数据内容: {metadata_json[:200]}...")  # 仅记录前200个字符避免日志过大
        
        # 保存元数据
        try:
            # 首先检查doris_metadata数据库是否存在,如果不存在则创建
            check_db_query = f"SHOW DATABASES LIKE '{METADATA_DB_NAME}'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                execute_query(CREATE_DATABASE_SQL)
                logger.info(f"已创建元数据库 {METADATA_DB_NAME}")
            
            # 检查business_metadata表是否存在
            check_table_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'business_metadata'"
            table_exists = execute_query(check_table_query)
            
            if not table_exists:
                # 创建表
                execute_query(METADATA_TABLES["business_metadata"], METADATA_DB_NAME)
                logger.info("已创建business_metadata表")
            
            # 转义元数据值中的单引号,防止SQL注入
            safe_metadata_json = metadata_json.replace("'", "''")
            
            # 根据metadata_type设置business_keywords值
            business_keywords_value = "NULL"
            if metadata_type == "business_keywords":
                business_keywords_value = f"'{safe_metadata_json}'"
            
            # 插入或更新元数据,包含business_keywords列
            upsert_query = f"""
            INSERT INTO `{METADATA_DB_NAME}`.`business_metadata` 
            (`db_name`, `table_name`, `metadata_type`, `metadata_value`, `business_keywords`, `update_time`)
            VALUES
            ('{db_name}', '{table_name or ""}', '{metadata_type}', '{safe_metadata_json}', {business_keywords_value}, NOW())
            """
            
            execute_query(upsert_query, METADATA_DB_NAME)
            
            logger.info(f"已保存数据库 {db_name} 表 {table_name or '无'} 的 {metadata_type} 元数据")
            
            # 如果是表级元数据，也更新table_metadata表
            if metadata_type == "table_summary" and table_name:
                # 检查表是否存在
                check_table_meta_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'table_metadata'"
                table_meta_exists = execute_query(check_table_meta_query)
                
                if not table_meta_exists:
                    # 创建table_metadata表
                    execute_query(METADATA_TABLES["table_metadata"], METADATA_DB_NAME)
                    logger.info("已创建table_metadata表")
                
                # 提取表业务描述
                business_summary = metadata.get("business_description", "")
                
                # 转义JSON字符串中的单引号
                safe_business_summary = json.dumps({"business_description": business_summary}, ensure_ascii=False).replace("'", "''")
                
                # 插入或更新记录
                update_table_meta_query = f"""
                INSERT INTO `{METADATA_DB_NAME}`.`table_metadata`
                (`database_name`, `table_name`, `business_summary`, `update_time`)
                VALUES
                ('{db_name}', '{table_name}', '{safe_business_summary}', NOW())
                """
                
                execute_query(update_table_meta_query, METADATA_DB_NAME)
                logger.info(f"已更新表 {db_name}.{table_name} 的业务描述到table_metadata表")
            
            # 返回保存成功信息
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": True,
                            "message": f"元数据已成功保存到 {db_name}{f'.{table_name}' if table_name else ''}",
                            "metadata_type": metadata_type,
                            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                        }, ensure_ascii=False)
                    }
                ]
            }
        except Exception as e:
            logger.error(f"保存元数据到数据库时出错: {str(e)}")
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": str(e),
                            "message": "保存元数据到数据库时出错",
                            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                        }, ensure_ascii=False)
                    }
                ]
            }
    except Exception as e:
        logger.error(f"保存元数据失败: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "保存元数据时出错",
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        }

async def get_schema_list(ctx) -> Dict[str, Any]:
    """
    获取数据库或表结构信息，用于Client端生成元数据
    
    Args:
        ctx: Context对象，包含请求参数
        - db_name: 数据库名称
        - table_name: 表名（可选，如果提供则只返回该表的结构）
        - simple_mode: 是否使用简化模式（只返回表列表，不包含提示信息）
        
    Returns:
        Dict[str, Any]: 数据库或表结构信息和提示词
    """
    try:
        db_name = ctx.params.get("db_name", os.getenv("DB_DATABASE", ""))
        table_name = ctx.params.get("table_name", "")
        simple_mode = ctx.params.get("simple_mode", False)
        
        # 处理跨库表名格式 (db_name.table_name)
        if table_name and "." in table_name:
            parts = table_name.split(".", 1)
            if len(parts) == 2:
                extracted_db_name, extracted_table_name = parts
                # 如果参数中没有显式指定db_name，或者指定的db_name与表名中提取的不同，则使用表名中提取的db_name
                if not db_name or db_name != extracted_db_name:
                    logger.info(f"从表名 {table_name} 中提取数据库名 {extracted_db_name}")
                    db_name = extracted_db_name
                    table_name = extracted_table_name
        
        # 导入元数据提取器
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        
        # 如果未指定simple_mode或非简化模式，导入提示词模板
        if not simple_mode:
            # 导入提示词模板
            from src.prompts.prompts import CLIENT_METADATA_PROMPT
        
        if table_name:
            # 获取单个表的结构
            schema = extractor.get_table_schema(table_name, db_name)
            
            if not schema:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": False,
                                "error": f"表 {db_name}.{table_name} 不存在或无法获取结构",
                                "message": "请检查表名是否正确"
                            }, ensure_ascii=False)
                        }
                    ]
                }
            
            # 获取表关系
            relationships = extractor.get_table_relationships()
            related_tables = []
            
            # 查找关联到此表的关系
            for rel in relationships:
                if rel.get('table') == table_name:
                    related_tables.append({
                        "type": "references",
                        "table": rel.get('references_table', ''),
                        "this_column": rel.get('column', ''),
                        "referenced_column": rel.get('references_column', '')
                    })
                elif rel.get('references_table') == table_name:
                    related_tables.append({
                        "type": "referenced_by",
                        "table": rel.get('table', ''),
                        "this_column": rel.get('references_column', ''),
                        "referenced_column": rel.get('column', '')
                    })
            
            # 创建详细的表结构对象
            detailed_schema = {
                "name": schema.get('name', table_name),
                "comment": schema.get('comment', ''),
                "columns": schema.get('columns', []),
                "relationships": related_tables
            }
            
            # 如果是简化模式，直接返回表结构
            if simple_mode:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": True,
                                "db_name": db_name,
                                "table_name": table_name,
                                "schema": detailed_schema
                            }, ensure_ascii=False, default=str)
                        }
                    ]
                }
                
            # 以下是非简化模式的处理逻辑
            # 准备字段信息和关系信息的格式化文本
            columns_info = ""
            for column in schema.get('columns', []):
                col_name = column.get('name', '')
                col_type = column.get('type', '')
                col_comment = column.get('comment', '')
                col_key = column.get('key', '')
                col_nullable = "可为空" if column.get('nullable', True) else "非空"
                
                columns_info += f"  - {col_name} ({col_type}, {col_nullable}{', ' + col_key if col_key else ''}): {col_comment}\n"
            
            # 添加表关系信息
            relationships_info = ""
            if related_tables:
                for rel in related_tables:
                    if rel['type'] == 'references':
                        relationships_info += f"  - {table_name}.{rel['this_column']} 引用 {rel['table']}.{rel['referenced_column']}\n"
                    else:
                        relationships_info += f"  - {rel['table']}.{rel['referenced_column']} 引用 {table_name}.{rel['this_column']}\n"
            
            # 使用提示词模板
            prompt = CLIENT_METADATA_PROMPT["table_metadata"].format(
                db_name=db_name,
                table_name=table_name,
                table_comment=schema.get('comment', '无注释'),
                columns_info=columns_info,
                relationships_info=relationships_info
            )
            
            # 检查是否已存在表元数据
            existing_metadata = None
            try:
                # 导入元数据模式定义
                from src.prompts.metadata_schema import METADATA_DB_NAME
                
                # 首先从business_metadata表查询
                business_metadata_query = f"""
                SELECT metadata_value
                FROM `{METADATA_DB_NAME}`.`business_metadata`
                WHERE db_name = '{db_name}' 
                AND table_name = '{table_name}'
                AND metadata_type = 'table_summary'
                ORDER BY update_time DESC
                LIMIT 1
                """
                business_metadata_result = execute_query(business_metadata_query)
                
                # 如果在business_metadata表中找到
                if business_metadata_result and len(business_metadata_result) > 0:
                    metadata_value = business_metadata_result[0].get("metadata_value")
                    if metadata_value:
                        try:
                            existing_metadata = json.loads(metadata_value)
                            logger.info(f"从business_metadata表获取到表 {table_name} 的元数据")
                        except json.JSONDecodeError:
                            existing_metadata = None
                
                # 如果在business_metadata表中没找到，查询table_metadata表
                if not existing_metadata:
                    table_metadata_query = f"""
                    SELECT business_summary
                    FROM `{METADATA_DB_NAME}`.`table_metadata`
                    WHERE database_name = '{db_name}' 
                    AND table_name = '{table_name}'
                    ORDER BY update_time DESC
                    LIMIT 1
                    """
                    table_metadata_result = execute_query(table_metadata_query)
                    
                    if table_metadata_result and len(table_metadata_result) > 0:
                        business_summary = table_metadata_result[0].get("business_summary")
                        if business_summary:
                            try:
                                summary_obj = json.loads(business_summary)
                                # 构建完整元数据对象
                                existing_metadata = {
                                    "table_name": table_name,
                                    "business_description": summary_obj.get("business_description", ""),
                                    "core_fields": [],
                                    "relationships": related_tables
                                }
                                logger.info(f"从table_metadata表获取到表 {table_name} 的业务摘要")
                            except json.JSONDecodeError:
                                existing_metadata = None
            except Exception as e:
                logger.warning(f"获取表 {table_name} 元数据时出错: {str(e)}")
                existing_metadata = None
            
            # 返回表结构、关系和提示词
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": True,
                            "db_name": db_name,
                            "table_name": table_name,
                            "schema": detailed_schema,
                            "existing_metadata": existing_metadata,
                            "has_existing_metadata": existing_metadata is not None and len(existing_metadata) > 0,
                            "prompt": prompt,
                            "metadata_type": "table_summary"
                        }, ensure_ascii=False, default=str)
                    }
                ]
            }
        else:
            # 获取数据库所有表的结构
            tables = extractor.get_database_tables(db_name)
            
            if not tables:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": False,
                                "error": f"数据库 {db_name} 不存在或没有表",
                                "message": "请检查数据库名称是否正确"
                            }, ensure_ascii=False)
                        }
                    ]
                }
            
            # 收集所有表的结构信息
            tables_info = []
            for table_name in tables:
                schema = extractor.get_table_schema(table_name, db_name)
                if schema:
                    # 表信息对象
                    table_info = {
                        "name": table_name,
                        "comment": schema.get("table_comment", ""),
                        "column_count": len(schema.get("columns", []))
                    }
                    
                    # 只有在非简化模式下才添加has_metadata标志
                    if not simple_mode:
                        # 检查是否有元数据
                        has_metadata = False
                        try:
                            # 导入元数据模式定义
                            from src.prompts.metadata_schema import METADATA_DB_NAME
                            # 查询是否有元数据
                            metadata_query = f"""
                            SELECT COUNT(*) as count 
                            FROM `{METADATA_DB_NAME}`.`business_metadata`
                            WHERE db_name = '{db_name}' 
                            AND table_name = '{table_name}'
                            AND metadata_type = 'table_summary'
                            """
                            metadata_result = execute_query(metadata_query)
                            if metadata_result and metadata_result[0].get("count", 0) > 0:
                                has_metadata = True
                        except Exception:
                            has_metadata = False
                        
                        table_info["has_metadata"] = has_metadata
                    
                    tables_info.append(table_info)
            
            # 构建数据库结构对象
            db_structure = {
                "success": True,
                "db_name": db_name,
                "tables": tables_info,
                "table_count": len(tables)
            }
            
            # 如果不是简化模式，添加提示消息
            if not simple_mode:
                db_structure["message"] = "请选择一个表以获取其结构详情和元数据生成提示"
                
                # 准备所有表的格式化文本用于生成提示词
                formatted_tables_info = ""
                for table_info in tables_info:
                    formatted_tables_info += f"- {table_info['name']} ({table_info['column_count']} 列)\n"
                
                # 使用提示词模板
                prompt = CLIENT_METADATA_PROMPT["database_metadata"].format(
                    db_name=db_name,
                    table_count=len(tables),
                    tables_info=formatted_tables_info
                )
                
                # 返回数据库结构信息和提示词
                db_structure["prompt"] = prompt
            
            # 返回结果
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(db_structure, ensure_ascii=False)
                    }
                ]
            }
    except Exception as e:
        logger.error(f"获取结构信息失败: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "获取结构信息时出错"
                    }, ensure_ascii=False)
                }
            ]
        }

async def get_db_metadata(ctx) -> Dict[str, Any]:
    """
    获取数据库元数据信息
    
    Args:
        ctx: Context对象，包含请求参数
        
    Returns:
        Dict[str, Any]: 数据库元数据信息
    """
    try:
        db_name = ctx.params.get("db_name", os.getenv("DB_DATABASE", ""))
        include_structure = ctx.params.get("include_structure", True)
        
        # 导入元数据提取器
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        
        # 从元数据库中获取数据库业务概览元数据
        business_metadata = extractor.get_business_metadata_from_database(db_name)
        
        # 获取表列表
        tables = extractor.get_database_tables(db_name)
        table_count = len(tables)
        
        # 构建结果
        result = {
            "success": True,
            "db_name": db_name,
            "table_count": table_count,
            "business_metadata": business_metadata if business_metadata else {},
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 如果需要包含表结构信息
        if include_structure:
            tables_info = []
            for table_name in tables:
                schema = extractor.get_table_schema(table_name, db_name)
                if schema:
                    # 尝试获取表元数据
                    table_metadata = None
                    try:
                        metadata_db = "doris_metadata"  # 默认元数据库名称
                        query = f"""
                        SELECT metadata_value
                        FROM {metadata_db}.table_metadata
                        WHERE db_name = '{db_name}' AND table_name = '{table_name}'
                        ORDER BY created_time DESC
                        LIMIT 1
                        """
                        metadata_result = execute_query(query)
                        if metadata_result and len(metadata_result) > 0:
                            metadata_value = metadata_result[0].get("metadata_value")
                            if metadata_value:
                                try:
                                    table_metadata = json.loads(metadata_value)
                                except json.JSONDecodeError:
                                    table_metadata = None
                    except Exception as e:
                        logger.warning(f"获取表 {table_name} 元数据时出错: {str(e)}")
                        table_metadata = None
                    
                    tables_info.append({
                        "name": table_name,
                        "comment": schema.get("table_comment", ""),
                        "column_count": len(schema.get("columns", [])),
                        "has_metadata": table_metadata is not None,
                        "metadata": table_metadata
                    })
            
            result["tables"] = tables_info
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, ensure_ascii=False, default=str)
                }
            ]
        }
    except Exception as e:
        logger.error(f"获取数据库元数据失败: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "获取数据库元数据时出错",
                        "db_name": db_name if 'db_name' in locals() else None,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, ensure_ascii=False)
                }
            ]
        }

async def get_business_overview_data(db_name: str = None) -> Dict:
    """
    获取数据库业务概览数据，不包含 content 包装
    
    Args:
        db_name: 数据库名，默认为当前数据库
        
    Returns:
        Dict: 业务概览数据
    """
    try:
        # 使用环境变量默认数据库（如果未指定）
        if not db_name:
            db_name = os.getenv("DB_DATABASE", "")
        
        # 从元数据表中直接查询业务概览
        from src.prompts.metadata_schema import METADATA_DB_NAME
        try:
            # 查询业务概览元数据
            query = f"""
            SELECT metadata_value
            FROM `{METADATA_DB_NAME}`.`business_metadata`
            WHERE db_name = '{db_name}'
            AND table_name = ''
            AND metadata_type = 'business_summary'
            ORDER BY update_time DESC
            LIMIT 1
            """
            result = execute_query(query)
            
            if result and len(result) > 0:
                metadata_value = result[0].get("metadata_value")
                if metadata_value:
                    try:
                        business_overview = json.loads(metadata_value)
                        logger.info(f"从元数据表中获取到业务概览: {db_name}")
                        return business_overview
                    except json.JSONDecodeError:
                        logger.warning(f"解析业务概览JSON失败: {metadata_value[:100]}...")
            
            logger.info("未在元数据表中找到业务概览，将生成默认概览")
        except Exception as e:
            logger.warning(f"从元数据表查询业务概览失败: {str(e)}")
        
        # 如果元数据存储中没有，则生成业务概览
        logger.info(f"生成业务概览: {db_name}")
        
        # 获取所有表
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        tables = extractor.get_database_tables(db_name)
        
        # 默认业务概览
        default_overview = {
            "business_domain": f"{db_name} 数据库",
            "core_entities": [],
            "business_processes": [],
            "source": "default",
            "table_count": len(tables)
        }
        
        # 根据表名生成一些核心实体（简单示例）
        for table in tables[:min(5, len(tables))]:
            default_overview["core_entities"].append({
                "name": table,
                "description": f"{table} 相关数据"
            })
        
        return default_overview
        
    except Exception as e:
        logger.error(f"获取业务概览数据失败: {str(e)}")
        return {
            "business_domain": "未知",
            "core_entities": [],
            "business_processes": [],
            "source": "error",
            "error": str(e)
        }

async def get_metadata(ctx) -> Dict[str, Any]:
    """
    获取元数据信息，可以是数据库元数据或表元数据
    
    Args:
        ctx: Context对象，包含请求参数
        - db_name: 数据库名称（可选）
        - table_name: 表名（可选，如果提供则返回该表的元数据）
        
    Returns:
        Dict[str, Any]: 元数据信息和状态
    """
    try:
        db_name = ctx.params.get("db_name", os.getenv("DB_DATABASE", ""))
        table_name = ctx.params.get("table_name", "")
        
        # 导入元数据提取器
        from src.utils.metadata_extractor import MetadataExtractor
        extractor = MetadataExtractor(db_name)
        
        logger.info(f"获取元数据: db_name={db_name}, table_name={table_name}")
        
        if table_name:
            # 获取表级别元数据
            logger.info(f"获取表元数据: {table_name}")
            
            # 获取表结构
            schema = extractor.get_table_schema(table_name, db_name)
            
            if not schema:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": False,
                                "error": f"表 {table_name} 不存在或无法获取结构",
                                "message": "请检查表名是否正确"
                            }, ensure_ascii=False)
                        }
                    ]
                }
            
            # 获取表业务元数据
            table_metadata = extractor.get_business_metadata_for_table(db_name, table_name)
            
            # 获取表关系
            relationships = extractor.get_table_relationships()
            related_tables = []
            
            # 查找关联到此表的关系
            for rel in relationships:
                if rel.get('table') == table_name:
                    related_tables.append({
                        "type": "references",
                        "table": rel.get('references_table', ''),
                        "this_column": rel.get('column', ''),
                        "referenced_column": rel.get('references_column', '')
                    })
                elif rel.get('references_table') == table_name:
                    related_tables.append({
                        "type": "referenced_by",
                        "table": rel.get('table', ''),
                        "this_column": rel.get('references_column', ''),
                        "referenced_column": rel.get('column', '')
                    })
            
            # 创建详细的表结构和元数据对象
            metadata_result = {
                "schema": {
                    "name": schema.get('name', table_name),
                    "comment": schema.get('comment', ''),
                    "columns": schema.get('columns', []),
                    "relationships": related_tables
                },
                "business_metadata": table_metadata
            }
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": True,
                            "db_name": db_name,
                            "table_name": table_name,
                            "metadata": metadata_result,
                            "metadata_type": "table_metadata"
                        }, ensure_ascii=False, default=str)
                    }
                ]
            }
        else:
            # 获取数据库级别元数据
            logger.info(f"获取数据库元数据: {db_name}")
            
            # 获取数据库业务元数据
            db_metadata = extractor.get_business_metadata_from_database(db_name)
            
            # 获取数据库所有表
            tables = extractor.get_database_tables(db_name)
            
            if not tables:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": False,
                                "error": f"数据库 {db_name} 不存在或没有表",
                                "message": "请检查数据库名称是否正确"
                            }, ensure_ascii=False)
                        }
                    ]
                }
            
            # 收集所有表的简要信息
            tables_info = []
            for table_name in tables:
                schema = extractor.get_table_schema(table_name, db_name)
                if schema:
                    # 检查是否存在表元数据
                    has_metadata = False
                    try:
                        # 导入元数据模式定义
                        from src.prompts.metadata_schema import METADATA_DB_NAME
                        
                        # 检查business_metadata表
                        metadata_query = f"""
                        SELECT COUNT(*) as count
                        FROM `{METADATA_DB_NAME}`.`business_metadata`
                        WHERE db_name = '{db_name}' 
                        AND table_name = '{table_name}'
                        AND metadata_type = 'table_summary'
                        """
                        metadata_result = execute_query(metadata_query)
                        has_metadata = metadata_result and metadata_result[0].get("count", 0) > 0
                        
                        # 如果business_metadata表中没找到，检查table_metadata表
                        if not has_metadata:
                            table_metadata_query = f"""
                            SELECT COUNT(*) as count
                            FROM `{METADATA_DB_NAME}`.`table_metadata`
                            WHERE database_name = '{db_name}' 
                            AND table_name = '{table_name}'
                            AND business_summary IS NOT NULL
                            """
                            table_metadata_result = execute_query(table_metadata_query)
                            has_metadata = table_metadata_result and table_metadata_result[0].get("count", 0) > 0
                    except Exception as e:
                        logger.warning(f"检查表 {table_name} 是否有元数据时出错: {str(e)}")
                        has_metadata = False
                        
                    tables_info.append({
                        "name": table_name,
                        "comment": schema.get("comment", ""),
                        "column_count": len(schema.get("columns", [])),
                        "has_metadata": has_metadata
                    })
            
            # 创建数据库元数据结果
            metadata_result = {
                "business_metadata": db_metadata,
                "tables": tables_info,
                "table_count": len(tables_info)
            }
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": True,
                            "db_name": db_name,
                            "metadata": metadata_result,
                            "metadata_type": "database_metadata"
                        }, ensure_ascii=False, default=str)
                    }
                ]
            }
    except Exception as e:
        logger.error(f"获取元数据失败: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": str(e),
                        "message": "获取元数据时出错"
                    }, ensure_ascii=False)
                }
            ]
        } 