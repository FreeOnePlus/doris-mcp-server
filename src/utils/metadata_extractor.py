"""
元数据提取工具

负责从数据库中提取表结构、关系等元数据
"""

import os
import json
import pandas as pd
import re
import time
from typing import Dict, List, Any, Optional, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging

# 导入统一日志配置
from src.utils.logger import get_logger

# 配置日志
logger = get_logger(__name__)

# 加载环境变量
load_dotenv(override=True)

# 导入本地模块
from src.utils.db import execute_query_df, execute_query, get_db_connection, ENABLE_MULTI_DATABASE, MULTI_DATABASE_NAMES
from src.utils.llm_client import get_llm_client, Message
from src.prompts.prompts import BUSINESS_METADATA_PROMPTS

class MetadataExtractor:
    """Apache Doris元数据提取器"""
    
    def __init__(self, db_name: str = None):
        """
        初始化元数据提取器
        
        Args:
            db_name: 数据库名称,默认使用环境变量中的DB_DATABASE
        """
        self.db_name = db_name or os.getenv("DB_DATABASE", "")
        
        # 设置元数据数据库名称
        self.metadata_db = "doris_metadata"
        
        # 缓存设置
        self.metadata_cache = {}
        self.metadata_cache_time = {}
        self.cache_ttl = int(os.getenv("METADATA_CACHE_TTL", "3600"))  # 默认缓存1小时
        
        # 启用多数据库支持 - 使用从db.py导入的变量
        self.enable_multi_database = ENABLE_MULTI_DATABASE
        
        # 如果开启了多数据库支持,获取多数据库列表
        if self.enable_multi_database:
            logger.info(f"多数据库模式已启用,配置的数据库: {MULTI_DATABASE_NAMES}")
        
        # 加载表层级匹配配置
        self.enable_table_hierarchy = os.getenv("ENABLE_TABLE_HIERARCHY", "false").lower() == "true"
        if self.enable_table_hierarchy:
            self.table_hierarchy_patterns = self._load_table_hierarchy_patterns()
            logger.info(f"表层级匹配已启用,模式: {self.table_hierarchy_patterns}")
        else:
            self.table_hierarchy_patterns = []
        
        # 加载需要排除的数据库列表
        self.excluded_databases = self._load_excluded_databases()
    
    def _load_excluded_databases(self) -> List[str]:
        """
        加载排除的数据库列表配置
        
        Returns:
            排除的数据库列表
        """
        excluded_dbs_str = os.getenv("EXCLUDED_DATABASES", 
                               '["information_schema", "mysql", "performance_schema", "sys", "doris_metadata"]')
        try:
            excluded_dbs = json.loads(excluded_dbs_str)
            if isinstance(excluded_dbs, list):
                logger.info(f"已加载排除的数据库列表: {excluded_dbs}")
                return excluded_dbs
            else:
                logger.warning("排除的数据库列表配置不是列表格式,使用默认值")
        except json.JSONDecodeError:
            logger.warning("解析排除的数据库列表JSON时出错,使用默认值")
        
        # 默认值
        default_excluded_dbs = ["information_schema", "mysql", "performance_schema", "sys", "doris_metadata"]
        return default_excluded_dbs
        
    def _load_table_hierarchy_patterns(self) -> List[str]:
        """
        加载表层级匹配模式配置
        
        Returns:
            表层级匹配正则表达式列表
        """
        patterns_str = os.getenv("TABLE_HIERARCHY_PATTERNS", 
                               '["^ads_.*$","^dim_.*$","^dws_.*$","^dwd_.*$","^ods_.*$","^tmp_.*$","^stg_.*$","^.*$"]')
        try:
            patterns = json.loads(patterns_str)
            if isinstance(patterns, list):
                # 确保所有模式都是有效的正则表达式
                validated_patterns = []
                for pattern in patterns:
                    try:
                        re.compile(pattern)
                        validated_patterns.append(pattern)
                    except re.error:
                        logger.warning(f"无效的正则表达式模式: {pattern}")
                
                logger.info(f"已加载表层级匹配模式: {validated_patterns}")
                return validated_patterns
            else:
                logger.warning("表层级匹配模式配置不是列表格式,使用默认值")
        except json.JSONDecodeError:
            logger.warning("解析表层级匹配模式JSON时出错,使用默认值")
        
        # 默认值
        default_patterns = ["^ads_.*$", "^dim_.*$", "^dws_.*$", "^dwd_.*$", "^ods_.*$", "^.*$"]
        return default_patterns
        
    def get_all_databases(self) -> List[str]:
        """
        获取所有数据库列表
        
        Returns:
            数据库名列表
        """
        cache_key = "databases"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 使用information_schema.schemata表获取数据库列表
            query = """
            SELECT 
                SCHEMA_NAME 
            FROM 
                information_schema.schemata 
            WHERE 
                SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY 
                SCHEMA_NAME
            """
            
            result = execute_query(query)
            
            if not result:
                databases = []
            else:
                databases = [db["SCHEMA_NAME"] for db in result]
                logger.info(f"获取到的数据库列表: {databases}")
            
            # 更新缓存
            self.metadata_cache[cache_key] = databases
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return databases
        except Exception as e:
            logger.error(f"获取数据库列表时出错: {str(e)}")
            return []

    def get_all_target_databases(self) -> List[str]:
        """
        获取所有目标数据库
        
        如果启用了多数据库支持,则返回配置中的所有数据库；
        否则返回当前数据库
        
        Returns:
            目标数据库列表
        """
        if self.enable_multi_database:
            # 从配置中获取多数据库列表
            from src.utils.db import MULTI_DATABASE_NAMES
            
            # 如果配置为空,则返回当前数据库和系统中所有数据库
            if not MULTI_DATABASE_NAMES:
                all_dbs = self.get_all_databases()
                # 把当前数据库放在最前面
                if self.db_name in all_dbs:
                    all_dbs.remove(self.db_name)
                    all_dbs = [self.db_name] + all_dbs
                return all_dbs
            else:
                # 确保当前数据库在列表中且位于最前面
                db_names = list(MULTI_DATABASE_NAMES)  # 复制一份避免修改原始列表
                if self.db_name and self.db_name not in db_names:
                    db_names.insert(0, self.db_name)
                elif self.db_name and self.db_name in db_names:
                    # 如果当前数据库在列表中但不在第一位,调整位置
                    db_names.remove(self.db_name)
                    db_names.insert(0, self.db_name)
                return db_names
        else:
            # 只返回当前数据库
            return [self.db_name] if self.db_name else []
    
    def get_database_tables(self, db_name: Optional[str] = None) -> List[str]:
        """
        获取数据库中所有表的列表
        
        Args:
            db_name: 数据库名称,如果为None则使用当前数据库
            
        Returns:
            表名列表
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("未指定数据库名称")
            return []
        
        cache_key = f"tables_{db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 使用information_schema.tables表获取表列表
            query = f"""
            SELECT 
                TABLE_NAME 
            FROM 
                information_schema.tables 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_TYPE = 'BASE TABLE'
            """
            
            result = execute_query(query, db_name)
            logger.info(f"{db_name}.information_schema.tables查询结果: {result}")
            
            if not result:
                tables = []
            else:
                tables = [table['TABLE_NAME'] for table in result]
                logger.info(f"从{db_name}.information_schema.tables获取的表名: {tables}")
            
            # 按层级匹配对表进行排序（如果启用）
            if self.enable_table_hierarchy and tables:
                tables = self._sort_tables_by_hierarchy(tables)
            
            # 更新缓存
            self.metadata_cache[cache_key] = tables
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return tables
        except Exception as e:
            logger.error(f"获取表列表时出错: {str(e)}")
            return []
    
    def get_all_tables_and_columns(self) -> Dict[str, Any]:
        """
        获取所有表和列的信息
        
        Returns:
            Dict[str, Any]: 包含所有表和列信息的字典
        """
        cache_key = f"all_tables_columns_{self.db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            result = {}
            tables = self.get_database_tables(self.db_name)
            
            for table_name in tables:
                schema = self.get_table_schema(table_name, self.db_name)
                if schema:
                    columns = schema.get("columns", [])
                    column_names = [col.get("name") for col in columns if col.get("name")]
                    column_types = {col.get("name"): col.get("type") for col in columns if col.get("name") and col.get("type")}
                    column_comments = {col.get("name"): col.get("comment") for col in columns if col.get("name")}
                    
                    result[table_name] = {
                        "comment": schema.get("comment", ""),
                        "columns": column_names,
                        "column_types": column_types,
                        "column_comments": column_comments
                    }
            
            # 更新缓存
            self.metadata_cache[cache_key] = result
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return result
        except Exception as e:
            logger.error(f"获取所有表和列信息时出错: {str(e)}")
            return {}
    
    def _sort_tables_by_hierarchy(self, tables: List[str]) -> List[str]:
        """
        根据层级匹配模式对表进行排序
        
        Args:
            tables: 表名列表
            
        Returns:
            排序后的表名列表
        """
        if not self.enable_table_hierarchy or not self.table_hierarchy_patterns:
            return tables
        
        # 按照模式优先级对表进行分组
        table_groups = []
        remaining_tables = set(tables)
        
        for pattern in self.table_hierarchy_patterns:
            matching_tables = []
            regex = re.compile(pattern)
            
            for table in list(remaining_tables):
                if regex.match(table):
                    matching_tables.append(table)
                    remaining_tables.remove(table)
            
            if matching_tables:
                # 在每个分组内部,按字母顺序排序
                matching_tables.sort()
                table_groups.append(matching_tables)
        
        # 将剩余的表添加到最后
        if remaining_tables:
            table_groups.append(sorted(list(remaining_tables)))
        
        # 扁平化分组
        return [table for group in table_groups for table in group]
    
    def get_all_tables_from_all_databases(self) -> Dict[str, List[str]]:
        """
        获取所有目标数据库中的所有表
        
        Returns:
            数据库名到表名列表的映射
        """
        all_tables = {}
        target_dbs = self.get_all_target_databases()
        
        for db_name in target_dbs:
            tables = self.get_database_tables(db_name)
            if tables:
                all_tables[db_name] = tables
        
        return all_tables
    
    def find_tables_by_pattern(self, pattern: str, db_name: Optional[str] = None) -> List[Tuple[str, str]]:
        """
        根据模式在数据库中查找匹配的表
        
        Args:
            pattern: 表名模式（正则表达式）
            db_name: 数据库名称,如果为None则搜索所有目标数据库
            
        Returns:
            匹配的(数据库名,表名)元组列表
        """
        try:
            regex = re.compile(pattern)
        except re.error:
            logger.error(f"无效的正则表达式模式: {pattern}")
            return []
        
        matches = []
        
        if db_name:
            # 只在指定数据库中搜索
            tables = self.get_database_tables(db_name)
            matches = [(db_name, table) for table in tables if regex.match(table)]
        else:
            # 在所有目标数据库中搜索
            all_tables = self.get_all_tables_from_all_databases()
            
            for db, tables in all_tables.items():
                db_matches = [(db, table) for table in tables if regex.match(table)]
                matches.extend(db_matches)
        
        return matches
    
    def get_table_schema(self, table_name: str, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取表的结构信息
        
        Args:
            table_name: 表名
            db_name: 数据库名称,如果为None则使用当前数据库
            
        Returns:
            表结构信息,包含列名、类型、是否允许空值、默认值、注释等
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("未指定数据库名称")
            return {}
        
        cache_key = f"schema_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 使用information_schema.columns表获取表结构
            query = f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                IS_NULLABLE, 
                COLUMN_DEFAULT, 
                COLUMN_COMMENT,
                ORDINAL_POSITION,
                COLUMN_KEY,
                EXTRA
            FROM 
                information_schema.columns 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}'
            ORDER BY 
                ORDINAL_POSITION
            """
            
            result = execute_query(query)
            
            if not result:
                logger.warning(f"表 {db_name}.{table_name} 不存在或没有列")
                return {}
                
            # 创建结构化的表模式信息
            columns = []
            for col in result:
                # 确保使用实际的列值,而不是列名
                column_info = {
                    "name": col.get("COLUMN_NAME", ""),
                    "type": col.get("DATA_TYPE", ""),
                    "nullable": col.get("IS_NULLABLE", "") == "YES",
                    "default": col.get("COLUMN_DEFAULT", ""),
                    "comment": col.get("COLUMN_COMMENT", "") or "",
                    "position": col.get("ORDINAL_POSITION", ""),
                    "key": col.get("COLUMN_KEY", "") or "",
                    "extra": col.get("EXTRA", "") or ""
                }
                columns.append(column_info)
                
            # 获取表注释
            table_comment = self.get_table_comment(table_name, db_name)
            
            # 构建完整结构
            schema = {
                "name": table_name,
                "database": db_name,
                "comment": table_comment,
                "columns": columns,
                "create_time": datetime.now().isoformat()
            }
            
            # 获取表类型信息
            try:
                table_type_query = f"""
                SELECT 
                    TABLE_TYPE,
                    ENGINE 
                FROM 
                    information_schema.tables 
                WHERE 
                    TABLE_SCHEMA = '{db_name}' 
                    AND TABLE_NAME = '{table_name}'
                """
                table_type_result = execute_query(table_type_query)
                if table_type_result:
                    schema["table_type"] = table_type_result[0].get("TABLE_TYPE", "")
                    schema["engine"] = table_type_result[0].get("ENGINE", "")
            except Exception as e:
                logger.warning(f"获取表类型信息时出错: {str(e)}")
            
            # 更新缓存
            self.metadata_cache[cache_key] = schema
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return schema
        except Exception as e:
            logger.error(f"获取表结构时出错: {str(e)}")
            return {}
    
    def get_table_comment(self, table_name: str, db_name: Optional[str] = None) -> str:
        """
        获取表的注释
        
        Args:
            table_name: 表名
            db_name: 数据库名称,如果为None则使用当前数据库
            
        Returns:
            表注释
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("未指定数据库名称")
            return ""
        
        cache_key = f"table_comment_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 使用information_schema.tables表获取表注释
            query = f"""
            SELECT 
                TABLE_COMMENT 
            FROM 
                information_schema.tables 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}'
            """
            
            result = execute_query(query)
            
            if not result or not result[0]:
                comment = ""
            else:
                comment = result[0].get("TABLE_COMMENT", "")
            
            # 更新缓存
            self.metadata_cache[cache_key] = comment
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return comment
        except Exception as e:
            logger.error(f"获取表注释时出错: {str(e)}")
            return ""
    
    def get_column_comments(self, table_name: str, db_name: Optional[str] = None) -> Dict[str, str]:
        """
        获取表中所有列的注释
        
        Args:
            table_name: 表名
            db_name: 数据库名称,如果为None则使用当前数据库
            
        Returns:
            列名和注释的字典
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("未指定数据库名称")
            return {}
        
        cache_key = f"column_comments_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 使用information_schema.columns表获取列注释
            query = f"""
            SELECT 
                COLUMN_NAME, 
                COLUMN_COMMENT 
            FROM 
                information_schema.columns 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}'
            ORDER BY 
                ORDINAL_POSITION
            """
            
            result = execute_query(query)
            
            comments = {}
            for col in result:
                column_name = col.get("COLUMN_NAME", "")
                column_comment = col.get("COLUMN_COMMENT", "")
                if column_name:
                    comments[column_name] = column_comment
            
            # 更新缓存
            self.metadata_cache[cache_key] = comments
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return comments
        except Exception as e:
            logger.error(f"获取列注释时出错: {str(e)}")
            return {}
    
    def get_table_indexes(self, table_name: str, db_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取表的索引信息
        
        Args:
            table_name: 表名
            db_name: 数据库名称,如果为None则使用初始化时指定的数据库
            
        Returns:
            List[Dict[str, Any]]: 索引信息列表
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("未指定数据库名称")
            return []
        
        cache_key = f"indexes_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            query = f"SHOW INDEX FROM `{db_name}`.`{table_name}`"
            df = execute_query_df(query)
            
            # 处理结果
            indexes = []
            current_index = None
            
            for _, row in df.iterrows():
                index_name = row['Key_name']
                column_name = row['Column_name']
                
                if current_index is None or current_index['name'] != index_name:
                    if current_index is not None:
                        indexes.append(current_index)
                    
                    current_index = {
                        'name': index_name,
                        'columns': [column_name],
                        'unique': row['Non_unique'] == 0,
                        'type': row['Index_type']
                    }
                else:
                    current_index['columns'].append(column_name)
            
            if current_index is not None:
                indexes.append(current_index)
            
            # 更新缓存
            self.metadata_cache[cache_key] = indexes
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return indexes
        except Exception as e:
            logger.error(f"获取索引信息时出错: {str(e)}")
            return []
    
    def get_table_relationships(self) -> List[Dict[str, Any]]:
        """
        从表的注释和命名模式推断表关系
        
        Returns:
            List[Dict[str, Any]]: 表关系信息列表
        """
        cache_key = f"relationships_{self.db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 获取所有表
            tables = self.get_database_tables(self.db_name)
            relationships = []
            
            # 简单的外键命名规则检测
            # 例如: 如果一个表有一个列名为 xxx_id,并且有另一个表名为 xxx,则可能是外键关系
            for table_name in tables:
                schema = self.get_table_schema(table_name, self.db_name)
                columns = schema.get("columns", [])
                
                for column in columns:
                    column_name = column["name"]
                    if column_name.endswith('_id'):
                        # 可能的外键表名
                        ref_table_name = column_name[:-3]  # 移除 _id 后缀
                        
                        # 检查可能的表是否存在
                        if ref_table_name in tables:
                            # 查找可能的主键列
                            ref_schema = self.get_table_schema(ref_table_name, self.db_name)
                            ref_columns = ref_schema.get("columns", [])
                            
                            # 假设主键列名为 id
                            if any(col["name"] == "id" for col in ref_columns):
                                relationships.append({
                                    "table": table_name,
                                    "column": column_name,
                                    "references_table": ref_table_name,
                                    "references_column": "id",
                                    "relationship_type": "many-to-one",
                                    "confidence": "medium"  # 置信度不高,基于命名约定
                                })
            
            # 更新缓存
            self.metadata_cache[cache_key] = relationships
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return relationships
        except Exception as e:
            logger.error(f"推断表关系时出错: {str(e)}")
            return []
    
    def get_recent_audit_logs(self, days: int = 7, limit: int = 100) -> pd.DataFrame:
        """
        获取最近的审计日志
        
        Args:
            days: 获取最近几天的审计日志
            limit: 最多返回多少条记录
            
        Returns:
            pd.DataFrame: 审计日志数据框
        """
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            query = f"""
            SELECT client_ip, user, db, time, stmt_id, stmt, state, error_code
            FROM `__internal_schema`.`audit_log`
            WHERE `time` >= '{start_date}'
            AND state = 'EOF' AND error_code = 0
            AND `stmt` NOT LIKE 'SHOW%'
            AND `stmt` NOT LIKE 'DESC%'
            AND `stmt` NOT LIKE 'EXPLAIN%'
            AND `stmt` NOT LIKE 'SELECT 1%'
            ORDER BY time DESC
            LIMIT {limit}
            """
            df = execute_query_df(query)
            return df
        except Exception as e:
            logger.error(f"获取审计日志时出错: {str(e)}")
            return pd.DataFrame()
    
    def extract_sql_comments(self, sql: str) -> str:
        """
        从SQL中提取注释
        
        Args:
            sql: SQL查询
            
        Returns:
            str: 提取的注释
        """
        # 提取单行注释
        single_line_comments = re.findall(r'--\s*(.*?)(?:\n|$)', sql)
        
        # 提取多行注释
        multi_line_comments = re.findall(r'/\*(.*?)\*/', sql, re.DOTALL)
        
        # 合并所有注释
        all_comments = single_line_comments + multi_line_comments
        return '\n'.join(comment.strip() for comment in all_comments if comment.strip())
    
    def extract_common_sql_patterns(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        提取常见的SQL模式
        
        Args:
            limit: 最多获取多少条审计日志
            
        Returns:
            List[Dict[str, Any]]: SQL模式信息列表,包含pattern, type, frequency等字段
        """
        try:
            # 获取审计日志
            audit_logs = self.get_recent_audit_logs(days=30, limit=limit)
            if audit_logs.empty:
                # 如果无法获取审计日志,返回一些默认模式
                default_patterns = [
                    {
                        "pattern": "SELECT * FROM {table} WHERE {condition}",
                        "type": "SELECT",
                        "frequency": 1
                    },
                    {
                        "pattern": "SELECT {columns} FROM {table} GROUP BY {group_by} ORDER BY {order_by} LIMIT {limit}",
                        "type": "SELECT",
                        "frequency": 1
                    }
                ]
                return default_patterns
            
            # 按SQL类型分组处理
            patterns_by_type = {}
            for _, row in audit_logs.iterrows():
                sql = row['stmt']
                if not sql:
                    continue
                
                # 确定SQL类型
                sql_type = self._get_sql_type(sql)
                if not sql_type:
                    continue
                
                # 简化SQL
                simplified_sql = self._simplify_sql(sql)
                
                # 提取涉及的表
                tables = self._extract_tables_from_sql(sql)
                
                # 提取SQL注释
                comments = self.extract_sql_comments(sql)
                
                # 如果是新模式,初始化
                if sql_type not in patterns_by_type:
                    patterns_by_type[sql_type] = []
                    
                # 查找是否有类似的模式
                found_similar = False
                for pattern in patterns_by_type[sql_type]:
                    if self._are_sqls_similar(simplified_sql, pattern['simplified_sql']):
                        pattern['count'] += 1
                        pattern['examples'].append(sql)
                        if comments:
                            pattern['comments'].append(comments)
                        found_similar = True
                        break
                        
                # 如果没有找到类似的模式,添加新模式
                if not found_similar:
                    patterns_by_type[sql_type].append({
                        'simplified_sql': simplified_sql,
                        'examples': [sql],
                        'comments': [comments] if comments else [],
                        'count': 1,
                        'tables': tables
                    })
                    
            # 将分组的模式转换为必要的输出格式
            result_patterns = []
            
            # 按照频率排序并转换格式
            for sql_type, type_patterns in patterns_by_type.items():
                sorted_patterns = sorted(type_patterns, key=lambda x: x['count'], reverse=True)
                
                # 提取前3个模式并转换为预期格式
                for pattern in sorted_patterns[:3]:
                    # 创建与_update_sql_patterns_for_all_databases中使用的格式一致的输出
                    result_patterns.append({
                        "pattern": pattern['simplified_sql'],
                        "type": sql_type,
                        "frequency": pattern['count'],
                        "examples": json.dumps(pattern['examples'][:3], ensure_ascii=False),
                        "comments": json.dumps(pattern['comments'][:3], ensure_ascii=False) if pattern['comments'] else "[]",
                        "tables": json.dumps(pattern['tables'], ensure_ascii=False)
                    })
            
            # 如果没有找到任何模式,返回默认值
            if not result_patterns:
                default_patterns = [
                    {
                        "pattern": "SELECT * FROM {table} WHERE {condition}",
                        "type": "SELECT",
                        "frequency": 1,
                        "examples": "[]",
                        "comments": "[]",
                        "tables": "[]"
                    },
                    {
                        "pattern": "SELECT {columns} FROM {table} GROUP BY {group_by} ORDER BY {order_by} LIMIT {limit}",
                        "type": "SELECT",
                        "frequency": 1,
                        "examples": "[]",
                        "comments": "[]",
                        "tables": "[]"
                    }
                ]
                return default_patterns
            
            return result_patterns
            
        except Exception as e:
            logger.error(f"提取SQL模式时出错: {str(e)}")
            # 返回一些默认模式,确保不会导致后续处理错误
            default_patterns = [
                {
                    "pattern": "SELECT * FROM {table} WHERE {condition}",
                    "type": "SELECT",
                    "frequency": 1,
                    "examples": "[]",
                    "comments": "[]",
                    "tables": "[]"
                },
                {
                    "pattern": "SELECT {columns} FROM {table} GROUP BY {group_by} ORDER BY {order_by} LIMIT {limit}",
                    "type": "SELECT",
                    "frequency": 1,
                    "examples": "[]",
                    "comments": "[]",
                    "tables": "[]"
                }
            ]
            return default_patterns
    
    def _simplify_sql(self, sql: str) -> str:
        """
        简化SQL以便更好地识别模式
        
        Args:
            sql: SQL查询
            
        Returns:
            str: 简化后的SQL
        """
        # 移除注释
        sql = re.sub(r'--.*?(\n|$)', ' ', sql)
        sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
        
        # 替换字符串和数字常量
        sql = re.sub(r"'[^']*'", "'?'", sql)
        sql = re.sub(r'\b\d+\b', '?', sql)
        
        # 替换IN条件的内容
        sql = re.sub(r'IN\s*\([^)]+\)', 'IN (?)', sql, flags=re.IGNORECASE)
        
        # 移除多余的空白字符
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        return sql
    
    def _are_sqls_similar(self, sql1: str, sql2: str) -> bool:
        """
        判断两个SQL是否相似
        
        Args:
            sql1: 第一个SQL
            sql2: 第二个SQL
            
        Returns:
            bool: 如果相似则返回True
        """
        # 简单相似度检查
        return sql1 == sql2
    
    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """
        从SQL中提取表名
        
        Args:
            sql: SQL查询
            
        Returns:
            List[str]: 表名列表
        """
        # 这是一个非常简化的实现
        # 实际应用中需要更复杂的SQL解析
        tables = set()
        
        # 查找FROM子句后的表名
        from_matches = re.finditer(r'\bFROM\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in from_matches:
            tables.add(match.group(1))
        
        # 查找JOIN子句后的表名
        join_matches = re.finditer(r'\bJOIN\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in join_matches:
            tables.add(match.group(1))
        
        # 查找INSERT INTO后的表名
        insert_matches = re.finditer(r'\bINSERT\s+INTO\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in insert_matches:
            tables.add(match.group(1))
        
        # 查找UPDATE后的表名
        update_matches = re.finditer(r'\bUPDATE\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in update_matches:
            tables.add(match.group(1))
        
        # 查找DELETE FROM后的表名
        delete_matches = re.finditer(r'\bDELETE\s+FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in delete_matches:
            tables.add(match.group(1))
        
        return list(tables)
    
    def _get_sql_type(self, sql: str) -> str:
        """
        判断SQL语句的类型
        
        Args:
            sql: SQL语句
            
        Returns:
            str: SQL类型,如SELECT, INSERT, UPDATE等
        """
        sql = sql.strip().upper()
        
        if sql.startswith('SELECT'):
            return 'SELECT'
        elif sql.startswith('INSERT'):
            return 'INSERT'
        elif sql.startswith('UPDATE'):
            return 'UPDATE'
        elif sql.startswith('DELETE'):
            return 'DELETE'
        elif sql.startswith('CREATE'):
            return 'CREATE'
        elif sql.startswith('DROP'):
            return 'DROP'
        elif sql.startswith('ALTER'):
            return 'ALTER'
        elif sql.startswith('SHOW'):
            return 'SHOW'
        elif sql.startswith('DESCRIBE'):
            return 'DESCRIBE'
        else:
            return 'OTHER'
    
    def summarize_business_metadata(self, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取业务元数据摘要,优先从元数据库获取,若不存在则通过LLM生成
        
        Args:
            db_name: 数据库名称,如果为None则使用初始化时指定的数据库
            
        Returns:
            Dict[str, Any]: 业务元数据总结
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("未指定数据库名称")
            return {"error": "未指定数据库名称", "tables_summary": []}
        
        cache_key = f"business_metadata_{db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        # 定义默认元数据
        default_metadata = {
            "business_domain": f"{db_name}数据库",
            "core_entities": [],
            "business_processes": [],
            "tables_summary": []
        }
        
        try:
            # 首先尝试从元数据库获取业务元数据
            logger.info(f"尝试从元数据库获取 {db_name} 的业务元数据")
            
            # 修改为正确的查询方式
            query = f"""
            SELECT metadata_value 
            FROM {self.metadata_db}.business_metadata 
            WHERE db_name = '{db_name}' 
            AND table_name = '' 
            AND metadata_type = 'business_summary'
            """
            
            try:
                result = execute_query(query)
                logger.info(f"元数据查询结果: {result}")
                
                if result and len(result) > 0 and 'metadata_value' in result[0]:
                    try:
                        db_metadata = json.loads(result[0]['metadata_value'])
                        if isinstance(db_metadata, dict) and len(db_metadata) > 0:
                            logger.info(f"成功从元数据库获取 {db_name} 的业务元数据")
                            
                            # 更新缓存
                            self.metadata_cache[cache_key] = db_metadata
                            self.metadata_cache_time[cache_key] = datetime.now()
                            
                            return db_metadata
                    except json.JSONDecodeError as e:
                        logger.warning(f"解析元数据JSON时出错: {str(e)}")
                        # 继续执行,通过LLM生成新的元数据
                
                logger.info(f"元数据库中没有找到 {db_name} 的业务元数据,将通过LLM生成")
            except Exception as e:
                logger.warning(f"查询元数据时出错: {str(e)},将通过LLM生成新的元数据")
            
            # 获取数据库所有表
            tables = self.get_database_tables(db_name)
            logger.info(f"为 {db_name} 总结业务元数据,发现 {len(tables)} 个表")
            
            # 收集所有表的元数据
            tables_metadata = []
            for table_name in tables:
                schema = self.get_table_schema(table_name, db_name)
                if schema:  # 只添加成功获取的表结构
                    tables_metadata.append(schema)
            
            # 提取SQL模式
            sql_patterns = self.extract_common_sql_patterns(limit=50)
            
            # 准备表结构信息文本
            tables_info = ""
            for table_meta in tables_metadata:
                table_name = table_meta.get("name", "")
                table_comment = table_meta.get("comment", "")
                columns = table_meta.get("columns", [])
                
                tables_info += f"- 表名: {table_name} (注释: {table_comment})\n"
                tables_info += "  列:\n"
                for column in columns:
                    name = column.get("name", "")
                    type = column.get("type", "")
                    comment = column.get("comment", "")
                    tables_info += f"    - {name} ({type}) - {comment}\n"
                tables_info += "\n"
            
            # 准备SQL模式信息文本
            sql_patterns_text = ""
            if isinstance(sql_patterns, list):
                for pattern in sql_patterns[:5]:  # 最多展示5个模式
                    sql_type = pattern.get("type", "未知")
                    pattern_text = pattern.get("pattern", "")
                    frequency = pattern.get("frequency", 0)
                    sql_patterns_text += f"- {sql_type}查询 (频率: {frequency}):\n  {pattern_text}\n\n"
            
            # 使用提示词模板
            system_prompt = BUSINESS_METADATA_PROMPTS["system"]
            user_prompt = BUSINESS_METADATA_PROMPTS["user"].format(
                db_name=db_name,
                tables_info=tables_info,
                sql_patterns=sql_patterns_text
            )
            
            # 创建日志目录
            from pathlib import Path
            project_root = Path(__file__).parents[3]
            log_dir = project_root / "log" / "llm_calls"
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 记录LLM请求
            llm_request_log = {
                "function": "summarize_business_metadata",
                "db_name": db_name,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "timestamp": datetime.now().isoformat()
            }
            
            # 保存请求日志
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            request_log_path = log_dir / f"{timestamp}_business_metadata_request.json"
            with open(request_log_path, 'w', encoding='utf-8') as f:
                json.dump(llm_request_log, f, ensure_ascii=False, indent=2)
            
            logger.info("调用LLM生成业务元数据摘要")
            logger.info(f"LLM请求日志保存到: {request_log_path}")
            
            # 调用LLM
            try:
                from src.utils.llm_client import get_llm_client, Message
                
                client = get_llm_client(stage="metadata")
                
                # 如果LLM客户端为None（可能是程序正在退出）,返回默认值
                if client is None:
                    logger.warning("无法获取LLM客户端,可能是程序正在退出")
                    return default_metadata
                
                response = client.chat([
                    Message.system(system_prompt),
                    Message.user(user_prompt)
                ])
                
                if not response or not response.content:
                    logger.warning("LLM响应为空")
                    return default_metadata
                
                # 解析LLM回复获取业务元数据
                try:
                    metadata = self._extract_json_from_llm_response(response.content)
                except Exception as e:
                    logger.error(f"解析LLM响应提取JSON时出错: {str(e)}")
                    return default_metadata
                
                if not metadata or metadata.get("extraction_failed", False):
                    logger.warning(f"从LLM响应中提取JSON失败: {response.content[:200]}...")
                    return default_metadata
                
                # 确保必要字段存在
                if not isinstance(metadata, dict):
                    logger.warning("LLM返回的不是有效的字典对象")
                    return default_metadata
                
                if "tables_summary" not in metadata:
                    metadata["tables_summary"] = {}
                
                # 记录业务元数据以供参考
                metadata_log_path = os.path.join(log_dir, f"metadata_{db_name}_{timestamp}.json")
                with open(metadata_log_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                logger.info(f"业务元数据保存到: {metadata_log_path}")
                
                return metadata
            except Exception as e:
                logger.error(f"调用LLM生成业务元数据摘要时出错: {str(e)}")
                return default_metadata
                
        except Exception as e:
            logger.error(f"调用LLM生成业务元数据摘要时出错: {str(e)}")
            return default_metadata
    
    def _preprocess_llm_response(self, content: str) -> str:
        """
        预处理LLM响应内容，处理常见的格式问题
        
        Args:
            content: 原始LLM响应内容
            
        Returns:
            str: 处理后的内容
        """
        try:
            # 移除可能的Markdown格式标记
            content = re.sub(r'#+ ', '', content)
            
            # 处理代码块
            if '```json' in content and not content.endswith('```'):
                content += '\n```'
            
            # 处理不完整的JSON
            if 'core_e...' in content:
                # 尝试补全常见字段名
                content = content.replace('core_e...', 'core_entities')
            
            # 移除Unicode转义序列
            content = content.encode('utf-8').decode('unicode_escape')
            
            # 清理空白字符
            content = content.strip()
            
            return content
        except Exception as e:
            logger.warning(f"预处理LLM响应时出错: {str(e)}")
            return content
            
    def _extract_json_from_llm_response(self, content: str) -> Dict:
        """
        从LLM响应中提取JSON内容
        
        Args:
            content: LLM响应内容
            
        Returns:
            Dict: 提取的JSON对象
        """
        # 预处理内容
        content = self._preprocess_llm_response(content)
        
        result = {}
        
        # 1. 寻找代码块中的JSON
        try:
            json_block_pattern = r'```(?:json)?\s*\n([\s\S]*?)\n```'
            json_block_matches = re.findall(json_block_pattern, content)
            
            if json_block_matches:
                for json_str in json_block_matches:
                    try:
                        json_obj = json.loads(json_str)
                        
                        # 处理tables_summary可能是列表的情况
                        if "tables_summary" in json_obj and isinstance(json_obj["tables_summary"], list):
                            logger.info(f"检测到tables_summary是列表格式，包含 {len(json_obj['tables_summary'])} 个表")
                            
                        return json_obj
                    except json.JSONDecodeError as je:
                        logger.warning(f"JSON代码块解析失败: {str(je)}")
                        
                        # 尝试修复常见问题并重新解析
                        try:
                            # 处理缺失的右括号
                            if json_str.count('{') > json_str.count('}'):
                                logger.info("尝试修复缺失的右括号")
                                diff = json_str.count('{') - json_str.count('}')
                                json_str += "}" * diff
                            
                            # 处理末尾多余的逗号
                            json_str = re.sub(r',\s*}', "}", json_str)
                            json_str = re.sub(r',\s*]', "]", json_str)
                            
                            # 再次尝试解析
                            json_obj = json.loads(json_str)
                            logger.info("成功修复并解析JSON")
                            return json_obj
                        except Exception:
                            continue
            
            # 2. 如果代码块中没有找到，尝试在整个内容中寻找完整的JSON对象
            bracket_pattern = r'\{[\s\S]*?\}'
            bracket_matches = re.findall(bracket_pattern, content)
            
            if bracket_matches:
                for json_str in bracket_matches:
                    try:
                        # 尝试直接解析
                        json_obj = json.loads(json_str)
                        return json_obj
                    except json.JSONDecodeError:
                        # 尝试修复JSON并解析
                        try:
                            # 处理缺失的右括号
                            if json_str.count('{') > json_str.count('}'):
                                diff = json_str.count('{') - json_str.count('}')
                                json_str += "}" * diff
                            
                            # 处理末尾多余的逗号
                            json_str = re.sub(r',\s*}', "}", json_str)
                            json_str = re.sub(r',\s*]', "]", json_str)
                            
                            json_obj = json.loads(json_str)
                            return json_obj
                        except:
                            continue
            
            # 3. 尝试从文件系统中查找最近的元数据JSON文件
            try:
                from pathlib import Path
                import glob
                
                # 查找最近的元数据日志文件
                log_dir = Path("logs")
                if log_dir.exists():
                    metadata_files = sorted(glob.glob(str(log_dir / "metadata_*.json")), reverse=True)
                    if metadata_files:
                        latest_file = metadata_files[0]
                        logger.info(f"尝试从文件加载元数据: {latest_file}")
                        with open(latest_file, 'r', encoding='utf-8') as f:
                            json_obj = json.load(f)
                            if isinstance(json_obj, dict) and "business_domain" in json_obj:
                                logger.info(f"成功从文件 {latest_file} 加载元数据")
                                return json_obj
            except Exception as e:
                logger.warning(f"尝试从文件加载元数据时出错: {str(e)}")
                
            # 4. 如果仍然失败，尝试提取部分有效的键值对
            try:
                # 提取键值对
                pairs_pattern = r'"([^"]+)":\s*(?:"([^"]*)"|\[([^\]]*)\]|(\{[^\}]*\}))'
                pairs = re.findall(pairs_pattern, content)
                if pairs:
                    partial_result = {}
                    for pair in pairs:
                        key = pair[0]
                        value = pair[1] or pair[2] or pair[3]
                        if value:
                            try:
                                # 尝试解析JSON值
                                if value.startswith('[') or value.startswith('{'):
                                    value = json.loads(value)
                                partial_result[key] = value
                            except:
                                partial_result[key] = value
                    if partial_result:
                        logger.info("成功提取部分JSON键值对")
                        return partial_result
            except Exception as e:
                logger.warning(f"提取部分JSON时出错: {str(e)}")
        
        except Exception as e:
            logger.warning(f"提取JSON时出错: {str(e)}")
        
        # 5. 如果所有尝试都失败，提供默认响应
        logger.warning(f"无法从内容中提取有效的JSON: {content[:100]}...")
        
        # 创建一个基本的默认响应
        default_response = {
            "default_response": True,
            "extraction_failed": True,
            "content_sample": content[:200] + ("..." if len(content) > 200 else ""),
            "business_domain": "未能成功提取业务领域描述",
            "core_entities": [],
            "tables_summary": []
        }
        
        return default_response
    
    def refresh_all_databases_metadata(self, force: bool = False) -> bool:
        """
        刷新所有目标数据库的元数据
        
        Args:
            force: 是否强制执行全量刷新,默认为False(增量刷新)
            
        Returns:
            bool: 刷新是否成功
        """
        try:
            # 获取所有目标数据库
            target_databases = self.get_all_target_databases()
            logger.info(f"将刷新以下数据库的元数据: {target_databases}")
            
            # 检查元数据表是否存在
            check_db_query = "SHOW DATABASES LIKE 'doris_metadata'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                logger.warning("元数据数据库不存在,将创建并执行全量刷新")
                force = True
            else:
                check_table_query = "SHOW TABLES FROM `doris_metadata` LIKE 'business_metadata'"
                table_exists = execute_query(check_table_query)
                
                if not table_exists:
                    logger.warning("元数据表不存在,将创建并执行全量刷新")
                    force = True
            
            overall_success = True
            
            # 遍历每个数据库,刷新元数据
            for db_name in target_databases:
                try:
                    # 如果不是强制刷新,先检查是否已存在元数据
                    if not force:
                        # 检查是否已有元数据
                        query = f"""
                        SELECT COUNT(*) as count
                        FROM {self.metadata_db}.business_metadata 
                        WHERE db_name = '{db_name}' 
                        AND table_name = '' 
                        AND metadata_type = 'business_summary'
                        """
                        
                        try:
                            result = execute_query(query)
                            if result and result[0]['count'] > 0:
                                logger.info(f"数据库 {db_name} 已存在元数据,跳过刷新")
                                continue
                        except Exception as e:
                            logger.warning(f"检查数据库 {db_name} 元数据时出错: {str(e)}")
                            # 出错时继续刷新
                    
                    # 更新SQL模式和业务元数据
                    logger.info(f"开始刷新数据库 {db_name} 的元数据")
                    success = self._update_sql_patterns_and_business_metadata(db_name, force)
                    
                    if success:
                        logger.info(f"成功刷新数据库 {db_name} 的元数据")
                    else:
                        logger.warning(f"刷新数据库 {db_name} 的元数据失败")
                        overall_success = False
                except Exception as e:
                    logger.error(f"刷新数据库 {db_name} 的元数据时出错: {str(e)}")
                    overall_success = False
            
            # 记录最后刷新时间
            self.last_refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return overall_success
        except Exception as e:
            logger.error(f"刷新所有数据库元数据时出错: {str(e)}")
            return False
    
    def _update_sql_patterns_and_business_metadata(self, db_name: str, force: bool = False) -> bool:
        """
        更新SQL模式和业务元数据
        
        Args:
            db_name: 数据库名称
            force: 是否强制执行全量刷新,默认为False(增量刷新)
            
        Returns:
            bool: 更新是否成功
        """
        try:
            logger.info(f"开始更新数据库 {db_name} 的SQL模式和业务元数据")
            
            # 清除缓存（如果强制刷新）
            if force:
                for key in list(self.metadata_cache.keys()):
                    if key.endswith(db_name):
                        del self.metadata_cache[key]
                        if key in self.metadata_cache_time:
                            del self.metadata_cache_time[key]
                logger.info(f"强制刷新：已清除数据库 {db_name} 的元数据缓存")
            
            # 获取业务元数据（这会触发从数据库获取或通过LLM生成）
            business_metadata = self.summarize_business_metadata(db_name)
            
            # 检查业务元数据是否成功获取
            if not business_metadata or not isinstance(business_metadata, dict):
                logger.warning(f"获取数据库 {db_name} 的业务元数据失败")
                return False
            
            # 获取表级别的摘要并单独保存每个表的元数据
            tables_summary = business_metadata.get("tables_summary", {})
            
            # 处理tables_summary可能是列表或字典的情况
            if isinstance(tables_summary, list):
                logger.info(f"检测到表摘要为列表格式, 包含 {len(tables_summary)} 个表")
                tables_dict = {}
                for table_info in tables_summary:
                    if isinstance(table_info, dict) and "name" in table_info:
                        table_name = table_info["name"]
                        # 移除name字段，将剩余内容作为摘要
                        table_summary = {k: v for k, v in table_info.items() if k != "name"}
                        tables_dict[table_name] = table_summary
                tables_summary = tables_dict
                
            if isinstance(tables_summary, dict) and tables_summary:
                logger.info(f"更新数据库 {db_name} 的表级别业务元数据,发现 {len(tables_summary)} 个表摘要")
                for table_name, table_summary in tables_summary.items():
                    try:
                        # 构建表级别元数据
                        table_metadata = {
                            "summary": table_summary,
                            "update_time": datetime.now().isoformat()
                        }
                        
                        # 保存表级别元数据
                        self._save_business_metadata(
                            db_name, 
                            table_name, 
                            "table_summary", 
                            json.dumps(table_metadata, ensure_ascii=False)
                        )
                        logger.info(f"已保存表 {db_name}.{table_name} 的业务元数据摘要")
                    except Exception as e:
                        logger.warning(f"保存表 {db_name}.{table_name} 的业务元数据时出错: {str(e)}")
            
            logger.info(f"成功更新数据库 {db_name} 的业务元数据")
            
            # 记录最后刷新时间
            self.last_refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return True
        except Exception as e:
            logger.error(f"更新数据库 {db_name} 的SQL模式和业务元数据时出错: {str(e)}")
            return False
    
    def _save_business_metadata(self, db_name: str, table_name: str, metadata_type: str, metadata_value: str) -> None:
        """
        保存业务元数据到元数据表
        
        Args:
            db_name: 数据库名称
            table_name: 表名
            metadata_type: 元数据类型
            metadata_value: 元数据值
        """
        try:
            # 首先检查doris_metadata数据库是否存在,如果不存在则创建
            check_db_query = "SHOW DATABASES LIKE 'doris_metadata'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                create_db_query = "CREATE DATABASE IF NOT EXISTS `doris_metadata`"
                execute_query(create_db_query)
                logger.info("已创建元数据库 doris_metadata")
            
            # 检查business_metadata表是否存在,检查表结构
            check_table_query = "SHOW TABLES FROM `doris_metadata` LIKE 'business_metadata'"
            table_exists = execute_query(check_table_query)
            
            old_table_backup = False
            
            # 如果表存在,检查列结构是否正确
            if table_exists:
                # 检查表结构
                desc_query = "DESC `doris_metadata`.`business_metadata`"
                table_structure = execute_query(desc_query)
                
                # 检查是否包含db_name列和business_keywords列
                column_names = [col.get('Field', '') for col in table_structure]
                
                # 如果表结构不正确但存在旧数据,先备份旧数据
                if ('db_name' in column_names) and ('business_keywords' not in column_names):
                    logger.info("检测到旧版元数据表结构,准备备份数据并升级表结构")
                    # 检查是否有数据需要备份
                    count_query = "SELECT COUNT(*) as count FROM `doris_metadata`.`business_metadata`"
                    count_result = execute_query(count_query)
                    
                    if count_result and count_result[0]['count'] > 0:
                        # 备份旧数据
                        backup_query = "SELECT * FROM `doris_metadata`.`business_metadata`"
                        old_data = execute_query(backup_query)
                        old_table_backup = True
                        logger.info(f"成功备份 {len(old_data)} 条旧元数据记录")
                
                # 如果表结构不正确,删除表并重新创建
                if 'db_name' not in column_names or 'business_keywords' not in column_names:
                    logger.warning("表business_metadata结构不正确,将重新创建")
                    drop_query = "DROP TABLE IF EXISTS `doris_metadata`.`business_metadata`"
                    execute_query(drop_query)
                    table_exists = False
                    
            # 如果表不存在,创建表
            if not table_exists:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS `doris_metadata`.`business_metadata` (
                    `db_name` VARCHAR(100) NOT NULL COMMENT '数据库名',
                    `table_name` VARCHAR(100) COMMENT '表名',
                    `metadata_type` VARCHAR(50) NOT NULL COMMENT '元数据类型',
                    `metadata_value` TEXT COMMENT '元数据内容',
                    `business_keywords` TEXT COMMENT '业务关键词',
                    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
                )
                ENGINE=OLAP
                UNIQUE KEY(`db_name`, `table_name`, `metadata_type`)
                COMMENT '业务元数据表'
                DISTRIBUTED BY HASH(`db_name`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
                """
                execute_query(create_table_query, "doris_metadata")
                logger.info("已创建business_metadata表")
                
                # 恢复备份的数据
                if old_table_backup and locals().get('old_data'):
                    logger.info("开始恢复备份的元数据...")
                    count = 0
                    for record in old_data:
                        try:
                            # 转义元数据值中的单引号
                            safe_metadata_value = record['metadata_value'].replace("'", "''") if record.get('metadata_value') else ''
                            
                            # 构建插入语句,包括新的business_keywords列（设为NULL）
                            restore_query = f"""
                            INSERT INTO `doris_metadata`.`business_metadata` 
                            (`db_name`, `table_name`, `metadata_type`, `metadata_value`, `business_keywords`, `update_time`)
                            VALUES
                            ('{record.get('db_name', '')}', '{record.get('table_name', '')}', 
                             '{record.get('metadata_type', '')}', '{safe_metadata_value}', 
                             NULL, '{record.get('update_time', 'NOW()')}')
                            """
                            execute_query(restore_query, "doris_metadata")
                            count += 1
                        except Exception as e:
                            logger.error(f"恢复元数据记录时出错: {str(e)}")
                    
                    logger.info(f"成功恢复 {count} 条备份的元数据记录")
            
            # 也检查并创建table_metadata表
            check_table_meta_query = "SHOW TABLES FROM `doris_metadata` LIKE 'table_metadata'"
            table_meta_exists = execute_query(check_table_meta_query)
            
            if not table_meta_exists:
                # 创建table_metadata表
                create_table_meta_query = """
                CREATE TABLE IF NOT EXISTS `doris_metadata`.`table_metadata` (
                    `database_name` VARCHAR(100) NOT NULL COMMENT '数据库名',
                    `table_name` VARCHAR(100) NOT NULL COMMENT '表名',
                    `table_type` VARCHAR(50) NULL COMMENT '表类型',
                    `engine` VARCHAR(50) NULL COMMENT '存储引擎',
                    `table_comment` TEXT NULL COMMENT '表注释',
                    `column_info` TEXT NULL COMMENT '列信息',
                    `partition_info` TEXT NULL COMMENT '分区信息',
                    `business_summary` TEXT NULL COMMENT '业务含义',
                    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
                ) 
                ENGINE = OLAP 
                UNIQUE KEY(`database_name`, `table_name`) 
                COMMENT '表结构元数据' 
                DISTRIBUTED BY HASH(`database_name`) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1"
                );
                """
                execute_query(create_table_meta_query, "doris_metadata")
                logger.info("已创建table_metadata表")
            
            # 转义元数据值中的单引号,防止SQL注入
            safe_metadata_value = metadata_value.replace("'", "''")
            
            # 根据metadata_type设置business_keywords值
            business_keywords_value = "NULL"
            if metadata_type == "business_keywords":
                business_keywords_value = f"'{safe_metadata_value}'"
                
            # 插入或更新元数据,包含business_keywords列
            upsert_query = f"""
            INSERT INTO `doris_metadata`.`business_metadata` 
            (`db_name`, `table_name`, `metadata_type`, `metadata_value`, `business_keywords`, `update_time`)
            VALUES
            ('{db_name}', '{table_name}', '{metadata_type}', '{safe_metadata_value}', {business_keywords_value}, NOW())
            """
            execute_query(upsert_query, "doris_metadata")
            
            logger.info(f"已保存数据库 {db_name} 表 {table_name} 的 {metadata_type} 元数据")
        except Exception as e:
            logger.error(f"保存业务元数据时出错: {str(e)}")
            # 不要抛出异常,以避免影响主流程
            
    def save_table_metadata(self, db_name: str, table_name: str, table_info: Dict[str, Any]) -> None:
        """
        保存表结构元数据到table_metadata表
        
        Args:
            db_name: 数据库名称
            table_name: 表名称
            table_info: 表结构信息字典，包含表类型、存储引擎、表注释、列信息等
        """
        try:
            # 首先确保doris_metadata数据库和table_metadata表存在
            check_db_query = "SHOW DATABASES LIKE 'doris_metadata'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                create_db_query = "CREATE DATABASE IF NOT EXISTS `doris_metadata`"
                execute_query(create_db_query)
                logger.info("已创建元数据库 doris_metadata")
            
            # 检查table_metadata表是否存在
            check_table_query = "SHOW TABLES FROM `doris_metadata` LIKE 'table_metadata'"
            table_exists = execute_query(check_table_query)
            
            if not table_exists:
                # 创建table_metadata表
                create_table_query = """
                CREATE TABLE IF NOT EXISTS `doris_metadata`.`table_metadata` (
                    `database_name` VARCHAR(100) NOT NULL COMMENT '数据库名',
                    `table_name` VARCHAR(100) NOT NULL COMMENT '表名',
                    `table_type` VARCHAR(50) NULL COMMENT '表类型',
                    `engine` VARCHAR(50) NULL COMMENT '存储引擎',
                    `table_comment` TEXT NULL COMMENT '表注释',
                    `column_info` TEXT NULL COMMENT '列信息',
                    `partition_info` TEXT NULL COMMENT '分区信息',
                    `business_summary` TEXT NULL COMMENT '业务含义',
                    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
                ) 
                ENGINE = OLAP 
                UNIQUE KEY(`database_name`, `table_name`) 
                COMMENT '表结构元数据' 
                DISTRIBUTED BY HASH(`database_name`) BUCKETS 3
                PROPERTIES (
                    "replication_num" = "1"
                );
                """
                execute_query(create_table_query, "doris_metadata")
                logger.info("已创建table_metadata表")
            
            # 从table_info提取所需信息并转义单引号
            table_type = table_info.get('table_type', '').replace("'", "''")
            engine = table_info.get('engine', '').replace("'", "''")
            table_comment = table_info.get('comment', '').replace("'", "''")
            
            # 将列信息转换为JSON字符串
            column_info = json.dumps(table_info.get('columns', []), ensure_ascii=False).replace("'", "''")
            
            # 获取分区信息
            partition_info = json.dumps(table_info.get('partition_info', {}), ensure_ascii=False).replace("'", "''")
            
            # 尝试获取业务摘要信息
            business_summary = ""
            try:
                table_metadata = self.get_business_metadata_for_table(db_name, table_name)
                if isinstance(table_metadata, dict) and 'summary' in table_metadata:
                    summary_dict = table_metadata['summary']
                    # 将字典转换为JSON，或者如果是字符串则直接使用
                    if isinstance(summary_dict, dict):
                        business_summary = json.dumps(summary_dict, ensure_ascii=False).replace("'", "''")
                    elif isinstance(summary_dict, str):
                        business_summary = summary_dict.replace("'", "''")
            except Exception as e:
                logger.warning(f"获取表{db_name}.{table_name}的业务摘要时出错: {str(e)}")
            
            # 插入或更新表结构元数据
            upsert_query = f"""
            INSERT INTO `doris_metadata`.`table_metadata` 
            (`database_name`, `table_name`, `table_type`, `engine`, `table_comment`, 
             `column_info`, `partition_info`, `business_summary`, `update_time`)
            VALUES
            ('{db_name}', '{table_name}', '{table_type}', '{engine}', '{table_comment}', 
             '{column_info}', '{partition_info}', '{business_summary}', NOW())
            """
            
            execute_query(upsert_query, "doris_metadata")
            logger.info(f"已保存表 {db_name}.{table_name} 的结构元数据到table_metadata表")
        except Exception as e:
            logger.error(f"保存表结构元数据时出错: {str(e)}")
            # 不要抛出异常,以避免影响主流程
            
    def get_table_partition_info(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """
        获取表的分区信息
        
        Args:
            db_name: 数据库名称
            table_name: 表名称
            
        Returns:
            Dict: 分区信息
        """
        try:
            # 获取分区信息
            query = f"""
            SELECT 
                PARTITION_NAME,
                PARTITION_EXPRESSION,
                PARTITION_DESCRIPTION,
                TABLE_ROWS
            FROM 
                information_schema.partitions
            WHERE 
                TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{table_name}'
            """
            
            partitions = execute_query(query)
            
            if not partitions:
                return {}
                
            partition_info = {
                "has_partitions": True,
                "partitions": []
            }
            
            for part in partitions:
                partition_info["partitions"].append({
                    "name": part.get("PARTITION_NAME", ""),
                    "expression": part.get("PARTITION_EXPRESSION", ""),
                    "description": part.get("PARTITION_DESCRIPTION", ""),
                    "rows": part.get("TABLE_ROWS", 0)
                })
                
            return partition_info
        except Exception as e:
            logger.error(f"获取表{db_name}.{table_name}的分区信息时出错: {str(e)}")
            return {}

    def get_business_metadata_from_database(self, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        从数据库获取业务元数据
        
        Args:
            db_name: 数据库名称,默认为当前数据库
            
        Returns:
            Dict: 业务元数据
        """
        # 如果没有指定数据库名,使用当前数据库
        if db_name is None:
            db_name = self.db_name
            
        # 如果仍然没有数据库名,返回空结果
        if not db_name:
            logger.warning("未指定数据库名,无法获取业务元数据")
            return {}
            
        try:
            # 从缓存中获取
            cache_key = f"business_metadata_{db_name}"
            if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
                logger.info(f"从缓存中获取数据库 {db_name} 的业务元数据")
                return self.metadata_cache[cache_key]
            
            # 确保元数据表存在
            check_db_query = "SHOW DATABASES LIKE 'doris_metadata'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                logger.warning("元数据数据库不存在,无法获取业务元数据")
                return {}
                
            check_table_query = "SHOW TABLES FROM `doris_metadata` LIKE 'business_metadata'"
            table_exists = execute_query(check_table_query)
            
            if not table_exists:
                logger.warning("元数据表不存在,无法获取业务元数据")
                return {}
            
            # 直接从数据库获取已保存的业务元数据
            query = f"""
            SELECT metadata_value 
            FROM {self.metadata_db}.business_metadata 
            WHERE db_name = '{db_name}' 
            AND table_name = '' 
            AND metadata_type = 'business_summary'
            LIMIT 1
            """
            
            logger.info(f"执行业务元数据查询: {query}")
            
            try:
                result = execute_query(query)
                logger.info(f"元数据查询结果: {result}")
                
                if result and len(result) > 0 and 'metadata_value' in result[0]:
                    try:
                        metadata_value = result[0]['metadata_value']
                        logger.info(f"解析业务元数据JSON: {metadata_value[:100]}...")
                        
                        db_metadata = json.loads(metadata_value)
                        if isinstance(db_metadata, dict) and len(db_metadata) > 0:
                            logger.info(f"成功从元数据库获取 {db_name} 的业务元数据")
                            
                            # 更新缓存
                            self.metadata_cache[cache_key] = db_metadata
                            self.metadata_cache_time[cache_key] = datetime.now()
                            
                            return db_metadata
                    except json.JSONDecodeError as e:
                        logger.warning(f"解析元数据JSON时出错: {str(e)}, 原始数据: {metadata_value[:100]}...")
                else:
                    # 尝试执行更详细的调试
                    debug_query = f"SELECT COUNT(*) as count FROM {self.metadata_db}.business_metadata"
                    debug_result = execute_query(debug_query)
                    logger.info(f"元数据表总记录数: {debug_result}")
                    
                    # 检查是否有特定db_name的记录
                    db_debug_query = f"SELECT COUNT(*) as count FROM {self.metadata_db}.business_metadata WHERE db_name = '{db_name}'"
                    db_debug_result = execute_query(db_debug_query)
                    logger.info(f"数据库 {db_name} 的元数据记录数: {db_debug_result}")
                
                logger.info(f"元数据库中没有找到 {db_name} 的业务元数据摘要,将通过LLM生成")
            except Exception as e:
                logger.warning(f"查询元数据时出错: {str(e)},将通过LLM生成新的元数据")
            
            # 获取数据库所有表
            tables = self.get_database_tables(db_name)
            logger.info(f"为 {db_name} 总结业务元数据,发现 {len(tables)} 个表")
            
            # 收集所有表的元数据
            tables_metadata = []
            for table_name in tables:
                schema = self.get_table_schema(table_name, db_name)
                if schema:  # 只添加成功获取的表结构
                    tables_metadata.append(schema)
            
            # 提取SQL模式
            sql_patterns = self.extract_common_sql_patterns(limit=50)
            
            # 准备表结构信息文本
            tables_info = ""
            for table_meta in tables_metadata:
                table_name = table_meta.get("name", "")
                table_comment = table_meta.get("comment", "")
                columns = table_meta.get("columns", [])
                
                tables_info += f"- 表名: {table_name} (注释: {table_comment})\n"
                tables_info += "  列:\n"
                for column in columns:
                    name = column.get("name", "")
                    type = column.get("type", "")
                    comment = column.get("comment", "")
                    tables_info += f"    - {name} ({type}) - {comment}\n"
                tables_info += "\n"
            
            # 准备SQL模式信息文本
            sql_patterns_text = ""
            if isinstance(sql_patterns, list):
                for pattern in sql_patterns[:5]:  # 最多展示5个模式
                    sql_type = pattern.get("type", "未知")
                    pattern_text = pattern.get("pattern", "")
                    frequency = pattern.get("frequency", 0)
                    sql_patterns_text += f"- {sql_type}查询 (频率: {frequency}):\n  {pattern_text}\n\n"
            
            # 使用提示词模板
            system_prompt = BUSINESS_METADATA_PROMPTS["system"]
            user_prompt = BUSINESS_METADATA_PROMPTS["user"].format(
                db_name=db_name,
                tables_info=tables_info,
                sql_patterns=sql_patterns_text
            )
            
            # 创建日志目录
            from pathlib import Path
            project_root = Path(__file__).parents[3]
            log_dir = project_root / "log" / "llm_calls"
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 记录LLM请求
            llm_request_log = {
                "function": "summarize_business_metadata",
                "db_name": db_name,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "timestamp": datetime.now().isoformat()
            }
            
            # 保存请求日志
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            request_log_path = log_dir / f"{timestamp}_business_metadata_request.json"
            with open(request_log_path, 'w', encoding='utf-8') as f:
                json.dump(llm_request_log, f, ensure_ascii=False, indent=2)
            
            logger.info("调用LLM生成业务元数据摘要")
            logger.info(f"LLM请求日志保存到: {request_log_path}")
            
            # 调用LLM
            try:
                from src.utils.llm_client import get_llm_client, Message
                
                client = get_llm_client(stage="metadata")
                
                # 如果LLM客户端为None（可能是程序正在退出）,返回默认值
                if client is None:
                    logger.warning("无法获取LLM客户端,可能是程序正在退出")
                    return {}
                
                response = client.chat([
                    Message.system(system_prompt),
                    Message.user(user_prompt)
                ])
                
                if not response or not response.content:
                    logger.warning("LLM响应为空")
                    return {}
                
                # 解析LLM回复获取业务元数据
                try:
                    result = self._extract_json_from_llm_response(response.content)
                except Exception as e:
                    logger.error(f"解析LLM响应提取JSON时出错: {str(e)}")
                    return {}
                
                # 检查解析结果
                if not result or result.get("extraction_failed", False):
                    logger.warning(f"从LLM响应中提取JSON失败: {response.content[:200]}...")
                    return {}
                
                # 确保必要字段存在
                if not isinstance(result, dict):
                    logger.warning("LLM返回的不是有效的字典对象")
                    return {}
                
                if "business_summary" not in result:
                    result["business_summary"] = f"数据库 {db_name} 的业务功能"
                
                if "tables_summary" not in result:
                    result["tables_summary"] = {}
                
                if "table_relationships" not in result:
                    result["table_relationships"] = {}
                
                if "business_keywords" not in result:
                    result["business_keywords"] = {}
                
                # 将业务元数据保存到元数据库
                try:
                    business_summary_json = json.dumps(result, ensure_ascii=False)
                    logger.info(f"保存生成的业务元数据摘要: {business_summary_json[:100]}...")
                    self._save_business_metadata(db_name, "", "business_summary", business_summary_json)
                    logger.info("成功保存业务元数据摘要")
                except Exception as e:
                    logger.warning(f"保存业务元数据时出错: {str(e)}")
                
                # 更新缓存
                self.metadata_cache[cache_key] = result
                self.metadata_cache_time[cache_key] = datetime.now()
                
                return result
            except Exception as e:
                logger.error(f"调用LLM生成业务元数据摘要时出错: {str(e)}")
                return {}
                
        except Exception as e:
            logger.error(f"获取数据库 {db_name} 的业务元数据时出错: {str(e)}")
            return {}

    def get_business_keywords_from_database(self, db_name=None):
        """从元数据库中获取业务关键词
        
        Args:
            db_name: 可选的数据库名称,如果提供则只返回该数据库的关键词
            
        Returns:
            Dict: 业务关键词字典，格式为 {keyword: confidence}
        """
        try:
            where_clause = f"WHERE db_name = '{db_name}'" if db_name else ""
            sql = f"""
            SELECT DISTINCT business_keywords 
            FROM {self.metadata_db}.business_metadata 
            {where_clause}
            """
            result = execute_query(sql)
            
            # 返回格式为字典 {keyword: confidence}
            keywords_dict = {}
            
            for row in result:
                if row and 'business_keywords' in row and row['business_keywords']:
                    try:
                        keywords = json.loads(row['business_keywords'])
                        if isinstance(keywords, list):
                            # 处理关键词列表格式
                            for item in keywords:
                                if isinstance(item, dict) and 'keyword' in item and 'confidence' in item:
                                    # 新格式: [{keyword: xxx, confidence: yyy}, ...]
                                    keywords_dict[item['keyword']] = item['confidence']
                                elif isinstance(item, str):
                                    # 旧格式: [keyword1, keyword2, ...]
                                    keywords_dict[item] = 0.7  # 默认置信度
                        elif isinstance(keywords, dict):
                            # 如果直接是字典格式 {keyword1: confidence1, ...}
                            keywords_dict.update(keywords)
                    except Exception as e:
                        logger.warning(f"解析业务关键词JSON出错: {str(e)}")
                        
            if not keywords_dict:
                # 如果没有找到关键词，返回空字典而不是空列表
                logger.info(f"数据库 {db_name} 未找到业务关键词")
                return {}
                
            return keywords_dict
        except Exception as e:
            logger.error(f"从元数据库获取业务关键词出错: {str(e)}")
            return {}

    def save_business_keywords(self, db_name: str, keywords: List[str]) -> bool:
        """保存业务关键词到元数据数据库
        
        Args:
            db_name: 数据库名称
            keywords: 业务关键词列表
            
        Returns:
            bool: 保存成功返回True,否则返回False
        """
        try:
            # 确保keywords是列表
            if not isinstance(keywords, list):
                logger.warning(f"保存的业务关键词必须是列表,当前为: {type(keywords)}")
                return False
                
            # 将关键词列表转换为JSON字符串
            keywords_json = json.dumps(keywords, ensure_ascii=False)
            
            # 保存到元数据数据库
            self._save_business_metadata(
                db_name, 
                "", 
                "business_keywords", 
                keywords_json
            )
            
            logger.info(f"成功保存数据库 {db_name} 的业务关键词,共 {len(keywords)} 个")
            return True
        except Exception as e:
            logger.error(f"保存业务关键词到元数据数据库出错: {str(e)}")
            return False

    def get_business_metadata_for_table(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """获取指定表的业务元数据
        
        Args:
            db_name: 数据库名称
            table_name: 表名称
            
        Returns:
            dict: 表的业务元数据
        """
        try:
            # 检查参数
            if not db_name or not table_name:
                logger.warning("获取表业务元数据时未指定数据库名或表名")
                return {}
                
            # 从缓存中获取
            cache_key = f"table_metadata_{db_name}_{table_name}"
            if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
                logger.info(f"从缓存中获取表 {db_name}.{table_name} 的业务元数据")
                return self.metadata_cache[cache_key]
                
            # 从元数据库查询
            query = f"""
            SELECT metadata_value 
            FROM {self.metadata_db}.business_metadata 
            WHERE db_name = '{db_name}' 
            AND table_name = '{table_name}' 
            AND metadata_type = 'table_summary'
            """
            
            result = execute_query(query)
            
            if result and len(result) > 0 and 'metadata_value' in result[0]:
                try:
                    table_metadata = json.loads(result[0]['metadata_value'])
                    
                    # 更新缓存
                    self.metadata_cache[cache_key] = table_metadata
                    self.metadata_cache_time[cache_key] = datetime.now()
                    
                    logger.info(f"成功从元数据库获取表 {db_name}.{table_name} 的业务元数据")
                    return table_metadata
                except json.JSONDecodeError as e:
                    logger.warning(f"解析表 {db_name}.{table_name} 的业务元数据JSON时出错: {str(e)}")
            
            # 如果表级元数据不存在,尝试从数据库级元数据中提取
            db_metadata = self.get_business_metadata_from_database(db_name)
            tables_summary = db_metadata.get("tables_summary", {})
            
            # 处理tables_summary可能是列表的情况
            if isinstance(tables_summary, list):
                logger.info(f"从数据库元数据中提取表摘要，检测到列表格式，共 {len(tables_summary)} 个表")
                # 在列表中查找匹配的表
                for table_info in tables_summary:
                    if isinstance(table_info, dict) and table_info.get("name") == table_name:
                        # 找到匹配的表，创建表元数据
                        table_summary = {k: v for k, v in table_info.items() if k != "name"}
                        table_metadata = {
                            "summary": table_summary,
                            "update_time": datetime.now().isoformat()
                        }
                        
                        # 保存表级元数据以便下次直接获取
                        self._save_business_metadata(
                            db_name, 
                            table_name, 
                            "table_summary", 
                            json.dumps(table_metadata, ensure_ascii=False)
                        )
                        
                        # 更新缓存
                        self.metadata_cache[cache_key] = table_metadata
                        self.metadata_cache_time[cache_key] = datetime.now()
                        
                        logger.info(f"已从数据库级元数据列表中提取并保存表 {db_name}.{table_name} 的业务元数据")
                        return table_metadata
                
                # 如果未找到匹配的表
                logger.warning(f"在数据库元数据列表中未找到表 {table_name}")
                return {}
            
            # 处理tables_summary是字典的情况
            elif isinstance(tables_summary, dict) and table_name in tables_summary:
                table_metadata = {
                    "summary": tables_summary[table_name],
                    "update_time": datetime.now().isoformat()
                }
                
                # 保存表级元数据以便下次直接获取
                self._save_business_metadata(
                    db_name, 
                    table_name, 
                    "table_summary", 
                    json.dumps(table_metadata, ensure_ascii=False)
                )
                
                # 更新缓存
                self.metadata_cache[cache_key] = table_metadata
                self.metadata_cache_time[cache_key] = datetime.now()
                
                logger.info(f"已从数据库级元数据中提取并保存表 {db_name}.{table_name} 的业务元数据")
                return table_metadata
            
            logger.warning(f"未找到表 {db_name}.{table_name} 的业务元数据")
            return {}
        except Exception as e:
            logger.error(f"获取表{db_name}.{table_name}的业务元数据时出错: {str(e)}")
            return {}