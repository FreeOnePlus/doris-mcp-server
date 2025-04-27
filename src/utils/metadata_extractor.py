"""
元数据提取工具

负责从数据库中提取表结构、关系等元数据
"""

import os
import json
import pandas as pd
import re
import time
from typing import Dict, List, Any, Optional, Tuple, Union
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
from src.utils.db import execute_query_df, execute_query, ENABLE_MULTI_DATABASE, MULTI_DATABASE_NAMES
from src.prompts.metadata_schema import METADATA_DB_NAME, METADATA_TABLES, CREATE_DATABASE_SQL

class MetadataExtractor:
    """Apache Doris元数据提取器"""
    
    def __init__(self, db_name: str = None):
        """
        初始化元数据提取器
        
        Args:
            db_name: 默认数据库名称,如果不指定则使用当前连接的数据库
        """
        # 从环境变量获取配置
        self.db_name = db_name or os.getenv("DB_DATABASE", "")
        self.metadata_db = METADATA_DB_NAME  # 使用常量
        
        # 缓存系统
        self.metadata_cache = {}
        self.metadata_cache_time = {}
        self.cache_ttl = int(os.getenv("METADATA_CACHE_TTL", "3600"))  # 默认缓存1小时
        
        # 刷新时间
        self.last_refresh_time = None
        
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
        
        # 排除的系统数据库列表
        self.excluded_databases = self._load_excluded_databases()
        
        logger.info(f"元数据提取器初始化完成,默认数据库:{self.db_name},元数据库:{self.metadata_db}")
        logger.info(f"已排除系统数据库:{self.excluded_databases}")
        
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
                
                # 过滤掉排除的数据库
                all_dbs = [db for db in all_dbs if db not in self.excluded_databases]
                logger.info(f"未配置多数据库列表，从系统中获取数据库列表: {all_dbs}")
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
                
                # 过滤掉排除的数据库
                db_names = [db for db in db_names if db not in self.excluded_databases]
                logger.info(f"使用配置的多数据库列表: {db_names}")
                return db_names
        else:
            # 只返回当前数据库
            if self.db_name in self.excluded_databases:
                logger.warning(f"当前数据库 {self.db_name} 在排除列表中，可能无法正常获取元数据")
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
        获取业务元数据摘要,优先从元数据库获取
        
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
                return default_metadata
            except Exception as e:
                logger.warning(f"查询元数据时出错: {str(e)}")
                return default_metadata
        except Exception as e:
            logger.error(f"生成业务元数据摘要时出错: {str(e)}")
            return default_metadata
    
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
            if not target_databases:
                logger.warning("未获取到任何目标数据库，请检查配置")
                return False
                
            logger.info(f"将刷新以下数据库的元数据: {target_databases}")
            
            # 检查元数据表是否存在
            check_db_query = f"SHOW DATABASES LIKE '{METADATA_DB_NAME}'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                logger.warning(f"元数据数据库 {METADATA_DB_NAME} 不存在,将创建并执行全量刷新")
                force = True
            else:
                check_table_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'business_metadata'"
                table_exists = execute_query(check_table_query)
                
                if not table_exists:
                    logger.warning("元数据表不存在,将创建并执行全量刷新")
                    force = True
            
            # 全局成功标志
            overall_success = True
            
            # 先检查表结构元数据表是否存在
            check_table_metadata_table_query = f"SHOW TABLES FROM `doris_metadata` LIKE 'table_metadata'"
            table_metadata_table_exists = False
            try:
                table_metadata_table_exists = bool(execute_query(check_table_metadata_table_query))
            except Exception as e:
                logger.warning(f"检查表结构元数据表是否存在时出错: {str(e)}")
                
            if not table_metadata_table_exists:
                logger.warning("表结构元数据表不存在，需要执行全量刷新")
                force = True
                
            # 不再检查关键词表
            
            # 如果是强制刷新模式，记录日志并继续
            if force:
                logger.info("执行强制全量刷新模式")
            else:
                logger.info("执行增量刷新模式，将只刷新缺失元数据")
            
            # 识别真正需要刷新的数据库（在增量模式下）
            databases_to_refresh = []
            if not force:
                # 获取已有业务元数据摘要的数据库
                existing_metadata_dbs = set()
                try:
                    query = f"""
                    SELECT DISTINCT db_name FROM {METADATA_DB_NAME}.business_metadata 
                    WHERE metadata_type = 'business_summary'
                    """
                    result = execute_query(query)
                    if result:
                        existing_metadata_dbs = {row.get('db_name', '') for row in result}
                        logger.info(f"已有业务元数据摘要的数据库: {existing_metadata_dbs}")
                except Exception as e:
                    logger.warning(f"获取已有业务元数据摘要数据库时出错: {str(e)}")
                
                # 获取已有表结构元数据的数据库
                existing_table_metadata_dbs = set()
                if table_metadata_table_exists:
                    try:
                        query = f"""
                        SELECT DISTINCT database_name FROM doris_metadata.table_metadata
                        """
                        result = execute_query(query)
                        if result:
                            existing_table_metadata_dbs = {row.get('database_name', '') for row in result}
                            logger.info(f"已有表结构元数据的数据库: {existing_table_metadata_dbs}")
                    except Exception as e:
                        logger.warning(f"获取已有表结构元数据数据库时出错: {str(e)}")
                
                # 不再获取和检查业务关键词数据库
                # 删除关键词数据库检查相关代码
                
                # 确定完全缺失元数据的数据库
                completely_missing_dbs = set(target_databases) - existing_metadata_dbs
                logger.info(f"完全缺失业务元数据摘要的数据库: {completely_missing_dbs}")
                
                # 将完全缺失元数据的数据库添加到需要刷新的列表
                databases_to_refresh.extend(completely_missing_dbs)
                
                # 检查目标数据库中是否有只缺失某一部分元数据的
                for db_name in target_databases:
                    if db_name in databases_to_refresh:
                        continue  # 已经在刷新列表中的跳过
                        
                    missing_parts = []
                    if db_name not in existing_metadata_dbs:
                        missing_parts.append("业务元数据摘要")
                    if db_name not in existing_table_metadata_dbs:
                        missing_parts.append("表结构元数据")
                    # 不再检查业务关键词
                        
                    if missing_parts:
                        logger.info(f"数据库 {db_name} 缺失以下元数据: {', '.join(missing_parts)}")
                        databases_to_refresh.append(db_name)
                    else:
                        logger.info(f"数据库 {db_name} 的元数据已完整，跳过刷新")
                
                # 如果没有找到任何需要刷新的数据库，直接返回成功
                if not databases_to_refresh:
                    logger.info("所有目标数据库的元数据已完整，无需刷新")
                    return True
                
                logger.info(f"需要刷新元数据的数据库: {databases_to_refresh}")
            else:
                # 强制刷新模式下刷新所有数据库
                databases_to_refresh = target_databases
            
            # 遍历所有需要刷新的数据库
            for db_name in databases_to_refresh:
                try:
                    logger.info(f"开始刷新数据库 {db_name} 的元数据")
                    
                    # 更新业务元数据摘要
                    success = self._update_sql_patterns_and_business_metadata(db_name, force)
                    
                    if success:
                        logger.info(f"成功刷新数据库 {db_name} 的业务元数据摘要")
                    else:
                        logger.warning(f"刷新数据库 {db_name} 的业务元数据摘要失败")
                        overall_success = False
                    
                    # 刷新表结构元数据
                    try:
                        logger.info(f"开始刷新数据库 {db_name} 的表结构元数据")
                        # 获取数据库中的所有表
                        tables = self.get_database_tables(db_name)
                        
                        if not tables:
                            logger.info(f"数据库 {db_name} 不包含任何表，跳过表结构元数据刷新")
                        else:
                            logger.info(f"数据库 {db_name} 包含 {len(tables)} 张表")
                            
                            # 强制刷新时清除表结构相关缓存
                            if force:
                                delete_table_metadata_query = f"""
                                DELETE FROM doris_metadata.table_metadata 
                                WHERE database_name = '{db_name}'
                                """
                                try:
                                    execute_query(delete_table_metadata_query)
                                    logger.info(f"已删除数据库 {db_name} 的所有现有表结构元数据记录")
                                except Exception as e:
                                    logger.warning(f"删除现有表结构元数据记录时出错: {str(e)}")
                            
                            # 查询已有表结构元数据
                            existing_tables = set()
                            if not force:
                                try:
                                    query = f"""
                                    SELECT table_name FROM doris_metadata.table_metadata 
                                    WHERE database_name = '{db_name}'
                                    """
                                    tables_with_metadata = execute_query(query)
                                    if tables_with_metadata:
                                        existing_tables = set(row.get('table_name', '') for row in tables_with_metadata)
                                        logger.info(f"数据库 {db_name} 已有 {len(existing_tables)} 张表的结构元数据")
                                except Exception as e:
                                    logger.warning(f"查询已有表结构元数据时出错: {str(e)}")
                            
                            # 对每个表刷新结构元数据
                            tables_refreshed = 0
                            for table_name in tables:
                                try:
                                    # 增量刷新时检查表是否已有元数据
                                    if not force and table_name in existing_tables:
                                        continue  # 跳过已有元数据的表
                                    
                                    # 获取表的完整结构信息
                                    table_schema = self.get_table_schema(table_name, db_name)
                                    if table_schema:
                                        # 保存表结构元数据
                                        self.save_table_metadata(db_name, table_name, table_schema)
                                        tables_refreshed += 1
                                    else:
                                        logger.warning(f"未能获取表 {db_name}.{table_name} 的结构信息")
                                except Exception as e:
                                    logger.warning(f"刷新表 {db_name}.{table_name} 的结构元数据时出错: {str(e)}")
                            
                            logger.info(f"成功刷新数据库 {db_name} 的 {tables_refreshed} 张表的结构元数据")
                    except Exception as e:
                        logger.error(f"刷新数据库 {db_name} 的表结构元数据时出错: {str(e)}")
                        overall_success = False
                    
                    # 直接跳过关键词刷新，记录日志
                    logger.info(f"根据要求，业务关键词将不在元数据初始化时保存，而在查询过程中保存")
                    
                    logger.info(f"数据库 {db_name} 的所有元数据刷新完成")
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
                
            # 尝试使用LLM生成业务元数据摘要
            logger.info("开始生成业务元数据摘要")
            metadata = self.summarize_business_metadata(db_name)
            
            # 检查业务元数据是否成功获取
            if not metadata or not isinstance(metadata, dict):
                logger.warning(f"获取数据库 {db_name} 的业务元数据失败")
                return False
            
            # 首先保存数据库级别的业务元数据摘要
            logger.info(f"保存数据库 {db_name} 的业务元数据摘要")
            try:
                # 构建数据库级别元数据
                db_metadata = {
                    "business_domain": metadata.get("business_domain", "未知业务领域"),
                    "core_entities": metadata.get("core_entities", []),
                    "business_processes": metadata.get("business_processes", []),
                    "update_time": datetime.now().isoformat()
                }
                
                # 将db_metadata转换为JSON字符串
                db_metadata_json = json.dumps(db_metadata, ensure_ascii=False)
                logger.info(f"数据库级元数据JSON(前100字符): {db_metadata_json[:100]}...")
                
                # 保存数据库级别的业务元数据摘要
                self._save_business_metadata(
                    db_name, 
                    "", 
                    "business_summary", 
                    db_metadata_json
                )
                logger.info(f"已保存数据库 {db_name} 的业务元数据摘要")
            except Exception as e:
                logger.warning(f"保存数据库 {db_name} 的业务元数据摘要时出错: {str(e)}")
                # 继续处理表级元数据
            
            # 获取表级别的摘要并单独保存每个表的元数据
            tables_summary = metadata.get("tables_summary", {})
            
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
                        metadata_json = json.dumps(table_metadata, ensure_ascii=False)
                        logger.info(f"表 {db_name}.{table_name} 的元数据JSON: {metadata_json[:100]}...")
                        
                        self._save_business_metadata(
                            db_name, 
                            table_name, 
                            "table_summary", 
                            metadata_json
                        )
                        logger.info(f"已保存表 {db_name}.{table_name} 的业务元数据摘要")
                    except Exception as e:
                        logger.warning(f"保存表 {db_name}.{table_name} 的业务元数据时出错: {str(e)}")
            else:
                logger.warning(f"未发现有效的表级别摘要信息，元数据可能不完整")
                
            return True
        except Exception as e:
            logger.error(f"生成业务元数据时出错: {str(e)}")
            return False
                
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
            check_db_query = f"SHOW DATABASES LIKE '{METADATA_DB_NAME}'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                execute_query(CREATE_DATABASE_SQL)
                logger.info(f"已创建元数据库 {METADATA_DB_NAME}")
            
            # 检查business_metadata表是否存在,检查表结构
            check_table_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'business_metadata'"
            table_exists = execute_query(check_table_query)
            
            old_table_backup = False
            
            # 如果表存在,检查列结构是否正确
            if table_exists:
                # 检查表结构
                desc_query = f"DESC `{METADATA_DB_NAME}`.`business_metadata`"
                table_structure = execute_query(desc_query)
                
                # 检查是否包含db_name列和business_keywords列
                column_names = [col.get('Field', '') for col in table_structure]
                
                # 如果表结构不正确但存在旧数据,先备份旧数据
                if ('db_name' in column_names) and ('business_keywords' not in column_names):
                    logger.info("检测到旧版元数据表结构,准备备份数据并升级表结构")
                    # 检查是否有数据需要备份
                    count_query = f"SELECT COUNT(*) as count FROM `{METADATA_DB_NAME}`.`business_metadata`"
                    count_result = execute_query(count_query)
                    
                    if count_result and count_result[0]['count'] > 0:
                        # 备份旧数据
                        backup_query = f"SELECT * FROM `{METADATA_DB_NAME}`.`business_metadata`"
                        old_data = execute_query(backup_query)
                        old_table_backup = True
                        logger.info(f"成功备份 {len(old_data)} 条旧元数据记录")
                
                # 如果表结构不正确,删除表并重新创建
                if 'db_name' not in column_names or 'business_keywords' not in column_names:
                    logger.warning("表business_metadata结构不正确,将重新创建")
                    drop_query = f"DROP TABLE IF EXISTS `{METADATA_DB_NAME}`.`business_metadata`"
                    execute_query(drop_query)
                    table_exists = False
                    
            # 如果表不存在,创建表
            if not table_exists:
                execute_query(METADATA_TABLES["business_metadata"], METADATA_DB_NAME)
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
                            INSERT INTO `{METADATA_DB_NAME}`.`business_metadata` 
                            (`db_name`, `table_name`, `metadata_type`, `metadata_value`, `business_keywords`, `update_time`)
                            VALUES
                            ('{record.get('db_name', '')}', '{record.get('table_name', '')}', 
                             '{record.get('metadata_type', '')}', '{safe_metadata_value}', 
                             NULL, '{record.get('update_time', 'NOW()')}')
                            """
                            execute_query(restore_query, METADATA_DB_NAME)
                            count += 1
                        except Exception as e:
                            logger.error(f"恢复元数据记录时出错: {str(e)}")
                    
                    logger.info(f"成功恢复 {count} 条备份的元数据记录")
            
            # 也检查并创建table_metadata表
            check_table_meta_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'table_metadata'"
            table_meta_exists = execute_query(check_table_meta_query)
            
            if not table_meta_exists:
                # 创建table_metadata表
                execute_query(METADATA_TABLES["table_metadata"], METADATA_DB_NAME)
                logger.info("已创建table_metadata表")
            
            # 确保元数据值使用正确的UTF-8编码
            metadata_value = self._ensure_utf8_encoding(metadata_value)
            logger.info(f"元数据值已转换为UTF-8编码: {metadata_value[:50]}...")
            
            # 转义元数据值中的单引号,防止SQL注入
            safe_metadata_value = metadata_value.replace("'", "''")
            
            # 根据metadata_type设置business_keywords值
            business_keywords_value = "NULL"
            if metadata_type == "business_keywords":
                business_keywords_value = f"'{safe_metadata_value}'"
                
            # 插入或更新元数据,包含business_keywords列
            upsert_query = f"""
            INSERT INTO `{METADATA_DB_NAME}`.`business_metadata` 
            (`db_name`, `table_name`, `metadata_type`, `metadata_value`, `business_keywords`, `update_time`)
            VALUES
            ('{db_name}', '{table_name}', '{metadata_type}', '{safe_metadata_value}', {business_keywords_value}, NOW())
            """
            
            execute_query(upsert_query, METADATA_DB_NAME)
            
            logger.info(f"已保存数据库 {db_name} 表 {table_name} 的 {metadata_type} 元数据")
        except Exception as e:
            logger.error(f"保存业务元数据时出错: {str(e)}")
            # 不要抛出异常,以避免影响主流程
            
    def save_table_metadata(self, db_name: str, table_name: str, table_info: Dict[str, Any]) -> None:
        """
        保存表的元数据到元数据库
        
        Args:
            db_name: 数据库名称
            table_name: 表名称
            table_info: 表信息字典
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
                # 创建表
                from src.prompts.metadata_schema import METADATA_TABLES
                execute_query(METADATA_TABLES["table_metadata"])
                logger.info("已创建表 doris_metadata.table_metadata")
            
            # 提取表信息
            table_type = table_info.get('type', '')
            engine = table_info.get('engine', '')
            table_comment = table_info.get('comment', '').replace("'", "''")
            
            # 序列化列信息JSON并确保UTF-8编码
            columns = table_info.get('columns', [])
            column_info_json = json.dumps(columns, ensure_ascii=False)
            column_info_json = self._ensure_utf8_encoding(column_info_json)
            
            # 序列化分区信息JSON并确保UTF-8编码
            partition_info = table_info.get('partition_info', {})
            partition_info_json = json.dumps(partition_info, ensure_ascii=False)
            partition_info_json = self._ensure_utf8_encoding(partition_info_json)
            
            # 检查该表是否已存在记录并获取现有的business_summary
            existing_query = f"""
            SELECT business_summary 
            FROM doris_metadata.table_metadata 
            WHERE database_name = '{db_name}' 
            AND table_name = '{table_name}'
            """
            existing_record = execute_query(existing_query)
            
            if existing_record and 'business_summary' in existing_record[0]:
                # 使用现有的业务摘要
                business_summary = existing_record[0]['business_summary']
                logger.info(f"使用表 {db_name}.{table_name} 的现有业务摘要")
            else:
                # 获取新的业务摘要
                business_info = self.get_business_metadata_for_table(db_name, table_name)
                business_summary = json.dumps(business_info, ensure_ascii=False) if business_info else '{}'
                logger.info(f"获取表 {db_name}.{table_name} 的新业务摘要")
            
            # 确保业务摘要使用UTF-8编码
            business_summary = self._ensure_utf8_encoding(business_summary)
            
            # 转义JSON字符串中的单引号，防止SQL注入
            business_summary = business_summary.replace("'", "''")
            column_info_json = column_info_json.replace("'", "''")
            partition_info_json = partition_info_json.replace("'", "''")
            
            # 插入或更新记录
            upsert_query = f"""
            INSERT INTO doris_metadata.table_metadata
            (database_name, table_name, table_type, engine, table_comment, column_info, partition_info, business_summary, update_time)
            VALUES
            ('{db_name}', '{table_name}', '{table_type}', '{engine}', '{table_comment}', 
             '{column_info_json}', '{partition_info_json}', '{business_summary}', NOW())
            """
            
            execute_query(upsert_query)
            logger.info(f"已保存表 {db_name}.{table_name} 的结构元数据")
            
        except Exception as e:
            logger.error(f"保存表 {db_name}.{table_name} 的结构元数据时出错: {str(e)}")
            # 不要抛出异常，避免中断刷新流程
            
    def has_table_metadata(self, table_name: str, db_name: Optional[str] = None) -> bool:
        """
        检查表是否已经有保存的元数据
        
        Args:
            table_name: 表名称
            db_name: 数据库名称，默认使用当前数据库
            
        Returns:
            如果表有保存的元数据则返回True，否则返回False
        """
        db_name = db_name or self.db_name
        if not db_name or not table_name:
            return False
            
        try:
            # 查询元数据表检查是否存在该表的元数据
            query = f"""
            SELECT COUNT(*) as count
            FROM {self.metadata_db}.table_metadata
            WHERE db_name = '{db_name}' AND table_name = '{table_name}'
            """
            
            result = execute_query(query)
            if result and result[0]["count"] > 0:
                return True
            return False
        except Exception as e:
            logger.error(f"检查表元数据是否存在时出错: {str(e)}")
            return False
            
    def _ensure_utf8_encoding(self, text: str) -> str:
        """
        确保文本使用UTF-8编码，避免乱码问题
        
        Args:
            text: 输入文本
            
        Returns:
            str: UTF-8编码处理后的文本
        """
        try:
            # 如果不是字符串类型，转换为字符串
            if not isinstance(text, str):
                text = str(text)
                
            # 确保UTF-8编码
            return text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        except Exception as e:
            logger.error(f"处理文本编码时出错: {str(e)}")
            return text  # 返回原始文本，避免处理失败时丢失数据
    
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
        获取数据库的业务元数据
        
        Args:
            db_name: 数据库名称,默认使用当前数据库
            
        Returns:
            Dict[str, Any]: 数据库的业务元数据
        """
        # 如果未指定数据库名称,使用默认数据库
        if not db_name:
            db_name = self.db_name
            
        if not db_name:
            logger.warning("未指定数据库名称,无法获取业务元数据")
            return {}
            
        try:
            # 从缓存中获取
            cache_key = f"business_metadata_{db_name}"
            if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
                logger.info(f"从缓存中获取数据库 {db_name} 的业务元数据")
                return self.metadata_cache[cache_key]
            
            # 首先检查元数据数据库是否存在
            check_db_query = f"SHOW DATABASES LIKE '{METADATA_DB_NAME}'"
            db_exists = execute_query(check_db_query)
            
            if not db_exists:
                logger.warning(f"元数据数据库 {METADATA_DB_NAME} 不存在,无法获取业务元数据")
                return {}
            
            # 检查business_metadata表是否存在
            check_table_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'business_metadata'"
            table_exists = execute_query(check_table_query)
            
            if not table_exists:
                logger.warning(f"元数据表 {METADATA_DB_NAME}.business_metadata 不存在,无法获取业务元数据")
                return {}
            
            # 直接从数据库获取已保存的业务元数据
            query = f"""
            SELECT metadata_value 
            FROM {METADATA_DB_NAME}.business_metadata 
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
                    debug_query = f"SELECT COUNT(*) as count FROM {METADATA_DB_NAME}.business_metadata"
                    debug_result = execute_query(debug_query)
                    logger.info(f"元数据表总记录数: {debug_result}")
                    
                    # 检查是否有特定db_name的记录
                    db_debug_query = f"SELECT COUNT(*) as count FROM {METADATA_DB_NAME}.business_metadata WHERE db_name = '{db_name}'"
                    db_debug_result = execute_query(db_debug_query)
                    logger.info(f"数据库 {db_name} 的元数据记录数: {db_debug_result}")
                
                logger.info(f"元数据库中没有找到 {db_name} 的业务元数据摘要，请调用refresh_metadata工具刷新元数据")
            except Exception as e:
                logger.warning(f"查询元数据时出错: {str(e)},请调用refresh_metadata工具刷新元数据")
            
            return db_metadata
        except Exception as e:
            logger.error(f"获取数据库 {db_name} 的业务元数据时出错: {str(e)}")
            return {}

    def get_business_keywords_from_database(self, db_name=None):
        """
        从元数据数据库获取业务关键词
        
        Args:
            db_name: 数据库名称,默认使用当前数据库
            
        Returns:
            Dict[str, float]: 关键词及其置信度的字典
        """
        try:
            # 使用默认数据库如果db_name未指定
            if db_name is None:
                db_name = self.db_name
            
            if not db_name:
                logger.warning("未指定数据库名称,无法获取业务关键词")
                return {}
            
            # 从缓存中获取
            cache_key = f"business_keywords_{db_name}"
            if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
                logger.info(f"从缓存中获取数据库 {db_name} 的业务关键词")
                return self.metadata_cache[cache_key]
            
            # 首先尝试从business_keywords表获取
            try:
                # 检查业务关键词表是否存在
                check_table_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'business_keywords'"
                table_exists = execute_query(check_table_query)
                
                if table_exists:
                    # 从business_keywords表获取关键词
                    query = f"""
                    SELECT keyword, confidence 
                    FROM {METADATA_DB_NAME}.business_keywords 
                    WHERE database_name = '{db_name}'
                    """
                    
                    result = execute_query(query)
                    
                    if result and len(result) > 0:
                        keywords_dict = {row.get('keyword', ''): row.get('confidence', 0.0) for row in result}
                        
                        # 更新缓存
                        self.metadata_cache[cache_key] = keywords_dict
                        self.metadata_cache_time[cache_key] = datetime.now()
                        
                        logger.info(f"成功从business_keywords表获取数据库 {db_name} 的 {len(keywords_dict)} 个业务关键词")
                        return keywords_dict
            except Exception as e:
                logger.warning(f"从business_keywords表获取业务关键词时出错: {str(e)},将尝试从business_metadata表获取")
            
            # 如果business_keywords表不存在或查询失败,尝试从business_metadata表获取
            try:
                # 从business_metadata表获取关键词
                query = f"""
                SELECT metadata_value 
                FROM {METADATA_DB_NAME}.business_metadata 
                WHERE db_name = '{db_name}' 
                AND table_name = '' 
                AND metadata_type = 'business_keywords'
                """
                
                result = execute_query(query)
                
                if result and 'metadata_value' in result[0]:
                    try:
                        metadata_value = result[0]['metadata_value']
                        keywords_list = json.loads(metadata_value)
                        
                        # 转换为字典格式,设置默认置信度为0.8
                        if isinstance(keywords_list, list):
                            keywords_dict = {keyword: 0.8 for keyword in keywords_list if keyword and isinstance(keyword, str)}
                            
                            # 更新缓存
                            self.metadata_cache[cache_key] = keywords_dict
                            self.metadata_cache_time[cache_key] = datetime.now()
                            
                            logger.info(f"成功从business_metadata表获取数据库 {db_name} 的 {len(keywords_dict)} 个业务关键词")
                            return keywords_dict
                    except json.JSONDecodeError as e:
                        logger.warning(f"解析业务关键词JSON时出错: {str(e)}")
            except Exception as e:
                logger.warning(f"从business_metadata表获取业务关键词时出错: {str(e)}")
            
            # 如果都不存在,从业务元数据中提取
            db_metadata = self.get_business_metadata_from_database(db_name)
            business_keywords = db_metadata.get("business_keywords", {})
            
            # 处理不同格式的业务关键词
            keywords_dict = {}
            if isinstance(business_keywords, list):
                # 如果是列表格式,设置默认置信度为0.8
                keywords_dict = {keyword: 0.8 for keyword in business_keywords if keyword and isinstance(keyword, str)}
            elif isinstance(business_keywords, dict):
                # 如果是字典格式,直接使用
                keywords_dict = {k: v for k, v in business_keywords.items() if k and isinstance(k, str)}
            
            # 更新缓存
            self.metadata_cache[cache_key] = keywords_dict
            self.metadata_cache_time[cache_key] = datetime.now()
            
            logger.info(f"从业务元数据中提取数据库 {db_name} 的 {len(keywords_dict)} 个业务关键词")
            return keywords_dict
        except Exception as e:
            logger.error(f"获取数据库 {db_name} 的业务关键词时出错: {str(e)}")
            return {}

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
            FROM {METADATA_DB_NAME}.business_metadata 
            WHERE db_name = '{db_name}' 
            AND table_name = '{table_name}' 
            AND metadata_type = 'table_summary'
            """
            
            result = execute_query(query)
            
            if result and 'metadata_value' in result[0]:
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

    def save_business_keywords(self, db_name: str, keywords: Union[List[str], List[Dict[str, Any]]]) -> bool:
        """
        保存业务关键词到元数据数据库
        
        Args:
            db_name: 数据库名称
            keywords: 业务关键词列表或包含关键词信息的字典列表
        
        Returns:
            bool: 保存成功返回True,否则返回False
        """
        try:
            # 确保keywords是列表
            if not isinstance(keywords, list):
                logger.warning(f"保存的业务关键词必须是列表,当前为: {type(keywords)}")
                return False
            
            # 如果是简单的字符串列表，保存到业务元数据表中
            if all(isinstance(k, str) for k in keywords):
                # 将关键词列表转换为JSON字符串
                keywords_json = json.dumps(keywords, ensure_ascii=False)
                
                # 保存到元数据数据库
                self._save_business_metadata(
                    db_name, 
                    "", 
                    "business_keywords", 
                    keywords_json
                )
                
                logger.info(f"成功保存数据库 {db_name} 的业务关键词到元数据表,共 {len(keywords)} 个")
                return True
            
            # 如果是包含详细信息的字典列表，保存到业务关键词表中
            elif all(isinstance(k, dict) for k in keywords):
                # 首先确保元数据表存在
                check_db_query = f"SHOW DATABASES LIKE '{METADATA_DB_NAME}'"
                db_exists = execute_query(check_db_query)
                
                if not db_exists:
                    # 创建数据库
                    execute_query(CREATE_DATABASE_SQL)
                    logger.info(f"已创建元数据数据库: {METADATA_DB_NAME}")
                
                # 检查业务关键词表是否存在
                check_table_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE 'business_keywords'"
                table_exists = execute_query(check_table_query)
                
                if not table_exists:
                    # 创建表
                    execute_query(METADATA_TABLES["business_keywords"])
                    logger.info("已创建business_keywords表")
                
                # 批量插入关键词
                for keyword_info in keywords:
                    keyword = keyword_info.get('keyword', '').replace("'", "''")
                    confidence = keyword_info.get('confidence', 0.5)
                    category = keyword_info.get('category', '未分类').replace("'", "''")
                    source = keyword_info.get('source', '用户输入').replace("'", "''")
                    
                    # 避免添加空关键词或过短的关键词
                    if not keyword or len(keyword) < 2:
                        continue
                        
                    # 插入或更新记录
                    upsert_query = f"""
                    INSERT INTO `{METADATA_DB_NAME}`.`business_keywords`
                    (`database_name`, `keyword`, `confidence`, `category`, `source`, `create_time`, `update_time`)
                    VALUES
                    ('{db_name}', '{keyword}', {confidence}, '{category}', '{source}', NOW(), NOW())
                    """
                    
                    try:
                        execute_query(upsert_query)
                    except Exception as e:
                        logger.warning(f"保存关键词 '{keyword}' 时出错: {str(e)}")
                
                logger.info(f"已保存 {len(keywords)} 个详细业务关键词到关键词表")
                return True
            else:
                logger.warning("关键词列表格式不一致，必须全部为字符串或全部为字典")
                return False
                
        except Exception as e:
            logger.error(f"保存业务关键词到元数据数据库出错: {str(e)}")
            return False