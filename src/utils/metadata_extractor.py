"""
元数据提取工具

负责从数据库中提取表结构、关系等元数据
"""

import os
import logging
import json
import pandas as pd
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import re

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 导入本地模块
from src.utils.db import execute_query_df, execute_query, get_db_connection
from src.utils.llm_client import get_llm_client, Message
from src.prompts.prompts import BUSINESS_METADATA_PROMPTS

class MetadataExtractor:
    """Apache Doris元数据提取器"""
    
    def __init__(self, db_name: Optional[str] = None):
        """
        初始化元数据提取器
        
        Args:
            db_name: 数据库名称，如果为None则从环境变量获取
        """
        self.db_name = db_name or os.getenv("DB_DATABASE", "")
        self.metadata_cache = {}
        self.metadata_cache_time = {}
        self.cache_ttl = int(os.getenv("METADATA_CACHE_TTL", "3600"))  # 默认缓存1小时
        
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
    
    def get_database_tables(self, db_name: Optional[str] = None) -> List[str]:
        """
        获取数据库中所有表的列表
        
        Args:
            db_name: 数据库名称，如果为None则使用当前数据库
            
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
            
            result = execute_query(query)
            logger.info(f"information_schema.tables查询结果: {result}")
            
            if not result:
                tables = []
            else:
                tables = [table['TABLE_NAME'] for table in result]
                logger.info(f"从information_schema.tables获取的表名: {tables}")
            
            # 更新缓存
            self.metadata_cache[cache_key] = tables
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return tables
        except Exception as e:
            logger.error(f"获取表列表时出错: {str(e)}")
            return []
    
    def get_table_schema(self, table_name: str, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取表的结构信息
        
        Args:
            table_name: 表名
            db_name: 数据库名称，如果为None则使用当前数据库
            
        Returns:
            表结构信息，包含列名、类型、是否允许空值、默认值、注释等
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
                # 确保使用实际的列值，而不是列名
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
            db_name: 数据库名称，如果为None则使用当前数据库
            
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
            db_name: 数据库名称，如果为None则使用当前数据库
            
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
            db_name: 数据库名称，如果为None则使用初始化时指定的数据库
            
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
            # 例如: 如果一个表有一个列名为 xxx_id，并且有另一个表名为 xxx，则可能是外键关系
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
                                    "confidence": "medium"  # 置信度不高，基于命名约定
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
    
    def extract_common_sql_patterns(self, limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
        """
        提取常见的SQL模式
        
        Args:
            limit: 最多获取多少条审计日志
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: SQL模式信息
        """
        try:
            # 获取审计日志
            audit_logs = self.get_recent_audit_logs(days=30, limit=limit)
            if audit_logs.empty:
                return {}
            
            # 按SQL类型分组处理
            patterns = {}
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
                
                # 如果是新模式，初始化
                if sql_type not in patterns:
                    patterns[sql_type] = []
                    
                # 查找是否有类似的模式
                found_similar = False
                for pattern in patterns[sql_type]:
                    if self._are_sqls_similar(simplified_sql, pattern['simplified_sql']):
                        pattern['count'] += 1
                        pattern['examples'].append(sql)
                        if comments:
                            pattern['comments'].append(comments)
                        found_similar = True
                        break
                        
                # 如果没有找到类似的模式，添加新模式
                if not found_similar:
                    patterns[sql_type].append({
                        'simplified_sql': simplified_sql,
                        'examples': [sql],
                        'comments': [comments] if comments else [],
                        'count': 1,
                        'tables': tables
                    })
                    
            # 按照频率排序
            for sql_type in patterns:
                patterns[sql_type] = sorted(patterns[sql_type], key=lambda x: x['count'], reverse=True)
                
            return patterns
        except Exception as e:
            logger.error(f"提取SQL模式时出错: {str(e)}")
            return {}
    
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
    
    def summarize_business_metadata(self, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        通过LLM总结业务元数据
        
        Args:
            db_name: 数据库名称，如果为None则使用初始化时指定的数据库
            
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
        
        try:
            # 获取数据库所有表
            tables = self.get_database_tables(db_name)
            logger.info(f"为 {db_name} 总结业务元数据，发现 {len(tables)} 个表")
            
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
            for sql_type, patterns in sql_patterns.items():
                if patterns:
                    sql_patterns_text += f"- {sql_type} 查询:\n"
                    for pattern in patterns[:3]:  # 每种类型最多展示3个模式
                        example = pattern["examples"][0] if pattern["examples"] else ""
                        comments = pattern["comments"][0] if pattern["comments"] else ""
                        sql_patterns_text += f"  * 频率: {pattern['count']}\n"
                        sql_patterns_text += f"    示例: {example[:100]}...\n"
                        if comments:
                            sql_patterns_text += f"    注释: {comments}\n"
                    sql_patterns_text += "\n"
            
            # 使用提示词模板
            system_prompt = BUSINESS_METADATA_PROMPTS["system"]
            user_prompt = BUSINESS_METADATA_PROMPTS["user"].format(
                db_name=db_name,
                tables_info=tables_info,
                sql_patterns=sql_patterns_text
            )
            
            # 创建日志目录
            # 获取项目根目录下的log目录
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
                llm_client = get_llm_client()
                messages = [
                    Message.system(system_prompt),
                    Message.user(user_prompt)
                ]
                response = llm_client.chat(messages)
                
                # 检查返回的内容是否有效
                if not response.content or response.content.strip() == "":
                    logger.error("LLM返回了空的响应内容")
                    return {"error": "LLM返回了空的响应内容", "tables_summary": []}
                
                # 记录LLM响应
                llm_response_log = {
                    "function": "summarize_business_metadata",
                    "db_name": db_name,
                    "response_content": response.content,
                    "timestamp": datetime.now().isoformat()
                }
                
                # 保存响应日志 - 使用相同的时间戳前缀，便于关联请求和响应
                response_log_path = log_dir / f"{timestamp}_business_metadata_response.json"
                try:
                    with open(response_log_path, 'w', encoding='utf-8') as f:
                        # 使用ensure_ascii=False确保中文正常显示
                        # 这里为了处理可能存在的特殊字符，我们先转义一些可能导致JSON格式错误的字符
                        safe_content = response.content.replace("\\", "\\\\").replace("\"", "\\\"")
                        # 对于标签类内容，也进行转义，避免被截断
                        safe_content = safe_content.replace("<", "\\<").replace(">", "\\>")
                        
                        # 更新日志对象
                        llm_response_log["response_content"] = safe_content
                        
                        json.dump(llm_response_log, f, ensure_ascii=False, indent=2)
                    
                    logger.info(f"LLM响应日志保存到: {response_log_path}")
                except Exception as log_error:
                    logger.error(f"保存LLM响应日志时出错: {str(log_error)}")
                    # 尝试替代方案：以文本形式保存
                    try:
                        text_log_path = log_dir / f"{timestamp}_business_metadata_response.txt"
                        with open(text_log_path, 'w', encoding='utf-8') as f:
                            f.write(f"Function: summarize_business_metadata\n")
                            f.write(f"DB Name: {db_name}\n")
                            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                            f.write(f"Response Content:\n{response.content}")
                        logger.info(f"LLM响应保存为文本文件: {text_log_path}")
                    except Exception as text_error:
                        logger.error(f"保存LLM响应文本日志时出错: {str(text_error)}")
                
                logger.info(f"LLM响应内容前100个字符: {response.content[:100]}...")
            except Exception as e:
                error_msg = str(e)
                logger.error(f"调用LLM生成业务元数据摘要时出错: {error_msg}")
                return {"error": f"调用LLM时出错: {error_msg}", "tables_summary": []}
            
            # 增强的JSON解析，更健壮地处理LLM响应
            result = self._parse_llm_json_response(response.content)
            
            # 确保结果包含必要的字段，即使解析失败
            if "tables_summary" not in result:
                result["tables_summary"] = []
            
            # 更新缓存
            self.metadata_cache[cache_key] = result
            self.metadata_cache_time[cache_key] = datetime.now()
            
            logger.info(f"成功生成业务元数据摘要，包含 {len(result.get('tables_summary', []))} 个表摘要")
            return result
        except Exception as e:
            error_msg = str(e)
            logger.error(f"总结业务元数据时出错: {error_msg}")
            # 返回至少包含空tables_summary的结构，避免后续处理错误
            return {"error": error_msg, "tables_summary": []}
    
    def _parse_llm_json_response(self, content: str) -> Dict[str, Any]:
        """
        解析LLM返回的JSON格式响应
        采用提取而非删除的策略，保留原始响应完整性
        
        Args:
            content: 包含JSON数据的字符串
            
        Returns:
            Dict[str, Any]: 解析后的JSON数据，如果解析失败则返回包含error字段的字典
        """
        if not content or not content.strip():
            logging.error("LLM返回的内容为空")
            return {"error": "empty_response", "message": "LLM返回内容为空"}
            
        # 记录原始响应的前100个字符，用于调试
        logging.info(f"原始LLM响应(前100字符): {content[:100]}")
        
        # 方法0: 优先使用多行JSON处理函数，它已经包含了处理```json代码块和<think>标签的逻辑
        result = self._handle_multiline_json(content)
        if result:
            logging.info("成功使用多行JSON处理提取有效JSON")
            return result
        
        # 方法1: 尝试修复可能的JSON格式问题并解析
        try:
            # 处理常见的格式问题
            fixed_content = content.replace("'", '"')  # 将单引号替换为双引号
            fixed_content = re.sub(r'(\w+):', r'"\1":', fixed_content)  # 将没有引号的键名加上引号
            
            # 尝试解析修复后的内容
            data = json.loads(fixed_content)
            logging.info("成功修复并解析JSON")
            return data
        except json.JSONDecodeError:
            logging.warning("修复JSON格式后仍无法解析")
        
        # 方法2: 尝试提取可能的JSON部分
        # 查找最外层的花括号
        try:
            start_idx = content.find('{')
            end_idx = content.rfind('}')
            
            if start_idx >= 0 and end_idx > start_idx:
                potential_json = content[start_idx:end_idx+1]
                data = json.loads(potential_json)
                logging.info("成功从内容中提取JSON部分")
                return data
        except json.JSONDecodeError:
            logging.warning("无法从内容中提取有效的JSON部分")
        
        # 方法3: 所有方法都失败，解析为业务领域和核心实体的字典
        logging.error(f"所有方法都无法提取有效JSON: {content[:200]}")
        
        # 尝试检测是否包含一些关键信息，如业务领域描述
        business_domain_match = re.search(r'(业务领域|business_domain|领域描述)[:：][\s,]*([^,\n]+)', content, re.IGNORECASE)
        
        # 返回一个包含可能提取到的业务领域信息的字典
        result = {
            "business_domain": business_domain_match.group(2).strip() if business_domain_match else "未识别的业务领域",
            "core_entities": [],
            "business_processes": []
        }
        
        return result
    
    def save_metadata_to_database(self, db_name: Optional[str] = None) -> bool:
        """
        将提取和总结的元数据保存到数据库中
        
        Args:
            db_name: 数据库名称，如果为None则使用初始化时指定的数据库
            
        Returns:
            bool: 操作是否成功
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("未指定数据库名称")
            return False
        
        try:
            # 使用统一的元数据库
            metadata_db = "doris_metadata"
            
            # 创建元数据数据库（如果不存在）
            create_db_query = f"CREATE DATABASE IF NOT EXISTS `{metadata_db}`"
            execute_query(create_db_query)
            
            # 创建表结构元数据表，添加database_name作为区分不同数据库的字段
            create_schema_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{metadata_db}`.`table_metadata` (
                `database_name` VARCHAR(100) NOT NULL COMMENT '数据库名称',
                `table_name` VARCHAR(100) NOT NULL COMMENT '表名',
                `table_type` VARCHAR(50) COMMENT '表类型',
                `engine` VARCHAR(50) COMMENT '引擎类型',
                `table_comment` TEXT COMMENT '表注释',
                `column_info` TEXT COMMENT '列信息 (JSON格式)',
                `partition_info` TEXT COMMENT '分区信息 (JSON格式)',
                `business_summary` TEXT COMMENT '业务摘要',
                `update_time` DATETIME COMMENT '更新时间'
            )
            ENGINE=OLAP
            DUPLICATE KEY(`database_name`, `table_name`)
            COMMENT '表结构元数据'
            DISTRIBUTED BY HASH(`database_name`, `table_name`) BUCKETS 3
            PROPERTIES("replication_num" = "1");
            """
            execute_query(create_schema_table_query)
            
            # 创建SQL模式表，添加database_name
            create_sql_patterns_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{metadata_db}`.`sql_patterns` (
                `pattern_id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '模式ID',
                `database_name` VARCHAR(100) NOT NULL COMMENT '数据库名称',
                `sql_type` VARCHAR(20) COMMENT 'SQL类型 (SELECT, INSERT等)',
                `simplified_sql` TEXT COMMENT '简化后的SQL',
                `examples` TEXT COMMENT 'SQL示例 (JSON格式)',
                `comments` TEXT COMMENT 'SQL注释 (JSON格式)',
                `frequency` INT COMMENT '出现频率',
                `tables` TEXT COMMENT '相关表 (JSON格式)',
                `update_time` DATETIME COMMENT '更新时间'
            )
            ENGINE=OLAP
            DUPLICATE KEY(`pattern_id`)
            COMMENT 'SQL查询模式'
            DISTRIBUTED BY HASH(`pattern_id`) BUCKETS 3
            PROPERTIES("replication_num" = "1");
            """
            execute_query(create_sql_patterns_table_query)
            
            # 创建业务元数据总结表，添加database_name
            create_business_metadata_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{metadata_db}`.`business_metadata` (
                `database_name` VARCHAR(100) NOT NULL COMMENT '数据库名称',
                `metadata_key` VARCHAR(50) NOT NULL COMMENT '元数据键',
                `metadata_value` TEXT COMMENT '元数据值 (JSON格式)',
                `update_time` DATETIME COMMENT '更新时间'
            )
            ENGINE=OLAP
            DUPLICATE KEY(`database_name`, `metadata_key`)
            COMMENT '业务元数据总结'
            DISTRIBUTED BY HASH(`database_name`, `metadata_key`) BUCKETS 3
            PROPERTIES("replication_num" = "1");
            """
            execute_query(create_business_metadata_table_query)
            
            # 获取当前时间
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取数据库中的所有表
            tables = self.get_database_tables(db_name)
            logger.info(f"开始处理{db_name}数据库中的表: {tables}")
            
            for table_name in tables:
                logger.info(f"正在处理表: {db_name}.{table_name}")
                
                # 检查表名是否为"TABLE_NAME"，这可能是误解
                if table_name == "TABLE_NAME":
                    logger.warning(f"发现表名为'TABLE_NAME'，这可能是列名被错误解析为表名，跳过处理")
                    continue
                
                # 获取完整表信息
                table_info = self.get_complete_table_info(table_name, db_name)
                
                # 确保获取到了表信息
                if not table_info:
                    logger.warning(f"跳过表 {table_name} 的元数据保存，无法获取表结构")
                    continue
                
                try:
                    # 将表结构信息保存到数据库
                    tables_saved = 0
                    
                    # 从表信息中提取各字段值
                    columns_json = json.dumps(table_info.get("columns", []), ensure_ascii=False)
                    partitions_json = json.dumps(table_info.get("partitions", []), ensure_ascii=False)
                    
                    # 明确指定列名的插入语句，添加database_name
                    table_insert_query = f"""
                    INSERT INTO `{metadata_db}`.`table_metadata`
                    (`database_name`, `table_name`, `table_type`, `engine`, `table_comment`, 
                     `column_info`, `partition_info`, `business_summary`, `update_time`)
                    VALUES (
                        '{db_name}',
                        '{table_name}',
                        '{table_info.get('table_type', '')}',
                        '{table_info.get('engine', '')}',
                        '{table_info.get('table_comment', '').replace("'", "''")}',
                        '{columns_json.replace("'", "''")}',
                        '{partitions_json.replace("'", "''")}',
                        '{table_info.get('business_summary', '').replace("'", "''")}',
                        '{current_time}'
                    )
                    """
                    execute_query(table_insert_query)
                    tables_saved += 1
                    
                except Exception as e:
                    logger.error(f"保存表 {table_name} 元数据时出错: {str(e)}")
            
            # 将SQL模式保存到数据库
            patterns_saved = 0
            patterns = self.extract_common_sql_patterns()
            
            for sql_type, pattern_list in patterns.items():
                for pattern in pattern_list:
                    try:
                        simplified_sql = pattern.get("simplified_sql", "").replace("'", "''")
                        examples_json = json.dumps(pattern.get("examples", []), ensure_ascii=False).replace("'", "''")
                        comments_json = json.dumps(pattern.get("comments", []), ensure_ascii=False).replace("'", "''")
                        tables_json = json.dumps(pattern.get("tables", []), ensure_ascii=False).replace("'", "''")
                        
                        # 明确指定列名的插入语句，添加database_name
                        pattern_insert_query = f"""
                        INSERT INTO `{metadata_db}`.`sql_patterns`
                        (`database_name`, `sql_type`, `simplified_sql`, `examples`, `comments`, 
                         `frequency`, `tables`, `update_time`)
                        VALUES (
                            '{db_name}',
                            '{sql_type}',
                            '{simplified_sql}',
                            '{examples_json}',
                            '{comments_json}',
                            {pattern.get('count', 0)},
                            '{tables_json}',
                            '{current_time}'
                        )
                        """
                        execute_query(pattern_insert_query)
                        patterns_saved += 1
                    except Exception as e:
                        logger.error(f"保存SQL模式时出错: {str(e)}")
            
            logger.info(f"成功保存 {patterns_saved}/{sum(len(patterns[t]) for t in patterns)} 个SQL模式")
            
            # 将业务元数据总结保存到数据库
            business_summary = self.summarize_business_metadata(db_name)
            
            # 获取表数量
            tables_count = len(tables)
            logger.info(f"为 {db_name} 总结业务元数据，发现 {tables_count} 个表")
            
            try:
                # 将业务概要保存为JSON
                tables_summary = business_summary.get("tables_summary", [])
                
                # 处理业务领域信息
                business_domain = business_summary.get("business_domain", "")
                core_entities = business_summary.get("core_entities", [])
                business_processes = business_summary.get("business_processes", [])
                
                # 将业务概要保存为JSON
                metadata_json = json.dumps({
                    "tables_count": tables_count,
                    "tables": tables,
                    "summary": {
                        "business_domain": business_domain,
                        "core_entities": core_entities,
                        "business_processes": business_processes
                    },
                    "tables_summary": tables_summary
                }, ensure_ascii=False).replace("'", "''").replace('"', '\\"')
                
                # 保存业务概要
                business_summary_query = f"""
                INSERT INTO `{metadata_db}`.`business_metadata`
                (`database_name`, `metadata_key`, `metadata_value`, `update_time`)
                VALUES ('{db_name}', 'business_summary', "{metadata_json}", '{current_time}')
                """
                execute_query(business_summary_query)
                
                # 保存数据库统计信息
                db_stats = {
                    "tables_count": tables_count,
                    "updated_at": current_time
                }
                db_stats_json = json.dumps(db_stats, ensure_ascii=False).replace("'", "''").replace('"', '\\"')
                
                # 使用明确的列名插入
                db_stats_query = f"""
                INSERT INTO `{metadata_db}`.`business_metadata` 
                (`database_name`, `metadata_key`, `metadata_value`, `update_time`)
                VALUES ('{db_name}', 'db_stats', "{db_stats_json}", '{current_time}')
                """
                execute_query(db_stats_query)
                
                logger.info(f"成功保存业务元数据")
                return True
            
            except Exception as e:
                logger.error(f"保存业务元数据时出错: {str(e)}")
                return False
            
        except Exception as e:
            logger.error(f"保存元数据到数据库时出错: {str(e)}")
            return False
    
    def get_table_partitions(self, table_name: str, db_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取表的分区信息
        
        Args:
            table_name: 表名
            db_name: 数据库名称，如果为None则使用当前数据库
            
        Returns:
            分区信息列表
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("未指定数据库名称")
            return []
        
        cache_key = f"partitions_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 使用information_schema.partitions表获取分区信息
            query = f"""
            SELECT 
                PARTITION_NAME,
                PARTITION_ORDINAL_POSITION,
                PARTITION_METHOD,
                PARTITION_EXPRESSION,
                PARTITION_DESCRIPTION,
                TABLE_ROWS,
                DATA_LENGTH,
                CREATE_TIME,
                PARTITION_COMMENT
            FROM 
                information_schema.partitions
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}'
                AND PARTITION_NAME IS NOT NULL
            ORDER BY 
                PARTITION_ORDINAL_POSITION
            """
            
            result = execute_query(query)
            
            if not result:
                logger.info(f"表 {db_name}.{table_name} 没有分区信息")
                return []
                
            # 创建结构化的分区信息
            partitions = []
            for part in result:
                # 确保使用实际的分区值，而不是列名
                partition_info = {
                    "name": part.get("PARTITION_NAME", ""),
                    "position": part.get("PARTITION_ORDINAL_POSITION", ""),
                    "method": part.get("PARTITION_METHOD", ""),
                    "expression": part.get("PARTITION_EXPRESSION", ""),
                    "description": part.get("PARTITION_DESCRIPTION", ""),
                    "table_rows": part.get("TABLE_ROWS", ""),
                    "create_time": part.get("CREATE_TIME", ""),
                    "update_time": "", # 分区没有独立的更新时间字段
                    "comment": part.get("PARTITION_COMMENT", "") or ""
                }
                partitions.append(partition_info)
            
            # 更新缓存
            self.metadata_cache[cache_key] = partitions
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return partitions
        except Exception as e:
            logger.error(f"获取分区信息时出错: {str(e)}")
            return []
    
    def get_complete_table_info(self, table_name: str, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取表的完整信息，包括表基本信息、列信息和分区信息
        
        Args:
            table_name: 表名
            db_name: 数据库名称，如果为None则使用初始化时指定的数据库
            
        Returns:
            Dict[str, Any]: 表的完整信息
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("未指定数据库名称")
            return {}
        
        cache_key = f"complete_info_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 获取表基本信息
            schema = self.get_table_schema(table_name, db_name)
            if not schema:
                logger.warning(f"无法获取表 {db_name}.{table_name} 的模式信息")
                return {}
            
            # 获取分区信息
            partitions = self.get_table_partitions(table_name, db_name)
            
            # 获取表类型和引擎信息
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
            
            # 检查是否是视图
            is_view = False
            if table_type_result and table_type_result[0].get("TABLE_TYPE") == "VIEW":
                is_view = True
                view_query = f"""
                SELECT VIEW_DEFINITION
                FROM information_schema.views
                WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = '{table_name}'
                """
                view_result = execute_query(view_query)
                if view_result:
                    schema['view_definition'] = view_result[0].get("VIEW_DEFINITION", "")
            
            # 添加表类型和引擎信息
            if table_type_result:
                schema['table_type'] = table_type_result[0].get("TABLE_TYPE", "")
                schema['engine'] = table_type_result[0].get("ENGINE", "")
            
            # 组装完整信息
            complete_info = {
                **schema,
                "partitions": partitions
            }
            
            # 更新缓存
            self.metadata_cache[cache_key] = complete_info
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return complete_info
        except Exception as e:
            logger.error(f"获取表完整信息时出错: {str(e)}")
            return {}
    
    def get_database_metadata(self, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取数据库的完整元数据
        
        Args:
            db_name: 数据库名称，如果为None则使用初始化时指定的数据库
            
        Returns:
            Dict[str, Any]: 数据库元数据
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("未指定数据库名称")
            return {}
        
        cache_key = f"db_metadata_{db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # 获取数据库基本信息
            db_query = f"""
            SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, 
                   DEFAULT_COLLATION_NAME
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME = '{db_name}'
            """
            db_df = execute_query_df(db_query)
            
            if db_df.empty:
                logger.warning(f"数据库 {db_name} 不存在")
                return {}
            
            db_info = db_df.iloc[0].to_dict()
            
            # 获取所有表
            tables = self.get_database_tables(db_name)
            
            # 获取表信息
            table_info_list = []
            for table_name in tables:
                table_info = self.get_table_schema(table_name, db_name)
                if table_info:
                    # 获取分区信息（如果有）
                    partitions = self.get_table_partitions(table_name, db_name)
                    if partitions:
                        table_info['partitions'] = partitions
                    
                    table_info_list.append(table_info)
            
            # 获取视图信息
            view_query = f"""
            SELECT TABLE_NAME, VIEW_DEFINITION
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = '{db_name}'
            """
            view_df = execute_query_df(view_query)
            
            views = []
            if not view_df.empty:
                for _, row in view_df.iterrows():
                    view_info = {
                        "name": row['TABLE_NAME'],
                        "definition": row['VIEW_DEFINITION']
                    }
                    views.append(view_info)
            
            # 组装完整元数据
            metadata = {
                "database_name": db_name,
                "character_set": db_info.get('DEFAULT_CHARACTER_SET_NAME', ''),
                "collation": db_info.get('DEFAULT_COLLATION_NAME', ''),
                "tables": table_info_list,
                "views": views,
                "table_count": len(table_info_list),
                "view_count": len(views)
            }
            
            # 更新缓存
            self.metadata_cache[cache_key] = metadata
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return metadata
        except Exception as e:
            logger.error(f"获取数据库元数据时出错: {str(e)}")
            return {}
    
    def _get_sql_type(self, sql: str) -> str:
        """
        获取SQL语句的类型
        
        Args:
            sql: SQL语句
            
        Returns:
            str: SQL类型
        """
        sql_upper = sql.upper().strip()
        
        # 定义可能的SQL类型
        sql_types = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "SHOW", "DESCRIBE", "EXPLAIN"]
        
        # 检查SQL类型
        for sql_type in sql_types:
            if sql_upper.startswith(sql_type):
                return sql_type
        
        return "OTHER"

    def refresh_metadata(self, db_name: Optional[str] = None, force: bool = False) -> bool:
        """
        增量刷新元数据，仅在必要时更新
        
        Args:
            db_name: 数据库名称，如果为None则使用初始化时指定的数据库
            force: 是否强制全量刷新
            
        Returns:
            bool: 操作是否成功
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("未指定数据库名称")
            return False
        
        # 使用统一的元数据库名称
        metadata_db = "doris_metadata"
        
        try:
            # 检查元数据表是否存在数据
            check_query = f"SELECT COUNT(*) as count FROM `{metadata_db}`.`table_metadata` WHERE database_name = '{db_name}'"
            try:
                result = execute_query(check_query)
                metadata_exists = result[0]['count'] > 0 if result else False
            except Exception as e:
                # 如果出错，可能是表不存在，自动进行全量刷新
                logger.warning(f"检查元数据表时出错，将进行全量刷新: {str(e)}")
                return self.save_metadata_to_database(db_name)
            
            # 若无数据或强制刷新，则全量刷新
            if force or not metadata_exists:
                logger.info(f"执行全量元数据刷新: force={force}, metadata_exists={metadata_exists}")
                return self.save_metadata_to_database(db_name)
            
            # 获取当前所有表
            current_tables = self.get_database_tables(db_name)
            
            # 获取元数据中的表
            tables_query = f"SELECT table_name FROM `{metadata_db}`.`table_metadata` WHERE database_name = '{db_name}'"
            stored_tables_result = execute_query(tables_query)
            stored_tables = [t['table_name'] for t in stored_tables_result] if stored_tables_result else []
            
            # 新增的表
            new_tables = [t for t in current_tables if t not in stored_tables]
            if new_tables:
                logger.info(f"发现 {len(new_tables)} 个新表需要添加元数据: {new_tables}")
            
            # 检查已存在表的更新时间
            tables_to_update = []
            for table in current_tables:
                if table in new_tables:
                    continue  # 新表会全部处理，不需要检查
                
                # 获取表结构的最后更新时间
                try:
                    table_info_query = f"""
                    SELECT TABLE_NAME, UPDATE_TIME 
                    FROM information_schema.tables 
                    WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = '{table}'
                    """
                    table_info = execute_query(table_info_query)
                    if not table_info:
                        continue
                    
                    db_update_time = table_info[0].get('UPDATE_TIME')
                    if not db_update_time:
                        continue
                    
                    # 获取元数据中的更新时间
                    metadata_query = f"SELECT update_time FROM `{metadata_db}`.`table_metadata` WHERE database_name = '{db_name}' AND table_name = '{table}'"
                    metadata_info = execute_query(metadata_query)
                    if not metadata_info:
                        tables_to_update.append(table)
                        continue
                    
                    metadata_update_time = metadata_info[0].get('update_time')
                    if not metadata_update_time:
                        tables_to_update.append(table)
                        continue
                
                    # 比较更新时间
                    if isinstance(db_update_time, str):
                        db_update_time = datetime.strptime(db_update_time, '%Y-%m-%d %H:%M:%S')
                    if isinstance(metadata_update_time, str):
                        metadata_update_time = datetime.strptime(metadata_update_time, '%Y-%m-%d %H:%M:%S')
                    
                    if db_update_time > metadata_update_time:
                        tables_to_update.append(table)
                except Exception as e:
                    logger.warning(f"检查表 {table} 更新时间时出错: {str(e)}")
                    # 如果检查出错，为安全起见添加到更新列表
                    tables_to_update.append(table)
            
            if tables_to_update:
                logger.info(f"发现 {len(tables_to_update)} 个表需要更新元数据: {tables_to_update}")
            
            # 执行增量更新
            if new_tables or tables_to_update:
                tables_to_process = new_tables + tables_to_update
                # 更新元数据
                self._update_selected_tables_metadata(db_name, tables_to_process)
                logger.info(f"成功增量更新 {len(tables_to_process)} 个表的元数据")
            else:
                logger.info("所有表的元数据均为最新，无需刷新")
            
            # 更新SQL模式和业务元数据
            self._update_sql_patterns_and_business_metadata(db_name)
            
            return True
        except Exception as e:
            logger.error(f"增量刷新元数据时出错: {str(e)}")
            # 出错时默认尝试全量刷新
            return self.save_metadata_to_database(db_name)
    
    def _update_selected_tables_metadata(self, db_name: str, tables: List[str]) -> bool:
        """
        更新指定表的元数据
        
        Args:
            db_name: 数据库名称
            tables: 要更新的表名列表
            
        Returns:
            bool: 操作是否成功
        """
        if not tables:
            return True
        
        metadata_db = "doris_metadata"
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tables_updated = 0
        
        for table_name in tables:
            logger.info(f"正在处理表: {db_name}.{table_name}")
            
            # 获取完整表信息
            table_info = self.get_complete_table_info(table_name, db_name)
            
            # 确保获取到了表信息
            if not table_info:
                logger.warning(f"跳过表 {table_name} 的元数据保存，无法获取表结构")
                continue
            
            try:
                # 将表结构信息保存到数据库
                # 从表信息中提取各字段值
                columns_json = json.dumps(table_info.get("columns", []), ensure_ascii=False)
                partitions_json = json.dumps(table_info.get("partitions", []), ensure_ascii=False)
                
                # 检查表是否已存在
                check_query = f"SELECT COUNT(*) as count FROM `{metadata_db}`.`table_metadata` WHERE database_name = '{db_name}' AND table_name = '{table_name}'"
                result = execute_query(check_query)
                exists = result[0]['count'] > 0 if result else False
                
                if exists:
                    # 更新现有记录
                    table_update_query = f"""
                    UPDATE `{metadata_db}`.`table_metadata`
                    SET 
                        `table_type` = '{table_info.get('table_type', '')}',
                        `engine` = '{table_info.get('engine', '')}',
                        `table_comment` = '{table_info.get('table_comment', '').replace("'", "''")}',
                        `column_info` = '{columns_json.replace("'", "''")}',
                        `partition_info` = '{partitions_json.replace("'", "''")}',
                        `business_summary` = '{table_info.get('business_summary', '').replace("'", "''")}',
                        `update_time` = '{current_time}'
                    WHERE `database_name` = '{db_name}' AND `table_name` = '{table_name}'
                    """
                else:
                    # 插入新记录
                    table_insert_query = f"""
                    INSERT INTO `{metadata_db}`.`table_metadata`
                    (`database_name`, `table_name`, `table_type`, `engine`, `table_comment`, 
                     `column_info`, `partition_info`, `business_summary`, `update_time`)
                    VALUES (
                        '{db_name}',
                        '{table_name}',
                        '{table_info.get('table_type', '')}',
                        '{table_info.get('engine', '')}',
                        '{table_info.get('table_comment', '').replace("'", "''")}',
                        '{columns_json.replace("'", "''")}',
                        '{partitions_json.replace("'", "''")}',
                        '{table_info.get('business_summary', '').replace("'", "''")}',
                        '{current_time}'
                    )
                    """
                
                # 执行SQL
                execute_query(table_update_query if exists else table_insert_query)
                tables_updated += 1
                
            except Exception as e:
                logger.error(f"保存表 {table_name} 元数据时出错: {str(e)}")
        
        logger.info(f"成功更新 {tables_updated}/{len(tables)} 个表的元数据")
        return tables_updated > 0

    def _update_sql_patterns_and_business_metadata(self, db_name: str) -> bool:
        """
        更新SQL模式和业务元数据
        
        Args:
            db_name: 数据库名称
            
        Returns:
            bool: 操作是否成功
        """
        metadata_db = "doris_metadata"
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # 更新SQL模式
            patterns = self.extract_common_sql_patterns()
            patterns_saved = 0
            
            # 先清空该数据库的现有模式
            try:
                execute_query(f"DELETE FROM `{metadata_db}`.`sql_patterns` WHERE database_name = '{db_name}'")
            except Exception as e:
                logger.warning(f"清空SQL模式表时出错: {str(e)}")
            
            for sql_type, pattern_list in patterns.items():
                for pattern in pattern_list:
                    try:
                        simplified_sql = pattern.get("simplified_sql", "").replace("'", "''")
                        examples_json = json.dumps(pattern.get("examples", []), ensure_ascii=False).replace("'", "''")
                        comments_json = json.dumps(pattern.get("comments", []), ensure_ascii=False).replace("'", "''")
                        tables_json = json.dumps(pattern.get("tables", []), ensure_ascii=False).replace("'", "''")
                        
                        # 明确指定列名的插入语句
                        pattern_insert_query = f"""
                        INSERT INTO `{metadata_db}`.`sql_patterns`
                        (`database_name`, `sql_type`, `simplified_sql`, `examples`, `comments`, 
                         `frequency`, `tables`, `update_time`)
                        VALUES (
                            '{db_name}',
                            '{sql_type}',
                            '{simplified_sql}',
                            '{examples_json}',
                            '{comments_json}',
                            {pattern.get('count', 0)},
                            '{tables_json}',
                            '{current_time}'
                        )
                        """
                        execute_query(pattern_insert_query)
                        patterns_saved += 1
                    except Exception as e:
                        logger.error(f"保存SQL模式时出错: {str(e)}")
            
            logger.info(f"成功保存 {patterns_saved}/{sum(len(patterns[t]) for t in patterns)} 个SQL模式")
            
            # 更新业务元数据
            business_summary = self.summarize_business_metadata(db_name)
            
            # 获取表数量
            tables_count = len(self.get_database_tables(db_name))
            logger.info(f"为 {db_name} 总结业务元数据，发现 {tables_count} 个表")
            
            # 先清空该数据库的现有业务元数据
            try:
                execute_query(f"DELETE FROM `{metadata_db}`.`business_metadata` WHERE database_name = '{db_name}'")
            except Exception as e:
                logger.warning(f"清空业务元数据表时出错: {str(e)}")
            
            try:
                # 将业务概要保存为JSON
                tables_summary = business_summary.get("tables_summary", [])
                
                # 处理业务领域信息
                business_domain = business_summary.get("business_domain", "")
                core_entities = business_summary.get("core_entities", [])
                business_processes = business_summary.get("business_processes", [])
                
                # 将业务概要保存为JSON
                metadata_json = json.dumps({
                    "tables_count": tables_count,
                    "tables": self.get_database_tables(db_name),
                    "summary": {
                        "business_domain": business_domain,
                        "core_entities": core_entities,
                        "business_processes": business_processes
                    },
                    "tables_summary": tables_summary
                }, ensure_ascii=False).replace("'", "''").replace('"', '\\"')
                
                # 保存业务概要
                business_summary_query = f"""
                INSERT INTO `{metadata_db}`.`business_metadata`
                (`database_name`, `metadata_key`, `metadata_value`, `update_time`)
                VALUES ('{db_name}', 'business_summary', "{metadata_json}", '{current_time}')
                """
                execute_query(business_summary_query)
                
                # 保存数据库统计信息
                db_stats = {
                    "tables_count": tables_count,
                    "updated_at": current_time
                }
                db_stats_json = json.dumps(db_stats, ensure_ascii=False).replace("'", "''").replace('"', '\\"')
                
                # 使用明确的列名插入
                db_stats_query = f"""
                INSERT INTO `{metadata_db}`.`business_metadata` 
                (`database_name`, `metadata_key`, `metadata_value`, `update_time`)
                VALUES ('{db_name}', 'db_stats', "{db_stats_json}", '{current_time}')
                """
                execute_query(db_stats_query)
                
                logger.info(f"成功保存业务元数据")
                return True
            
            except Exception as e:
                logger.error(f"保存业务元数据时出错: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"更新SQL模式和业务元数据时出错: {str(e)}")
            return False
    
    def save_all_databases_metadata(self, exclude_dbs: List[str] = None) -> bool:
        """
        导出所有数据库的元数据到统一的元数据库
        
        Args:
            exclude_dbs: 要排除的数据库列表，默认排除系统数据库
            
        Returns:
            bool: 操作是否成功
        """
        # 默认排除的系统数据库
        if exclude_dbs is None:
            exclude_dbs = ['information_schema', 'mysql', '__internal_schema', 'doris_metadata']
            
        # 获取所有数据库
        all_databases = self.get_all_databases()
        
        # 排除系统数据库和指定排除的数据库
        databases_to_process = [db for db in all_databases if db not in exclude_dbs]
        
        logger.info(f"开始处理 {len(databases_to_process)} 个数据库的元数据：{databases_to_process}")
        
        # 跟踪成功处理的数据库数量
        success_count = 0
        
        # 依次处理每个数据库
        for db_name in databases_to_process:
            logger.info(f"开始处理数据库 {db_name} 的元数据")
            
            try:
                # 为该数据库保存元数据
                if self.save_metadata_to_database(db_name):
                    success_count += 1
                    logger.info(f"数据库 {db_name} 的元数据保存成功")
                else:
                    logger.warning(f"数据库 {db_name} 的元数据保存失败")
            except Exception as e:
                logger.error(f"处理数据库 {db_name} 的元数据时出错: {str(e)}")
        
        # 生成整体统计信息
        try:
            metadata_db = "doris_metadata"
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 保存全局统计信息
            global_stats = {
                "total_databases": len(databases_to_process),
                "processed_databases": success_count,
                "database_list": databases_to_process,
                "updated_at": current_time
            }
            global_stats_json = json.dumps(global_stats, ensure_ascii=False).replace("'", "''")
            
            # 保存到特殊的全局记录
            global_stats_query = f"""
            INSERT INTO `{metadata_db}`.`business_metadata`
            (`database_name`, `metadata_key`, `metadata_value`, `update_time`)
            VALUES ('global', 'databases_stats', '{global_stats_json}', '{current_time}')
            """
            execute_query(global_stats_query)
            
            logger.info(f"全局元数据统计信息保存成功，总共 {len(databases_to_process)} 个数据库，成功处理 {success_count} 个")
        except Exception as e:
            logger.error(f"保存全局元数据统计信息时出错: {str(e)}")
        
        return success_count == len(databases_to_process)

    def refresh_all_databases_metadata(self, exclude_dbs: List[str] = None, force: bool = False) -> bool:
        """
        增量刷新所有数据库的元数据
        
        Args:
            exclude_dbs: 要排除的数据库列表，默认排除系统数据库
            force: 是否强制全量刷新
            
        Returns:
            bool: 操作是否成功
        """
        # 默认排除的系统数据库
        if exclude_dbs is None:
            exclude_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys', 'doris_metadata']
            
        # 获取所有数据库
        all_databases = self.get_all_databases()
        
        # 排除系统数据库和指定排除的数据库
        databases_to_process = [db for db in all_databases if db not in exclude_dbs]
        
        logger.info(f"开始增量刷新 {len(databases_to_process)} 个数据库的元数据：{databases_to_process}")
        
        # 跟踪成功处理的数据库数量
        success_count = 0
        
        # 依次处理每个数据库
        for db_name in databases_to_process:
            logger.info(f"开始增量刷新数据库 {db_name} 的元数据")
            
            try:
                # 为该数据库增量刷新元数据
                if self.refresh_metadata(db_name, force):
                    success_count += 1
                    logger.info(f"数据库 {db_name} 的元数据增量刷新成功")
                else:
                    logger.warning(f"数据库 {db_name} 的元数据增量刷新失败")
            except Exception as e:
                logger.error(f"增量刷新数据库 {db_name} 的元数据时出错: {str(e)}")
        
        # 更新整体统计信息
        try:
            metadata_db = "doris_metadata"
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 保存全局统计信息
            global_stats = {
                "total_databases": len(databases_to_process),
                "processed_databases": success_count,
                "database_list": databases_to_process,
                "updated_at": current_time
            }
            global_stats_json = json.dumps(global_stats, ensure_ascii=False).replace("'", "''")
            
            # 更新全局记录
            try:
                # 检查是否存在全局记录
                check_query = f"SELECT COUNT(*) as count FROM `{metadata_db}`.`business_metadata` WHERE database_name = 'global' AND metadata_key = 'databases_stats'"
                result = execute_query(check_query)
                exists = result[0]['count'] > 0 if result else False
                
                if exists:
                    # 更新现有记录
                    global_stats_query = f"""
                    UPDATE `{metadata_db}`.`business_metadata`
                    SET `metadata_value` = '{global_stats_json}', `update_time` = '{current_time}'
                    WHERE `database_name` = 'global' AND `metadata_key` = 'databases_stats'
                    """
                else:
                    # 插入新记录
                    global_stats_query = f"""
                    INSERT INTO `{metadata_db}`.`business_metadata`
                    (`database_name`, `metadata_key`, `metadata_value`, `update_time`)
                    VALUES ('global', 'databases_stats', '{global_stats_json}', '{current_time}')
                    """
                execute_query(global_stats_query)
            except Exception as e:
                logger.error(f"更新全局元数据统计记录时出错: {str(e)}")
                # 尝试使用插入语句
                try:
                    global_stats_query = f"""
                    INSERT INTO `{metadata_db}`.`business_metadata`
                    (`database_name`, `metadata_key`, `metadata_value`, `update_time`)
                    VALUES ('global', 'databases_stats', '{global_stats_json}', '{current_time}')
                    """
                    execute_query(global_stats_query)
                except Exception as e2:
                    logger.error(f"插入全局元数据统计记录时出错: {str(e2)}")
            
            logger.info(f"全局元数据统计信息更新成功，总共 {len(databases_to_process)} 个数据库，成功处理 {success_count} 个")
        except Exception as e:
            logger.error(f"更新全局元数据统计信息时出错: {str(e)}")
        
        return success_count == len(databases_to_process) 

    def _handle_multiline_json(self, content: str) -> Dict[str, Any]:
        """
        处理可能包含多行JSON的内容，尝试提取第一个有效的JSON对象
        保留原始响应完整性，采用提取而非删除的策略
        
        Args:
            content: 可能包含JSON的内容
            
        Returns:
            Dict[str, Any]: 解析后的JSON数据，如果解析失败则返回空字典
        """
        if not content or not content.strip():
            logging.warning("传入的内容为空或只有空白")
            return {}
        
        # 1. 优先从```json代码块中提取内容
        json_block_start = content.find('```json')
        json_block_end = content.find('```', json_block_start + 6) if json_block_start >= 0 else -1
        
        if json_block_start >= 0 and json_block_end > json_block_start:
            # 提取```json和```之间的内容
            json_content = content[json_block_start + 7:json_block_end].strip()
            logging.debug("从Markdown代码块中提取JSON内容")
            try:
                result = json.loads(json_content)
                logging.debug(f"成功解析Markdown代码块中的JSON: {result}")
                return result
            except json.JSONDecodeError as e:
                logging.debug(f"解析Markdown代码块中的JSON失败: {str(e)}")
        
        # 2. 检查是否有<think>标签，如果有，尝试提取标签外的JSON
        think_start = content.find('<think>')
        think_end = content.find('</think>', think_start) if think_start >= 0 else -1
        
        if think_start >= 0:
            # 如果找到了开始标签但没有结束标签
            if think_end == -1:
                logging.warning("找到<think>标签但没有找到对应的</think>标签")
                # 尝试从<think>后面提取JSON
                post_think_text = content[think_start + 7:].strip()
                result = self._extract_json_from_text(post_think_text)
                if result:
                    logging.debug(f"成功从未闭合<think>标签后提取JSON: {result}")
                    return result
            else:
                # 尝试在</think>标签后查找JSON
                post_think_text = content[think_end + 8:].strip()
                if post_think_text:
                    # 尝试解析</think>后的内容
                    result = self._extract_json_from_text(post_think_text)
                    if result:
                        logging.debug(f"成功从</think>标签后提取JSON: {result}")
                        return result
                    
                # 如果</think>后没有内容或解析失败，检查<think>标签内的内容
                think_text = content[think_start + 7:think_end].strip()
                if think_text:
                    # 检查标签内的内容是否有JSON
                    result = self._extract_json_from_text(think_text)
                    if result:
                        logging.debug(f"成功从<think>标签内部提取JSON: {result}")
                        return result
        
        # 3. 如果以上方法都失败，尝试从完整内容中提取JSON
        return self._extract_json_from_text(content)
    
    def _extract_json_from_text(self, text: str) -> Dict[str, Any]:
        """
        从文本中提取JSON对象
        
        Args:
            text: 可能包含JSON的文本
            
        Returns:
            Dict[str, Any]: 解析后的JSON数据
        """
        # 尝试解析第一行
        try:
            lines = text.strip().split('\n')
            first_line = lines[0].strip()
            if first_line and (first_line.startswith('{') and first_line.endswith('}')):
                result = json.loads(first_line)
                logging.debug(f"成功解析第一行JSON: {result}")
                return result
        except (json.JSONDecodeError, IndexError) as e:
            logging.debug(f"解析第一行JSON失败: {str(e)}")
        
        # 逐行尝试解析
        for line in text.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            
            if line.startswith('{') and line.endswith('}'):
                try:
                    result = json.loads(line)
                    logging.debug(f"在多行内容中找到有效JSON: {result}")
                    return result
                except json.JSONDecodeError:
                    pass
        
        # 尝试解析整个内容
        try:
            result = json.loads(text)
            logging.debug(f"成功解析整个内容为JSON: {result}")
            return result
        except json.JSONDecodeError as e:
            logging.debug(f"解析整个内容为JSON失败: {str(e)}")
        
        # 尝试提取完整的JSON对象
        try:
            # 找到第一个左大括号和最后一个右大括号
            start_idx = text.find('{')
            if start_idx >= 0:
                # 计算嵌套层级，确保找到匹配的右大括号
                level = 0
                end_idx = -1
                for i in range(start_idx, len(text)):
                    if text[i] == '{':
                        level += 1
                    elif text[i] == '}':
                        level -= 1
                        if level == 0:
                            end_idx = i
                            break
                
                if end_idx > start_idx:
                    json_str = text[start_idx:end_idx+1]
                    try:
                        result = json.loads(json_str)
                        logging.debug(f"提取并解析了完整JSON对象: {result}")
                        return result
                    except json.JSONDecodeError as e:
                        logging.debug(f"解析提取的JSON对象失败: {str(e)}")
        except Exception as e:
            logging.debug(f"提取JSON对象时出错: {str(e)}")
        
        logging.warning(f"无法从内容中提取有效的JSON: {text[:100]}...")
        return {}

    def get_business_metadata_from_database(self, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        从元数据库中获取业务元数据信息
        
        Args:
            db_name: 数据库名称，如果为None则使用初始化时指定的数据库
            
        Returns:
            Dict[str, Any]: 业务元数据信息，如果不存在则返回空字典
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("未指定数据库名称")
            return {}
        
        # 使用统一的元数据库名称
        metadata_db = "doris_metadata"
        
        try:
            # 查询业务元数据
            query = f"""
            SELECT metadata_value, update_time 
            FROM `{metadata_db}`.`business_metadata` 
            WHERE database_name = '{db_name}' AND metadata_key = 'business_summary'
            """
            
            result = execute_query(query)
            
            if not result or len(result) == 0:
                logger.info(f"数据库 {db_name} 的业务元数据不存在")
                return {}
            
            # 解析JSON数据
            metadata_value = result[0].get('metadata_value', '{}')
            update_time = result[0].get('update_time')
            
            try:
                # 先解析metadata_value为JSON对象
                metadata_json = json.loads(metadata_value)
                logger.info(f"成功从元数据库获取数据库 {db_name} 的业务元数据，更新时间: {update_time}")
                
                # 从metadata_json直接获取summary字段，它现在应该是一个嵌套的JSON对象
                summary_obj = metadata_json.get('summary', {})
                
                # 如果summary_obj是空字典，说明元数据库中没有有效的业务概览信息
                if not summary_obj or not isinstance(summary_obj, dict):
                    logger.info(f"元数据库中的业务概览信息为空或格式不正确，需要重新生成")
                    return {}
                
                # 检查summary_obj是否包含必要的字段
                # 例如business_domain，core_entities等
                if not ('business_domain' in summary_obj or 
                      'core_entities' in summary_obj or 
                      'business_processes' in summary_obj):
                    logger.info(f"元数据库中的业务概览信息格式不包含必要字段，需要重新生成")
                    return {}
                
                return summary_obj
                
            except json.JSONDecodeError as e:
                logger.error(f"解析业务元数据JSON时出错: {str(e)}")
                return {}
                return {}
                
        except Exception as e:
            logger.error(f"从元数据库获取业务元数据时出错: {str(e)}")
            return {}