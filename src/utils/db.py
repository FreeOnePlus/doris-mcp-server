import os
import json
import pymysql
import pandas as pd
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(override=True)

# 数据库配置
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "9030")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_DATABASE", ""),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# 多数据库配置
ENABLE_MULTI_DATABASE = os.getenv("ENABLE_MULTI_DATABASE", "false").lower() == "true"

# 获取多数据库名称列表
def get_multi_database_names() -> List[str]:
    """获取多数据库名称列表"""
    if not ENABLE_MULTI_DATABASE:
        return []
        
    multi_db_names_str = os.getenv("MULTI_DATABASE_NAMES", "[]")
    try:
        multi_db_names = json.loads(multi_db_names_str)
        if not isinstance(multi_db_names, list):
            return []
        # 确保默认数据库也在列表中
        default_db = DB_CONFIG.get("database")
        if default_db and default_db not in multi_db_names:
            multi_db_names.insert(0, default_db)
        return multi_db_names
    except json.JSONDecodeError:
        return []

# 多数据库名称列表
MULTI_DATABASE_NAMES = get_multi_database_names()

def get_db_connection(db_name: Optional[str] = None):
    """
    获取数据库连接
    
    Args:
        db_name: 指定要连接的数据库名称,如果为None则使用默认配置
    
    Returns:
        数据库连接
    """
    if db_name:
        # 使用默认配置但覆盖数据库名
        config = DB_CONFIG.copy()
        config["database"] = db_name
        return pymysql.connect(**config)
    else:
        # 使用默认配置
        return pymysql.connect(**DB_CONFIG)

def get_db_name() -> str:
    """获取当前配置的默认数据库名"""
    return DB_CONFIG["database"] or os.getenv("DB_DATABASE", "")

def execute_query(sql, db_name: Optional[str] = None):
    """
    执行SQL查询并返回结果
    
    Args:
        sql: SQL查询语句
        db_name: 指定要连接的数据库名称,如果为None则使用默认配置
    
    Returns:
        查询结果
    """
    conn = get_db_connection(db_name)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result
    finally:
        conn.close()

def execute_query_df(sql, db_name: Optional[str] = None):
    """
    执行SQL查询并返回pandas DataFrame
    
    Args:
        sql: SQL查询语句
        db_name: 指定要连接的数据库名称,如果为None则使用默认配置
    
    Returns:
        pandas DataFrame
    """
    conn = get_db_connection(db_name)
    try:
        # 使用一个临时游标执行查询并获取结果
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        
        # 如果没有结果,返回空DataFrame
        if not result:
            return pd.DataFrame()
            
        # 手动将字典结果转换为DataFrame
        df = pd.DataFrame(result)
        return df
    finally:
        conn.close()

def is_read_only_query(sql):
    """检查SQL是否为只读查询"""
    sql_lower = sql.lower().strip()
    write_operations = [
        "insert ", "update ", "delete ", "drop ", "alter ", "create ", "truncate ", "rename "
    ]
    
    # 检查是否包含写操作关键字
    for op in write_operations:
        if op in sql_lower:
            return False
    
    # 必须以 SELECT 或 SHOW 或 DESCRIBE 开头
    if not (sql_lower.startswith("select ") or 
            sql_lower.startswith("show ") or 
            sql_lower.startswith("desc ") or 
            sql_lower.startswith("describe ")):
        return False
    
    return True

def get_all_databases():
    """获取所有数据库"""
    result = execute_query("SHOW DATABASES")
    return [db["Database"] for db in result]

def get_tables_in_database(database):
    """获取指定数据库中的所有表"""
    result = execute_query(f"SHOW TABLES FROM `{database}`")
    key = f"Tables_in_{database}"
    return [table[key] for table in result]

def get_database_schema():
    """获取数据库架构信息"""
    databases = get_all_databases()
    schema = {}
    
    for db in databases:
        # 跳过系统数据库
        if db in ['information_schema', 'mysql', 'performance_schema', 'sys']:
            continue
            
        tables = get_tables_in_database(db)
        schema[db] = {}
        
        for table in tables:
            try:
                # 获取表结构
                columns = execute_query(f"DESCRIBE `{db}`.`{table}`")
                schema[db][table] = columns
            except Exception as e:
                schema[db][table] = str(e)
    
    return schema 