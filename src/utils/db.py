import os
import json
import pymysql
import pandas as pd
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import re

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
        
    multi_db_names_str = os.getenv("MULTI_DATABASE_NAMES", "")
    
    # 首先尝试作为JSON解析
    try:
        multi_db_names = json.loads(multi_db_names_str)
        if isinstance(multi_db_names, list):
            # 确保默认数据库也在列表中
            default_db = DB_CONFIG.get("database")
            if default_db and default_db not in multi_db_names:
                multi_db_names.insert(0, default_db)
            return multi_db_names
    except json.JSONDecodeError:
        # 如果JSON解析失败，尝试作为逗号分隔的字符串处理
        pass
    
    # 按逗号分隔解析
    if multi_db_names_str:
        multi_db_names = [db.strip() for db in multi_db_names_str.split(',') if db.strip()]
        # 确保默认数据库也在列表中
        default_db = DB_CONFIG.get("database")
        if default_db and default_db not in multi_db_names:
            multi_db_names.insert(0, default_db)
        return multi_db_names
    
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
            # 在执行查询前先设置连接字符集为utf8
            cursor.execute("SET NAMES utf8")
            
            # 执行实际的查询
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
            # 在执行查询前先设置连接字符集为utf8
            cursor.execute("SET NAMES utf8")
            
            # 执行实际的查询
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
    if not sql:
        return False
    
    # 移除SQL注释
    # 移除单行注释 (-- 开始到行尾)
    sql_no_comments = re.sub(r'--.*?(\n|$)', ' ', sql)
    # 移除多行注释 (/* ... */)
    sql_no_comments = re.sub(r'/\*.*?\*/', ' ', sql_no_comments, flags=re.DOTALL)
    # 移除行内注释 (包括空格内的注释格式 /* xxx */)
    sql_no_comments = re.sub(r'/\*.*?\*/', ' ', sql_no_comments)
    
    # 标准化空白符
    sql_lower = sql_no_comments.lower().strip()
    
    # 检查是否包含写操作关键字
    write_operations = [
        r'\binsert\b', r'\bupdate\b', r'\bdelete\b', r'\bdrop\b', 
        r'\balter\b', r'\bcreate\b', r'\btruncate\b', r'\brename\b'
    ]
    
    # 使用正则表达式确保精确匹配写操作关键字
    for op in write_operations:
        if re.search(op, sql_lower):
            return False
    
    # 必须以 SELECT 或 SHOW 或 DESCRIBE 或 EXPLAIN 开头
    read_patterns = [
        r'^\s*select\b', 
        r'^\s*show\b', 
        r'^\s*desc\b', 
        r'^\s*describe\b', 
        r'^\s*explain\b'
    ]
    
    for pattern in read_patterns:
        if re.search(pattern, sql_lower):
            return True
    
    return False

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

# --- Added function to get detailed Doris version ---
def get_doris_version_comment(db_name: Optional[str] = None) -> str:
    """获取 Doris 数据库的详细版本注释信息"""
    try:
        # 执行查询获取 version_comment 变量
        result = execute_query("SHOW VARIABLES LIKE '%version_comment%';", db_name=db_name)
        
        # 检查结果是否有效
        if result and isinstance(result, list) and len(result) > 0:
            # 通常结果是一个包含字典的列表，字典包含 'Variable_name' 和 'Value'
            version_comment_info = result[0]
            if isinstance(version_comment_info, dict) and 'Value' in version_comment_info:
                return version_comment_info['Value']
            else:
                 # 如果结果格式不符合预期，记录警告并返回未知
                 # (此处简单处理，实际可能需要更详细的日志)
                 print(f"Warning: Unexpected format for version_comment: {version_comment_info}")
                 return "未知 (格式错误)"
        else:
            # 如果没有返回结果，也返回未知
            print(f"Warning: No result returned for version_comment query in db '{db_name or get_db_name()}'")
            return "未知 (无结果)"
            
    except Exception as e:
        # 发生异常时记录错误并返回未知
        print(f"Error fetching Doris version comment: {e}") # Consider using logger
        return "未知 (查询错误)"
# --- End of added function ---

def test_connection(db_name: Optional[str] = None) -> Dict[str, Any]:
    """
    测试数据库连接
    
    Args:
        db_name: 指定要连接的数据库名称,如果为None则使用默认配置
    
    Returns:
        Dict[str, Any]: 连接测试结果
    """
    import time
    
    start_time = time.time()
    try:
        # 获取数据库连接
        conn = get_db_connection(db_name)
        
        # 执行简单的测试查询
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 AS result")
            result = cursor.fetchone()
        
        conn.close()
        
        # 计算响应时间
        response_time = time.time() - start_time
        
        return {
            "status": "success",
            "response_time_ms": round(response_time * 1000, 2),
            "database": db_name or get_db_name(),
            "host": DB_CONFIG["host"],
            "port": DB_CONFIG["port"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        # 计算响应时间
        response_time = time.time() - start_time
        
        return {
            "status": "error",
            "error": str(e),
            "response_time_ms": round(response_time * 1000, 2),
            "database": db_name or get_db_name(),
            "host": DB_CONFIG["host"],
            "port": DB_CONFIG["port"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        } 