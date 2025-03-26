import os
import pymysql
import pandas as pd
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

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

def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG)

def get_db_name():
    """获取当前配置的数据库名"""
    return DB_CONFIG["database"] or os.getenv("DB_NAME", "")

def execute_query(sql):
    """执行SQL查询并返回结果"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result
    finally:
        conn.close()

def execute_query_df(sql):
    """执行SQL查询并返回pandas DataFrame"""
    conn = get_db_connection()
    try:
        # 使用一个临时游标执行查询并获取结果
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        
        # 如果没有结果，返回空DataFrame
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