#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Doris NL2SQL 命令行工具

这个简单的命令行工具允许您在没有 Node.js 的情况下测试 NL2SQL 功能。
"""

import os
import sys
import argparse
from dotenv import load_dotenv

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入数据库工具和查询分析函数
from doris_mcp.utils.db import execute_query, execute_query_df, get_all_databases, get_tables_in_database
from doris_mcp.tools.query_tools import analyze_query, explain_table

def main():
    """命令行工具主函数"""
    parser = argparse.ArgumentParser(description="Apache Doris NL2SQL 命令行工具")
    parser.add_argument("query", nargs="?", help="自然语言查询")
    parser.add_argument("--execute", "-e", action="store_true", help="执行生成的SQL查询")
    parser.add_argument("--list-tables", "-l", action="store_true", help="列出数据库中的表")
    parser.add_argument("--explain-table", "-t", help="解释指定的表结构")
    parser.add_argument("--database", "-d", help="指定数据库名")
    args = parser.parse_args()
    
    # 加载环境变量
    load_dotenv()
    
    # 如果指定了列出表
    if args.list_tables:
        list_database_tables(args.database)
        return
    
    # 如果指定了解释表
    if args.explain_table:
        explain_db_table(args.explain_table, args.database)
        return
    
    # 如果没有提供查询,进入交互模式
    if not args.query:
        show_menu()
        
        while True:
            choice = input("\n请选择操作 (1-4,或输入 'exit' 退出): ")
            
            if choice.lower() in ('exit', 'quit'):
                break
                
            if choice == '1':
                # 列出数据库表
                list_database_tables(None)
            elif choice == '2':
                # 解释表结构
                table_name = input("请输入表名: ")
                database = input("请输入数据库名 (可选): ")
                if not database.strip():
                    database = None
                explain_db_table(table_name, database)
            elif choice == '3':
                # NL2SQL转换
                query = input("请输入自然语言查询: ")
                process_query(query, execute_sql=args.execute)
            elif choice == '4':
                # 直接执行SQL
                sql = input("请输入SQL查询: ")
                if sql.strip():
                    execute_sql_query(sql)
            else:
                print("无效的选择,请重试")
    else:
        # 处理命令行传入的查询
        process_query(args.query, execute_sql=args.execute)

def show_menu():
    """显示主菜单"""
    print("=" * 80)
    print("Apache Doris NL2SQL 命令行工具")
    print("=" * 80)
    print("1. 列出数据库表")
    print("2. 解释表结构")
    print("3. NL2SQL转换")
    print("4. 直接执行SQL")
    print("输入 'exit' 或 'quit' 退出")
    print("-" * 80)

def list_database_tables(database=None):
    """列出数据库中的表"""
    try:
        if database:
            # 列出指定数据库中的表
            tables = get_tables_in_database(database)
            print(f"\n数据库 '{database}' 中的表:")
            for i, table in enumerate(tables, 1):
                print(f"{i}. {table}")
        else:
            # 列出所有数据库及其表
            databases = get_all_databases()
            print("\n可用的数据库和表:")
            for db in databases:
                # 跳过系统数据库
                if db in ['information_schema', 'mysql', 'performance_schema', 'sys']:
                    continue
                print(f"\n数据库: {db}")
                tables = get_tables_in_database(db)
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table}")
    except Exception as e:
        print(f"列出表时出错: {str(e)}")

def explain_db_table(table_name, database=None):
    """解释表结构"""
    try:
        result = explain_table(table_name, database)
        print("\n表结构解释:")
        print("-" * 80)
        print(result)
        print("-" * 80)
    except Exception as e:
        print(f"解释表时出错: {str(e)}")

def process_query(query, execute_sql=False):
    """处理自然语言查询"""
    print(f"\n分析查询: {query}")
    print("-" * 80)
    
    # 使用 analyze_query 工具分析查询
    result = analyze_query(query)
    print(result)
    
    # 提取SQL
    sql = extract_sql(result)
    if sql:
        print("\n提取的SQL查询:")
        print("-" * 80)
        print(sql)
        print("-" * 80)
        
        # 如果指定了执行SQL,则执行
        if execute_sql:
            execute_sql_query(sql)
        else:
            # 询问是否执行查询
            execute = input("\n是否执行这个SQL查询? (y/n): ")
            if execute.lower() == 'y':
                execute_sql_query(sql)

def extract_sql(result):
    """从分析结果中提取SQL"""
    if "```sql" in result:
        try:
            # 提取SQL代码块
            sql_start = result.find("```sql") + 6
            sql_end = result.find("```", sql_start)
            return result[sql_start:sql_end].strip()
        except:
            return None
    return None

def execute_sql_query(sql):
    """执行SQL查询并显示结果"""
    print("\n执行查询结果:")
    print("-" * 80)
    try:
        df = execute_query_df(sql)
        if len(df) > 20:
            print(f"查询结果包含 {len(df)} 行。为限制输出,仅显示前20行:\n")
            print(df.head(20).to_string(index=False))
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"执行查询时出错: {str(e)}")
        print("注意: SQL可能不匹配您的实际数据库结构。")

if __name__ == "__main__":
    main() 