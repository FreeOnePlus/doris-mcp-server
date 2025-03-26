#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Doris NL2SQL 示例

这个示例展示了如何使用Apache Doris NL2SQL服务器
将自然语言转换为SQL查询。
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入数据库工具
from src.doris_mcp.utils.db import execute_query, execute_query_df
from src.doris_mcp.utils.sql_samples import sql_sample_manager
from src.doris_mcp.tools.query_tools import analyze_query

# 示例自然语言查询
EXAMPLE_QUERIES = [
    "查询销售额前10的产品及其销售数量",
    "计算每个区域今年与去年同期的销售额对比",
    "分析过去30天内每个用户的活跃度",
    "统计各部门员工数量和平均薪资"
]

def call_nl2sql_api(query, api_host="localhost", api_port=3000, timeout=60, retries=3):
    """
    通过API调用NL2SQL服务
    
    Args:
        query: 自然语言查询
        api_host: API服务器主机名
        api_port: API服务器端口
        timeout: 请求超时时间（秒）
        retries: 重试次数
        
    Returns:
        dict: API响应结果
    """
    try:
        api_url = f"http://{api_host}:{api_port}/api/nl2sql"
        
        # 配置重试会话
        session = requests.Session()
        retry = requests.adapters.Retry(
            total=retries,
            backoff_factor=1,  # 重试间隔为 {backoff_factor} * (2 ** {retry number})
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 使用会话发送请求
        print(f"发送请求到 {api_url} (超时: {timeout}秒, 最大重试: {retries}次)")
        response = session.get(api_url, params={"query": query}, timeout=timeout)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "success": False,
                "message": f"API调用失败，状态码: {response.status_code}",
                "status_code": response.status_code
            }
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "message": f"API请求超时，已等待{timeout}秒"
        }
    except requests.exceptions.ConnectionError as e:
        return {
            "success": False,
            "message": f"API连接错误: {str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"API调用出错: {str(e)}"
        }

def nl2sql_demo():
    """
    NL2SQL转换示例演示
    """
    print("=" * 80)
    print("Apache Doris NL2SQL 示例")
    print("=" * 80)
    
    # 加载环境变量
    load_dotenv()
    print("已加载环境变量\n")
    
    # 连接信息
    db_host = os.getenv("DB_HOST", "www.freeoneplus.com")
    db_port = os.getenv("DB_PORT", "9030")
    db_user = os.getenv("DB_USER", "root")
    db_pass = os.getenv("DB_PASSWORD", "Doris2024")
    db_name = os.getenv("DB_DATABASE", "ssb")
    
    # API服务器信息
    api_host = os.getenv("API_HOST", "localhost")
    api_port = os.getenv("API_PORT", "3000")
    api_timeout = int(os.getenv("API_TIMEOUT", "300"))  # 默认5分钟超时
    api_retries = int(os.getenv("API_RETRIES", "3"))    # 默认重试3次
    
    print(f"数据库连接信息: {db_host}:{db_port}, 用户: {db_user}, 数据库: {db_name}")
    print(f"API服务器: http://{api_host}:{api_port} (超时: {api_timeout}秒, 重试: {api_retries}次)\n")
    
    # 选择操作
    print("选择操作:")
    print("1. 执行NL2SQL查询")
    print("2. 检查服务状态")
    print("3. 退出")
    
    choice = input("\n请选择操作 (1-3): ").strip()
    
    if choice == "2":
        verify_service_status(api_host, api_port, api_timeout, api_retries)
        return
    elif choice == "3":
        print("谢谢使用！")
        return
    elif choice != "1":
        print("无效选择，默认执行NL2SQL查询")
    
    # 示例查询
    print("\n示例自然语言查询:")
    for i, query in enumerate(EXAMPLE_QUERIES, 1):
        print(f"{i}. {query}")
    
    # 选择查询
    selected = input("\n请选择一个查询示例 (1-4) 或输入自定义查询: ")
    
    try:
        idx = int(selected) - 1
        if 0 <= idx < len(EXAMPLE_QUERIES):
            nl_query = EXAMPLE_QUERIES[idx]
        else:
            nl_query = selected
    except ValueError:
        nl_query = selected
    
    print(f"\n正在分析查询: {nl_query}")
    
    # 选择处理方式
    print("\n选择处理方式:")
    print("1. 本地处理（使用本地NL2SQL工具）")
    print("2. 调用服务器API（验证服务端运行）")
    
    mode = input("请选择处理方式 (1-2): ").strip()
    
    if mode == "2":
        # 通过API调用服务器
        print("-" * 80)
        print("通过API调用NL2SQL服务...")
        api_result = call_nl2sql_api(nl_query, api_host, api_port, api_timeout, api_retries)
        
        if api_result.get("success"):
            print("\nAPI调用成功!")
            print("-" * 80)
            
            # 提取SQL和解释
            sql = api_result.get("sql", "")
            explanation = api_result.get("explanation", "")
            
            print("生成的SQL查询:")
            print("-" * 80)
            print(sql)
            print("-" * 80)
            
            if explanation:
                print("\nSQL解释:")
                print("-" * 80)
                print(explanation)
                print("-" * 80)
            
            # 询问是否执行查询
            execute = input("\n是否执行这个SQL查询? (y/n): ")
            if execute.lower() == 'y':
                print("\n执行查询结果:")
                print("-" * 80)
                try:
                    df = execute_query_df(sql)
                    if len(df) > 20:
                        print(f"查询结果包含 {len(df)} 行。为限制输出，仅显示前20行:\n")
                        print(df.head(20).to_string(index=False))
                    else:
                        print(df.to_string(index=False))
                except Exception as e:
                    print(f"执行查询时出错: {str(e)}")
        else:
            print("\nAPI调用失败!")
            print("-" * 80)
            print(f"错误信息: {api_result.get('message', '未知错误')}")
            
            # 提供问题诊断和建议
            print("\n问题诊断:")
            if "Connection refused" in api_result.get('message', ''):
                print("1. 服务未启动或未监听指定端口")
                print("   建议: 运行 'python src/main.py' 启动服务")
            elif "Timeout" in api_result.get('message', ''):
                print("1. 服务响应超时")
                print("   建议: 增加超时时间或检查服务器负载")
            else:
                print("1. 请检查服务是否正常运行")
                print("2. 服务器地址和端口是否正确")
            
            # 如果API调用失败，提示用户是否尝试本地处理
            fallback = input("\nAPI调用失败。是否尝试本地处理? (y/n): ")
            if fallback.lower() != 'y':
                print("\n示例结束，谢谢使用！")
                return
            
            # 回退到本地处理
            print("\n使用本地处理...\n")
            mode = "1"
    
    if mode == "1":
        # 本地处理
        print("-" * 80)
        result = analyze_query(nl_query)
        print(result)
        
        # 提取SQL (简化示例，实际应用中可能需要更复杂的提取方法)
        sql = None
        if "```sql" in result:
            try:
                # 提取SQL代码块
                sql_start = result.find("```sql") + 6
                sql_end = result.find("```", sql_start)
                sql = result[sql_start:sql_end].strip()
                
                print("\n提取的SQL查询:")
                print("-" * 80)
                print(sql)
                print("-" * 80)
                
                # 询问是否执行查询
                execute = input("\n是否执行这个SQL查询? (y/n): ")
                if execute.lower() == 'y':
                    # 注意：实际应用中应该有更严格的安全检查
                    print("\n执行查询结果:")
                    print("-" * 80)
                    try:
                        df = execute_query_df(sql)
                        if len(df) > 20:
                            print(f"查询结果包含 {len(df)} 行。为限制输出，仅显示前20行:\n")
                            print(df.head(20).to_string(index=False))
                        else:
                            print(df.to_string(index=False))
                    except Exception as e:
                        print(f"执行查询时出错: {str(e)}")
                        # 可能是样例SQL，不匹配实际表结构
                        print("注意: 示例SQL可能不匹配您的实际数据库结构。")
            except Exception as e:
                print(f"提取或执行SQL时出错: {str(e)}")
    
    print("\n示例结束，谢谢使用！")

def verify_service_status(api_host="localhost", api_port="3000", timeout=60, retries=3):
    """
    验证NL2SQL服务的状态
    
    Args:
        api_host: API服务器主机名
        api_port: API服务器端口
        timeout: 请求超时时间（秒）
        retries: 重试次数
    """
    print("=" * 80)
    print("Apache Doris NL2SQL 服务状态验证")
    print("=" * 80)
    
    # 加载环境变量
    load_dotenv()
    
    print(f"正在检查API服务器: http://{api_host}:{api_port}...")
    print(f"超时设置: {timeout}秒, 最大重试次数: {retries}次")
    
    try:
        # 配置重试会话
        session = requests.Session()
        retry = requests.adapters.Retry(
            total=retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 尝试调用API健康检查端点
        health_url = f"http://{api_host}:{api_port}/api/health"
        print(f"\n[1/3] 检查健康端点: {health_url}")
        response = session.get(health_url, timeout=timeout)
        
        if response.status_code == 200:
            print("✅ 服务状态: 正常运行")
            print(f"响应: {response.json()}")
            
            # 测试简单查询
            test_query = "查询客户总数"
            api_url = f"http://{api_host}:{api_port}/api/nl2sql"
            print(f"\n[2/3] 发送测试查询到: {api_url}")
            print(f"查询内容: '{test_query}'")
            
            api_response = session.get(api_url, params={"query": test_query}, timeout=timeout)
            
            if api_response.status_code == 200:
                api_result = api_response.json()
                if api_result.get("success"):
                    print("✅ 查询处理: 成功")
                    print(f"生成的SQL: {api_result.get('sql', '无')}")
                else:
                    print("❌ 查询处理: 失败")
                    print(f"错误信息: {api_result.get('message', '未知错误')}")
            else:
                print(f"❌ 查询处理: 失败 (状态码: {api_response.status_code})")
                
            # 测试API文档端点
            docs_url = f"http://{api_host}:{api_port}/docs"
            print(f"\n[3/3] 检查API文档: {docs_url}")
            docs_response = session.get(docs_url, timeout=timeout)
            
            if docs_response.status_code == 200:
                print("✅ API文档: 可访问")
            else:
                print(f"❌ API文档: 不可访问 (状态码: {docs_response.status_code})")
        else:
            print(f"❌ 服务状态: 异常 (状态码: {response.status_code})")
    except requests.exceptions.Timeout:
        print(f"❌ 服务状态: 请求超时 (已等待{timeout}秒)")
        print("\n可能的原因:")
        print("1. 服务响应时间过长")
        print("2. 服务器负载过高")
        print("\n建议:")
        print("1. 增加超时时间")
        print("2. 检查服务器资源使用情况")
    except requests.exceptions.ConnectionError as e:
        print(f"❌ 服务状态: 无法连接")
        print(f"错误信息: {str(e)}")
        print("\n可能的原因:")
        print("1. 服务未启动")
        print("2. 服务器地址或端口配置错误")
        print("3. 防火墙阻止了连接")
        print("\n建议:")
        print("1. 使用命令 'python src/main.py' 启动服务")
        print("2. 确认环境变量中API_HOST和API_PORT配置正确")
        print("3. 检查防火墙设置")
    except Exception as e:
        print(f"❌ 服务状态检查出错: {str(e)}")
    
    print("\n验证完成。")

if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == '--check':
            # 加载环境变量
            load_dotenv()
            api_host = os.getenv("API_HOST", "localhost")
            api_port = os.getenv("API_PORT", "3000")
            api_timeout = int(os.getenv("API_TIMEOUT", "300"))
            api_retries = int(os.getenv("API_RETRIES", "3"))
            verify_service_status(api_host, api_port, api_timeout, api_retries)
        elif sys.argv[1] == '--local':
            # 本地处理模式
            load_dotenv()
            query = "查询销售额前10的产品及其销售数量"
            if len(sys.argv) > 2:
                query = sys.argv[2]
            print(f"使用本地处理模式执行查询: {query}")
            print("-" * 80)
            result = analyze_query(query)
            print(result)
            
            # 提取SQL
            if "```sql" in result:
                try:
                    sql_start = result.find("```sql") + 6
                    sql_end = result.find("```", sql_start)
                    sql = result[sql_start:sql_end].strip()
                    
                    print("\n提取的SQL查询:")
                    print("-" * 80)
                    print(sql)
                    print("-" * 80)
                    
                    # 自动执行查询
                    print("\n执行查询结果:")
                    print("-" * 80)
                    try:
                        df = execute_query_df(sql)
                        if len(df) > 20:
                            print(f"查询结果包含 {len(df)} 行。为限制输出，仅显示前20行:\n")
                            print(df.head(20).to_string(index=False))
                        else:
                            print(df.to_string(index=False))
                    except Exception as e:
                        print(f"执行查询时出错: {str(e)}")
                except Exception as e:
                    print(f"处理SQL时出错: {str(e)}")
        elif sys.argv[1] == '--help':
            print("用法:")
            print("  python examples/nl2sql_example.py          - 运行交互式演示")
            print("  python examples/nl2sql_example.py --check  - 检查服务状态")
            print("  python examples/nl2sql_example.py --local [query] - 本地处理指定查询")
            print("  python examples/nl2sql_example.py --help   - 显示帮助信息")
        else:
            print(f"未知参数: {sys.argv[1]}")
            print("使用 --help 查看可用选项")
    else:
        nl2sql_demo()