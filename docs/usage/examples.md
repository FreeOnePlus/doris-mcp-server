# Doris MCP 使用示例

本文档提供了一系列使用示例，展示如何通过MCP客户端访问Doris数据库资源。这些示例涵盖了常见的操作场景，包括获取数据库信息、表结构、执行SQL查询等。

## 准备工作

在开始之前，请确保：

1. Doris MCP服务器已启动并正常运行
2. 已安装必要的依赖包：`pip install requests sseclient-py`
3. 将MCP客户端代码复制到您的项目中

## 示例1：获取数据库基本信息

以下示例展示如何获取Doris数据库的基本信息：

```python
from mcp_client import MCPClient

def example_get_database_info():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 获取数据库信息
        print("\n获取数据库信息...")
        db_info = client.request("resource", {"uri": "doris://database/info"})
        
        # 打印结果
        print(f"数据库名称: {db_info['database']}")
        print(f"连接状态: {db_info['connection_status']}")
        print(f"服务器版本: {db_info['server_version']}")
        print(f"服务器时间: {db_info['server_time']}")
        print(f"表数量: {db_info['table_count']}")
        
        if db_info['last_refresh_time']:
            print(f"上次刷新时间: {db_info['last_refresh_time']}")
        else:
            print("上次刷新时间: 从未刷新")
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例
if __name__ == "__main__":
    example_get_database_info()
```

## 示例2：获取表列表

以下示例展示如何获取数据库中的所有表：

```python
from mcp_client import MCPClient
import json

def example_get_tables():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 获取表列表
        print("\n获取表列表...")
        result = client.request("resource", {"uri": "doris://database/tables"})
        
        # 打印结果
        print(f"数据库: {result['database']}")
        print(f"表数量: {result['count']}")
        
        print("\n表列表:")
        for i, table in enumerate(result['tables'], 1):
            size_mb = table['size'] / (1024 * 1024)
            print(f"{i}. {table['name']} ({table['type']}) - {table['rows']} 行, {size_mb:.2f} MB")
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例
if __name__ == "__main__":
    example_get_tables()
```

## 示例3：获取表结构

以下示例展示如何获取特定表的结构信息：

```python
from mcp_client import MCPClient
import html2text

def example_get_table_schema():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 先获取数据库信息
        db_info = client.request("resource", {"uri": "doris://database/info"})
        database_name = db_info['database']
        
        # 获取表结构
        table_name = "customer"  # 替换为您需要查询的表名
        print(f"\n获取表 {database_name}.{table_name} 的结构...")
        
        schema_html = client.request("resource", {"uri": f"schema://{database_name}/{table_name}"})
        
        # HTML转纯文本
        converter = html2text.HTML2Text()
        converter.ignore_links = False
        schema_text = converter.handle(schema_html)
        
        # 打印结果
        print("\n表结构:")
        print(schema_text)
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例 (需要安装html2text: pip install html2text)
if __name__ == "__main__":
    example_get_table_schema()
```

## 示例4：获取数据库元数据

以下示例展示如何获取数据库的详细元数据：

```python
from mcp_client import MCPClient
import json

def example_get_database_metadata():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 获取数据库元数据
        print("\n获取数据库元数据...")
        metadata = client.request("resource", {"uri": "doris://database/metadata"})
        
        # 打印基本信息
        print(f"数据库: {metadata['database']}")
        print(f"表数量: {len(metadata['tables'])}")
        
        if 'business_overview' in metadata:
            print(f"\n业务概览:\n{metadata['business_overview']}")
        
        # 打印表信息
        print("\n表列表:")
        for table in metadata['tables']:
            print(f"\n表名: {table['name']}")
            if 'comment' in table and table['comment']:
                print(f"说明: {table['comment']}")
            print(f"列数量: {len(table['columns'])}")
            
            # 只打印前3列
            print("部分列信息:")
            for i, column in enumerate(table['columns'][:3]):
                nullable = "可空" if column.get('nullable', True) else "非空"
                print(f"  - {column['name']} ({column['type']}, {nullable}): {column.get('comment', '')}")
            
            if len(table['columns']) > 3:
                print(f"  ... 还有 {len(table['columns']) - 3} 列未显示")
        
        # 打印关系
        if 'relationships' in metadata and metadata['relationships']:
            print("\n表关系:")
            for relation in metadata['relationships']:
                print(f"  {relation['table']}.{relation['column']} -> {relation['references_table']}.{relation['references_column']}")
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例
if __name__ == "__main__":
    example_get_database_metadata()
```

## 示例5：获取数据库统计信息

以下示例展示如何获取数据库的统计信息：

```python
from mcp_client import MCPClient
import json
from datetime import datetime

def example_get_database_stats():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 获取数据库统计信息
        print("\n获取数据库统计信息...")
        stats = client.request("resource", {"uri": "doris://database/stats"})
        
        # 打印基本统计信息
        print(f"数据库: {stats['database']}")
        print(f"总大小: {stats['total_size'] / (1024 * 1024 * 1024):.2f} GB")
        print(f"表数量: {stats['total_tables']}")
        print(f"总行数: {stats['total_rows']:,}")
        
        # 打印最近修改的表
        print("\n最近修改的表:")
        for table in stats['recent_tables']:
            print(f"  {table['name']} - 最后修改: {table['last_modified']}")
        
        # 打印最大的表
        print("\n最大的表:")
        for table in stats['largest_tables']:
            size_mb = table['size'] / (1024 * 1024)
            print(f"  {table['name']} - {table['rows']:,} 行, {size_mb:.2f} MB")
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例
if __name__ == "__main__":
    example_get_database_stats()
```

## 示例6：执行自然语言转SQL查询

以下示例展示如何使用NL2SQL查询功能：

```python
from mcp_client import MCPClient
import json

def example_nl2sql_query():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 自然语言查询
        query = "查询销售额最高的前10个产品及其销售数量"
        print(f"\n执行自然语言查询: {query}")
        
        # 发送请求
        result = client.request("nl2sql_query", {"query": query})
        
        # 检查结果
        if result['success']:
            # 打印生成的SQL
            print("\n生成的SQL:")
            print(result['sql'])
            
            # 打印查询结果
            print("\n查询结果:")
            headers = result['columns']
            print(" | ".join(headers))
            print("-" * (sum(len(h) + 3 for h in headers) - 1))
            
            for row in result['data'][:10]:  # 只显示前10行
                print(" | ".join(str(val) for val in row))
                
            if len(result['data']) > 10:
                print(f"... 还有 {len(result['data']) - 10} 行未显示")
                
            # 打印执行时间
            print(f"\n执行时间: {result['execution_time']:.3f} 秒")
        else:
            print(f"\n查询失败: {result['message']}")
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例
if __name__ == "__main__":
    example_nl2sql_query()
```

## 示例7：获取系统状态

以下示例展示如何获取系统状态信息：

```python
from mcp_client import MCPClient
import html2text

def example_get_system_status():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 获取系统状态
        print("\n获取系统状态...")
        status_html = client.request("resource", {"uri": "system://status"})
        
        # HTML转纯文本
        converter = html2text.HTML2Text()
        converter.ignore_links = False
        status_text = converter.handle(status_html)
        
        # 打印结果
        print("\n系统状态:")
        print(status_text)
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例 (需要安装html2text: pip install html2text)
if __name__ == "__main__":
    example_get_system_status()
```

## 示例8：获取文档

以下示例展示如何获取服务器文档：

```python
from mcp_client import MCPClient
import html2text

def example_get_documentation():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接服务器...")
        client.connect()
        print("连接成功！")
        
        # 获取文档
        doc_topic = "guide/getting-started"  # 可以是 guide/nl2sql, api/reference 等
        print(f"\n获取文档: {doc_topic}...")
        
        doc_html = client.request("resource", {"uri": f"docs://{doc_topic}"})
        
        # HTML转纯文本
        converter = html2text.HTML2Text()
        converter.ignore_links = False
        doc_text = converter.handle(doc_html)
        
        # 打印结果
        print("\n文档内容:")
        print(doc_text)
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

# 运行示例 (需要安装html2text: pip install html2text)
if __name__ == "__main__":
    example_get_documentation()
```

## 完整使用示例

以下是一个完整的脚本，结合了多个功能，展示了如何使用MCP客户端与Doris服务器进行交互：

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Doris MCP 客户端使用示例
"""

from mcp_client import MCPClient
import json
import html2text
import time

def format_bytes(size):
    """格式化字节大小为人类可读格式"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def display_html(html_content):
    """将HTML内容转换为纯文本显示"""
    converter = html2text.HTML2Text()
    converter.ignore_links = False
    return converter.handle(html_content)

def doris_mcp_demo():
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        print("正在连接到Doris MCP服务器...")
        client.connect()
        print("连接成功！\n")
        
        # 展示可用功能菜单
        while True:
            print("\n" + "="*50)
            print("Doris MCP 客户端演示")
            print("="*50)
            print("1. 获取数据库基本信息")
            print("2. 获取表列表")
            print("3. 获取数据库元数据")
            print("4. 获取数据库统计信息")
            print("5. 获取表结构")
            print("6. 执行自然语言查询")
            print("7. 查看系统状态")
            print("8. 查看服务器文档")
            print("0. 退出")
            
            choice = input("\n请选择功能 (0-8): ")
            
            if choice == "0":
                break
            
            # 获取数据库基本信息
            elif choice == "1":
                print("\n获取数据库基本信息...")
                db_info = client.request("resource", {"uri": "doris://database/info"})
                
                print(f"数据库名称: {db_info['database']}")
                print(f"连接状态: {db_info['connection_status']}")
                print(f"服务器版本: {db_info['server_version']}")
                print(f"服务器时间: {db_info['server_time']}")
                print(f"表数量: {db_info['table_count']}")
                
                if db_info.get('last_refresh_time'):
                    print(f"上次刷新时间: {db_info['last_refresh_time']}")
                else:
                    print("上次刷新时间: 从未刷新")
            
            # 获取表列表
            elif choice == "2":
                print("\n获取表列表...")
                result = client.request("resource", {"uri": "doris://database/tables"})
                
                print(f"数据库: {result['database']}")
                print(f"表数量: {result['count']}")
                
                print("\n表列表:")
                for i, table in enumerate(result['tables'], 1):
                    print(f"{i}. {table['name']} ({table['type']}) - {table['rows']:,} 行, {format_bytes(table['size'])}")
            
            # 获取数据库元数据
            elif choice == "3":
                print("\n获取数据库元数据...")
                metadata = client.request("resource", {"uri": "doris://database/metadata"})
                
                print(f"数据库: {metadata['database']}")
                print(f"表数量: {len(metadata['tables'])}")
                
                if 'business_overview' in metadata:
                    print(f"\n业务概览:\n{metadata['business_overview']}")
                
                print("\n表列表:")
                for i, table in enumerate(metadata['tables'], 1):
                    print(f"\n{i}. 表名: {table['name']}")
                    if 'comment' in table and table['comment']:
                        print(f"   说明: {table['comment']}")
                    print(f"   列数量: {len(table['columns'])}")
                    
                    # 显示关系
                    if 'relationships' in metadata and metadata['relationships']:
                        relations = [r for r in metadata['relationships'] if r['table'] == table['name']]
                        if relations:
                            print("   关系:")
                            for rel in relations:
                                print(f"     {rel['column']} -> {rel['references_table']}.{rel['references_column']}")
                
                # 是否查看表详情
                if metadata['tables']:
                    show_detail = input("\n是否查看表详情? (y/n): ").lower()
                    if show_detail == 'y':
                        table_idx = int(input(f"请输入表序号 (1-{len(metadata['tables'])}): "))
                        if 1 <= table_idx <= len(metadata['tables']):
                            table = metadata['tables'][table_idx-1]
                            print(f"\n表 {table['name']} 的详细信息:")
                            print(f"列数量: {len(table['columns'])}")
                            print("\n列信息:")
                            for col in table['columns']:
                                nullable = "可空" if col.get('nullable', True) else "非空"
                                print(f"  - {col['name']} ({col['type']}, {nullable}): {col.get('comment', '')}")
            
            # 获取数据库统计信息
            elif choice == "4":
                print("\n获取数据库统计信息...")
                stats = client.request("resource", {"uri": "doris://database/stats"})
                
                print(f"数据库: {stats['database']}")
                print(f"总大小: {format_bytes(stats['total_size'])}")
                print(f"表数量: {stats['total_tables']}")
                print(f"总行数: {stats['total_rows']:,}")
                
                print("\n最近修改的表:")
                for table in stats['recent_tables']:
                    print(f"  {table['name']} - 最后修改: {table['last_modified']}")
                
                print("\n最大的表:")
                for table in stats['largest_tables']:
                    print(f"  {table['name']} - {table['rows']:,} 行, {format_bytes(table['size'])}")
            
            # 获取表结构
            elif choice == "5":
                # 先获取表列表
                tables = client.request("resource", {"uri": "doris://database/tables"})
                db_name = tables['database']
                
                print("\n可用的表:")
                for i, table in enumerate(tables['tables'], 1):
                    print(f"{i}. {table['name']}")
                
                table_idx = int(input(f"\n请选择表 (1-{len(tables['tables'])}): "))
                if 1 <= table_idx <= len(tables['tables']):
                    table_name = tables['tables'][table_idx-1]['name']
                    
                    print(f"\n获取表 {db_name}.{table_name} 的结构...")
                    schema_html = client.request("resource", {"uri": f"schema://{db_name}/{table_name}"})
                    
                    print("\n表结构:")
                    print(display_html(schema_html))
            
            # 执行自然语言查询
            elif choice == "6":
                query = input("\n请输入自然语言查询: ")
                print(f"\n执行查询: {query}")
                
                result = client.request("nl2sql_query", {"query": query})
                
                if result['success']:
                    print("\n生成的SQL:")
                    print(result['sql'])
                    
                    print("\n查询结果:")
                    if result['data']:
                        headers = result['columns']
                        print(" | ".join(headers))
                        print("-" * (sum(len(h) + 3 for h in headers) - 1))
                        
                        # 显示前10行
                        for row in result['data'][:10]:
                            print(" | ".join(str(val) for val in row))
                            
                        if len(result['data']) > 10:
                            print(f"... 还有 {len(result['data']) - 10} 行未显示")
                    else:
                        print("查询结果为空")
                        
                    print(f"\n执行时间: {result['execution_time']:.3f} 秒")
                else:
                    print(f"\n查询失败: {result['message']}")
            
            # 获取系统状态
            elif choice == "7":
                print("\n获取系统状态...")
                status_html = client.request("resource", {"uri": "system://status"})
                
                print("\n系统状态:")
                print(display_html(status_html))
            
            # 查看服务器文档
            elif choice == "8":
                print("\n可用文档:")
                print("1. 入门指南 (guide/getting-started)")
                print("2. NL2SQL使用指南 (guide/nl2sql)")
                print("3. API参考 (api/reference)")
                print("4. MCP协议指南 (guide/mcp-protocol)")
                
                doc_choice = input("\n请选择文档 (1-4): ")
                
                topics = {
                    "1": "guide/getting-started",
                    "2": "guide/nl2sql",
                    "3": "api/reference",
                    "4": "guide/mcp-protocol"
                }
                
                if doc_choice in topics:
                    topic = topics[doc_choice]
                    print(f"\n获取文档: {topic}...")
                    
                    doc_html = client.request("resource", {"uri": f"docs://{topic}"})
                    
                    print("\n文档内容:")
                    print(display_html(doc_html))
                else:
                    print("无效选择")
            
            else:
                print("无效选择，请重试")
            
            input("\n按回车键继续...")
        
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect()
        print("\n已断开连接")

if __name__ == "__main__":
    doris_mcp_demo()
```

## 注意事项

使用MCP客户端时，请注意以下几点：

1. **连接顺序** - 必须先连接(connect)再发送请求，断开连接后不能继续发送请求
2. **异常处理** - 使用try/except/finally结构确保正确处理异常和断开连接
3. **资源释放** - 使用完成后务必调用disconnect()方法断开连接并释放资源
4. **HTML响应** - 部分资源返回HTML格式，可以使用html2text库转换为纯文本显示

## 依赖包

使用这些示例需要安装以下Python包：

```bash
pip install requests sseclient-py html2text
```

## 后续开发

在实际应用中，您可能需要扩展MCP客户端功能，例如：

1. 添加重连机制
2. 实现缓存策略
3. 添加异步请求支持
4. 开发特定业务功能的封装

这些扩展可以根据您的具体需求进行定制开发。 