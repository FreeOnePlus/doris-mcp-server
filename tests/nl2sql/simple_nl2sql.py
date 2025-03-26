#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
简单的NL2SQL服务测试工具

用于快速测试自然语言到SQL的转换功能
"""

import os
import sys
import json
import argparse
import time
from dotenv import load_dotenv

# 设置项目根目录到系统路径中
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.insert(0, PROJECT_ROOT)

# 导入相关模块
from src.nl2sql_service import NL2SQLService
from src.utils.db import execute_query_df

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="简单的NL2SQL服务测试工具")
    parser.add_argument("--query", type=str, help="自然语言查询")
    parser.add_argument("--format", type=str, default="pretty", choices=["pretty", "json"], help="输出格式：pretty或json")
    return parser.parse_args()

def format_output(result, format_type="pretty"):
    """格式化输出结果"""
    if format_type == "json":
        # 转换为JSON格式
        return json.dumps(result, ensure_ascii=False, indent=2)
    else:
        # 美化输出
        output = []
        output.append("=" * 80)
        output.append(f"查询: {result.get('query', '')}")
        output.append("=" * 80)
        
        if result.get('success'):
            output.append(f"\n生成的SQL:\n{result.get('sql', '')}\n")
            
            if 'explanation' in result:
                output.append(f"解释:\n{result.get('explanation', '')}\n")
            
            if 'data' in result:
                output.append(f"查询结果: {len(result.get('data', []))} 行")
                
                # 获取列和数据
                columns = result.get('columns', [])
                data = result.get('data', [])
                
                if columns and data:
                    # 计算每列的最大宽度
                    max_widths = {col: len(col) for col in columns}
                    for row in data[:10]:  # 只考虑前10行
                        for col in columns:
                            val = str(row.get(col, ""))
                            max_widths[col] = max(max_widths[col], len(val))
                    
                    # 输出表头
                    header = " | ".join(col.ljust(max_widths[col]) for col in columns)
                    output.append("\n" + header)
                    output.append("-" * len(header))
                    
                    # 输出数据（最多10行）
                    for row in data[:10]:
                        row_str = " | ".join(str(row.get(col, "")).ljust(max_widths[col]) for col in columns)
                        output.append(row_str)
                    
                    if len(data) > 10:
                        output.append(f"... (还有 {len(data) - 10} 行)")
            
            output.append(f"\n执行时间: {result.get('execution_time', 0):.2f} 秒")
        else:
            output.append(f"\n错误: {result.get('message', '未知错误')}")
        
        return "\n".join(output)

def main():
    """主函数"""
    # 加载环境变量
    load_dotenv()
    
    # 解析命令行参数
    args = parse_args()
    
    # 如果没有提供查询，进入交互模式
    if not args.query:
        print("NL2SQL交互模式 (输入 'exit' 或 'quit' 退出)")
        print("=" * 80)
        
        # 初始化服务
        service = NL2SQLService()
        
        while True:
            try:
                # 获取用户输入
                query = input("\n请输入自然语言查询: ")
                
                # 检查是否退出
                if query.lower() in ['exit', 'quit']:
                    print("再见!")
                    break
                
                # 跳过空输入
                if not query.strip():
                    continue
                
                # 处理查询
                start_time = time.time()
                result = service.process_query(query)
                execution_time = time.time() - start_time
                
                # 添加总执行时间
                result['total_execution_time'] = execution_time
                
                # 输出结果
                print(format_output(result, args.format))
            except KeyboardInterrupt:
                print("\n操作已取消。输入 'exit' 或 'quit' 退出，或继续输入查询。")
            except Exception as e:
                print(f"发生错误: {str(e)}")
    else:
        # 单次查询模式
        try:
            # 初始化服务
            service = NL2SQLService()
            
            # 处理查询
            start_time = time.time()
            result = service.process_query(args.query)
            execution_time = time.time() - start_time
            
            # 添加总执行时间
            result['total_execution_time'] = execution_time
            
            # 输出结果
            print(format_output(result, args.format))
        except Exception as e:
            if args.format == "json":
                print(json.dumps({'success': False, 'error': str(e)}, ensure_ascii=False))
            else:
                print(f"发生错误: {str(e)}")

if __name__ == "__main__":
    main() 