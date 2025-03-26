#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试NL2SQL处理器的SQL生成功能
"""

import os
import logging
import sys
import json
from pathlib import Path

# 添加项目根目录到路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# 导入相关模块
from src.utils.nl2sql_processor import NL2SQLProcessor
from src.utils.llm_client import get_llm_client, Message

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """测试NL2SQL处理器的SQL生成功能"""
    
    logger.info("开始测试SQL生成功能")
    
    # 初始化NL2SQLProcessor
    processor = NL2SQLProcessor()
    
    # 测试用例
    test_cases = [
        {
            "name": "简单的销售查询",
            "query": "查询最近一个月的销售额"
        },
        {
            "name": "带有分组的查询",
            "query": "按产品类别统计今年的销售量"
        },
        {
            "name": "带有时间条件的查询",
            "query": "查询2023年第一季度的利润率"
        }
    ]
    
    # 为每个测试用例生成SQL
    results = []
    for i, test in enumerate(test_cases):
        logger.info(f"运行测试用例 {i+1}/{len(test_cases)}: {test['name']}")
        
        try:
            # 调用生成SQL的方法
            result = processor.process(test['query'])
            
            # 检查结果
            if result and 'sql' in result:
                logger.info(f"成功生成SQL: {result['sql'][:100]}...")
                logger.info(f"执行状态: {result.get('success', False)}")
                logger.info(f"执行消息: {result.get('message', 'N/A')}")
                success = True
            else:
                logger.error(f"未能生成SQL: {result}")
                success = False
            
            # 保存结果
            test_result = {
                "name": test['name'],
                "query": test['query'],
                "result": result,
                "success": success
            }
            results.append(test_result)
            
        except Exception as e:
            logger.error(f"测试用例执行错误: {str(e)}")
            results.append({
                "name": test['name'],
                "query": test['query'],
                "error": str(e),
                "success": False
            })
    
    # 汇总结果
    success_count = sum(1 for r in results if r['success'])
    logger.info(f"\n==== 测试总结 ====")
    logger.info(f"SQL生成: {success_count}/{len(test_cases)} 通过")
    logger.info(f"总计: {success_count}/{len(test_cases)} 通过 (成功率: {success_count/len(test_cases)*100:.2f}%)")
    
    # 保存详细结果到文件
    results_dir = Path(PROJECT_ROOT) / "test_results"
    results_dir.mkdir(exist_ok=True)
    
    results_file = results_dir / f"sql_generation_test_results.json"
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"详细结果已保存至: {results_file}")

if __name__ == "__main__":
    main() 