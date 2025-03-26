#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试日志保存修复

测试针对<think>标签和特殊字符的日志保存修复
"""

import sys
import os
import json
import logging
import uuid
from datetime import datetime
import pathlib
from dotenv import load_dotenv

# 获取项目根目录
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

# 添加项目根目录到路径
sys.path.insert(0, PROJECT_ROOT)

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 导入模块
from src.utils.nl2sql_processor import log_query_process

def test_log_query_process():
    """测试log_query_process函数对特殊字符的处理"""
    logger.info("开始测试日志保存功能 - 特殊字符处理")
    
    # 测试案例1: 包含<think>标签
    test1 = {
        "query": "测试查询",
        "function": "test_function",
        "llm_response": "<think>\n这是思考内容\n</think>\n实际返回内容"
    }
    
    # 测试案例2: 包含其他特殊字符
    test2 = {
        "query": "另一个测试",
        "function": "test_function",
        "llm_response": "包含双引号\"和反斜杠\\以及尖括号<>等特殊字符"
    }
    
    # 测试案例3: 包含JSON字符串
    test3 = {
        "query": "JSON测试",
        "function": "test_function",
        "llm_response": """```json
{
  "result": "这是测试结果",
  "code": 200,
  "think": "<think>hidden</think>"
}
```"""
    }
    
    # 测试案例4: 实际由LLM生成的包含<think>标签的响应
    test4 = {
        "query": "最近的营销情况怎么样",
        "function": "_check_business_query_with_llm",
        "llm_response": "<think>\n让我思考一下这个问题是否是一个业务查询问题。\n\n这个问题「最近的营销情况怎么样」确实是在询问业务数据，似乎是要了解一些营销相关的数据或统计信息，这可能是一个有效的业务查询。\n\n然而，问题有点模糊，没有具体指明要查询什么具体的营销指标（如ROI、转化率、活动效果等）。不过总体来说，这应该算是一个业务数据查询问题，因为用户明显是想了解营销相关的业务数据。\n\n我的判断是这是一个业务查询问题，但置信度不是非常高，因为问题比较宽泛。\n</think>\n\n```json\n{\n  \"is_business_query\": true,\n  \"confidence\": 0.8,\n  \"explanation\": \"这是一个关于营销情况的业务查询问题，用户明显是想获取相关的营销数据或统计信息。\"\n}\n```"
    }
    
    # 运行测试
    for i, test in enumerate([test1, test2, test3, test4], 1):
        logger.info(f"测试案例 {i}:")
        logger.info(f"原始内容: {test['llm_response'][:50]}...")
        
        # 调用函数
        log_path = log_query_process(test, f"log_fix_test{i}")
        
        if log_path:
            # 读取保存的日志并验证
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    saved_log = json.load(f)
                
                # 校验内容是否完整保存
                if "llm_response" in saved_log:
                    saved_response = saved_log["llm_response"]
                    logger.info(f"保存的内容: {saved_response[:50]}...")
                    
                    # 检查内容是否被截断
                    if "<think>" in test["llm_response"] and "<think>" not in saved_response and "\\<think\\>" not in saved_response:
                        logger.error("内容被截断: <think>标签丢失")
                    elif "</think>" in test["llm_response"] and "</think>" not in saved_response and "\\</think\\>" not in saved_response:
                        logger.error("内容被截断: </think>标签丢失")
                    else:
                        logger.info("内容完整保存 ✓")
                        
                    # 比较内容长度
                    original_length = len(test["llm_response"])
                    saved_length = len(saved_response.replace("\\<", "<").replace("\\>", ">"))
                    
                    if abs(original_length - saved_length) > 10:  # 允许一些误差
                        logger.warning(f"内容长度不一致: 原始={original_length}, 保存={saved_length}")
                    else:
                        logger.info(f"内容长度匹配 ✓ (原始={original_length}, 保存={saved_length})")
                else:
                    logger.error("保存的日志中没有llm_response字段")
            except json.JSONDecodeError as e:
                logger.error(f"读取保存的日志时出错: {str(e)}")
            except Exception as e:
                logger.error(f"验证保存的日志时出错: {str(e)}")
        else:
            logger.error("日志保存失败")
    
    logger.info("日志保存功能测试完成")

if __name__ == "__main__":
    test_log_query_process() 