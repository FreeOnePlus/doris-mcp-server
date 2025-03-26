#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试业务查询判断函数

测试_check_business_query_with_llm函数对包含<think>标签的响应处理
"""

import sys
import os
import json
import logging
from unittest.mock import patch, MagicMock
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
from src.utils.nl2sql_processor import NL2SQLProcessor

def setup_mock_llm_client(response_content):
    """设置模拟的LLM客户端，返回指定内容"""
    mock_response = MagicMock()
    mock_response.content = response_content
    
    mock_llm_client = MagicMock()
    mock_llm_client.chat.return_value = mock_response
    
    return mock_llm_client

def test_check_business_query_with_llm():
    """测试_check_business_query_with_llm函数对各种响应格式的处理"""
    logger.info("开始测试业务查询判断函数")
    
    # 创建NL2SQLProcessor实例
    processor = NL2SQLProcessor()
    
    # 测试案例
    test_cases = [
        {
            "description": "带<think>标签的完整JSON响应",
            "response": """<think>
让我思考一下这个问题是否是一个业务查询问题。

这个问题「最近的营销情况怎么样」确实是在询问业务数据，似乎是要了解一些营销相关的数据或统计信息，这可能是一个有效的业务查询。

然而，问题有点模糊，没有具体指明要查询什么具体的营销指标（如ROI、转化率、活动效果等）。不过总体来说，这应该算是一个业务数据查询问题，因为用户明显是想了解营销相关的业务数据。

我的判断是这是一个业务查询问题，但置信度不是非常高，因为问题比较宽泛。
</think>

```json
{
  "is_business_query": true,
  "confidence": 0.8,
  "explanation": "这是一个关于营销情况的业务查询问题，用户明显是想获取相关的营销数据或统计信息。"
}
```""",
            "expected_result": (True, 0.8)
        },
        {
            "description": "直接返回JSON (无<think>标签)",
            "response": """{
  "is_business_query": false,
  "confidence": 0.3,
  "explanation": "这不是业务查询问题，而是闲聊。"
}""",
            "expected_result": (False, 0.3)
        },
        {
            "description": "带<think>标签但JSON格式不规范",
            "response": """<think>
这个问题不是业务查询。
</think>

is_business_query: false
confidence: 0.2
explanation: "不是业务相关的问题"
""",
            "expected_result": (False, 0.2)
        },
        {
            "description": "<think>标签和JSON不在一起的情况",
            "response": """<think>
分析中...
</think>

其他内容...

```json
{
  "is_business_query": true, 
  "confidence": 0.9,
  "explanation": "这是业务查询"
}
```""",
            "expected_result": (True, 0.9)
        }
    ]
    
    # 运行测试
    for i, test_case in enumerate(test_cases, 1):
        description = test_case["description"]
        response_content = test_case["response"]
        expected_is_business, expected_confidence = test_case["expected_result"]
        
        logger.info(f"\n测试案例 {i}: {description}")
        logger.info(f"响应内容前50个字符: {response_content[:50]}...")
        
        # 模拟LLM客户端
        with patch('src.utils.nl2sql_processor.get_llm_client') as mock_get_llm_client:
            mock_get_llm_client.return_value = setup_mock_llm_client(response_content)
            
            # 调用函数
            is_business, confidence = processor._check_business_query_with_llm("测试查询")
            
            # 验证结果
            result_correct = (is_business == expected_is_business and abs(confidence - expected_confidence) < 0.01)
            if result_correct:
                logger.info(f"✓ 测试通过! 结果: is_business={is_business}, confidence={confidence}")
            else:
                logger.error(f"✗ 测试失败! 期望: {expected_is_business}, {expected_confidence}, 实际: {is_business}, {confidence}")

if __name__ == "__main__":
    test_check_business_query_with_llm() 