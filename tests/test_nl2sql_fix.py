#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
import re
from pathlib import Path
from dotenv import load_dotenv
from unittest.mock import patch

# 将项目根目录添加到Python路径
project_root = Path(__file__).parent.absolute()
sys.path.append(str(project_root))

# 创建日志目录
LOG_DIR = os.path.join(project_root, "log")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)

# 导入被测试的模块
from src.utils.nl2sql_processor import NL2SQLProcessor
from src.utils.metadata_extractor import MetadataExtractor
from src.utils.llm_client import get_llm_client, Message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'test_nl2sql_fix.log')),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# 模拟log_query_process函数
def mock_log_query_process(log_data, log_type="test"):
    """模拟日志记录功能，仅在测试时使用"""
    logger.info(f"记录日志 [{log_type}]: {json.dumps(log_data, ensure_ascii=False)[:100]}...")
    return os.path.join(LOG_DIR, "test_log.json")

def test_parse_llm_json_response():
    """测试LLM JSON响应解析，特别是对转义字符和<think>标签的处理"""
    logger.info("开始测试NL2SQL处理器的JSON解析功能")
    
    # 创建NL2SQLProcessor实例
    processor = NL2SQLProcessor(db_name="test_db")
    
    # 测试用例列表
    test_cases = [
        {
            "name": "测试 - 只包含<think>标签",
            "content": "<think>",
            "expected_has_error": True
        },
        {
            "name": "测试 - 未闭合的<think>标签",
            "content": "<think>这是一些内容",
            "expected_has_error": True
        },
        {
            "name": "测试 - 带有转义的<think>标签",
            "content": "\\<think\\>内容\\</think\\> {\"is_business_query\": true, \"confidence\": 0.8}",
            "expected_result": {"is_business_query": True, "confidence": 0.8}
        },
        {
            "name": "测试 - 正常JSON内容",
            "content": "{\"is_business_query\": true, \"confidence\": 0.9, \"reasoning\": \"这是销售相关的查询\"}",
            "expected_result": {"is_business_query": True, "confidence": 0.9, "reasoning": "这是销售相关的查询"}
        },
        {
            "name": "测试 - 正常<think>标签后跟JSON",
            "content": "<think>分析过程</think>\n{\"is_business_query\": false, \"confidence\": 0.7, \"reasoning\": \"不是商业查询\"}",
            "expected_result": {"is_business_query": False, "confidence": 0.7, "reasoning": "不是商业查询"}
        },
        {
            "name": "测试 - JSON在代码块中",
            "content": "```json\n{\"is_business_query\": true, \"confidence\": 0.95, \"reasoning\": \"明确的销售查询\"}\n```",
            "expected_result": {"is_business_query": True, "confidence": 0.95, "reasoning": "明确的销售查询"}
        },
        {
            "name": "测试 - 复杂文本中的JSON",
            "content": "用户查询是关于销售数据的。\n\n<think>\n我需要判断这是否是商业查询。\n这明显是关于销售的，所以是商业查询。\n</think>\n\n根据分析，我判断如下：\n\n```json\n{\"is_business_query\": true, \"confidence\": 0.85, \"reasoning\": \"查询明确提到了销售数据\"}\n```",
            "expected_result": {"is_business_query": True, "confidence": 0.85, "reasoning": "查询明确提到了销售数据"}
        },
        {
            "name": "测试 - JSON包含双重转义",
            "content": "{\\\"is_business_query\\\": true, \\\"confidence\\\": 0.75, \\\"reasoning\\\": \\\"这是关于库存的查询\\\"}",
            "expected_result": {"is_business_query": True, "confidence": 0.75, "reasoning": "这是关于库存的查询"}
        }
    ]
    
    # 测试结果统计
    success_count = 0
    total_tests = len(test_cases)
    
    # 运行测试用例
    for i, test_case in enumerate(test_cases):
        logger.info(f"运行测试用例 {i+1}/{total_tests}: {test_case['name']}")
        
        result = processor._parse_llm_json_response(test_case["content"])
        
        # 检查结果是否符合预期
        if "expected_has_error" in test_case and test_case["expected_has_error"]:
            # 期望有错误
            if "error" in result:
                logger.info(f"测试通过: 成功捕获到预期的错误")
                success_count += 1
            else:
                logger.error(f"测试失败: 预期出现错误，但返回了正常结果: {result}")
        elif "expected_result" in test_case:
            # 期望正确结果
            expected = test_case["expected_result"]
            passed = True
            
            for key, expected_value in expected.items():
                if key not in result or result[key] != expected_value:
                    logger.error(f"测试失败: 键 '{key}' 预期值 '{expected_value}'，实际值 '{result.get(key, 'missing')}'")
                    passed = False
                    break
            
            if passed:
                logger.info(f"测试通过: 解析结果符合预期")
                success_count += 1
            else:
                logger.error(f"完整的返回结果: {result}")
        else:
            logger.warning(f"测试用例定义不完整，缺少预期结果")
    
    # 输出总结果
    success_rate = success_count / total_tests * 100
    logger.info(f"测试完成: {success_count}/{total_tests} 通过 (成功率: {success_rate:.2f}%)")
    
    return success_count, total_tests

def test_handle_multiline_json():
    """测试多行JSON处理函数"""
    logger.info("开始测试多行JSON处理函数")
    
    # 创建NL2SQLProcessor实例
    processor = NL2SQLProcessor(db_name="test_db")
    
    # 测试用例列表
    test_cases = [
        {
            "name": "测试 - 简单JSON对象",
            "content": "{\"key\": \"value\"}",
            "expected_result": {"key": "value"}
        },
        {
            "name": "测试 - 带转义字符的JSON",
            "content": "{\\\"key\\\": \\\"value\\\"}",
            "expected_result": {"key": "value"}
        },
        {
            "name": "测试 - 多行JSON",
            "content": "{\n  \"key1\": \"value1\",\n  \"key2\": \"value2\"\n}",
            "expected_result": {"key1": "value1", "key2": "value2"}
        },
        {
            "name": "测试 - JSON在文本中间",
            "content": "前导文本\n{\"key\": \"value\"}\n后续文本",
            "expected_result": {"key": "value"}
        },
        {
            "name": "测试 - 在代码块中的JSON",
            "content": "```json\n{\"key\": \"value\"}\n```",
            "expected_result": {"key": "value"}
        },
        {
            "name": "测试 - 在think标签后的JSON",
            "content": "<think>思考过程</think>\n{\"key\": \"value\"}",
            "expected_result": {"key": "value"}
        },
        {
            "name": "测试 - 转义的尖括号",
            "content": "\\<think\\>内容\\</think\\> {\"key\": \"value\"}",
            "expected_result": {"key": "value"}
        }
    ]
    
    # 测试结果统计
    success_count = 0
    total_tests = len(test_cases)
    
    # 运行测试用例
    for i, test_case in enumerate(test_cases):
        logger.info(f"运行测试用例 {i+1}/{total_tests}: {test_case['name']}")
        
        result = processor._handle_multiline_json(test_case["content"])
        
        # 检查结果是否符合预期
        if "expected_result" in test_case:
            expected = test_case["expected_result"]
            if result == expected:
                logger.info(f"测试通过: 解析结果符合预期")
                success_count += 1
            else:
                logger.error(f"测试失败: 预期结果 {expected}，实际结果 {result}")
        else:
            logger.warning(f"测试用例定义不完整，缺少预期结果")
    
    # 输出总结果
    success_rate = success_count / total_tests * 100
    logger.info(f"测试完成: {success_count}/{total_tests} 通过 (成功率: {success_rate:.2f}%)")
    
    return success_count, total_tests

def test_check_business_query_with_llm():
    """测试业务查询检查函数的鲁棒性"""
    logger.info("开始测试业务查询检查函数")
    
    # 创建模拟的LLM客户端
    class MockLLMClient:
        def __init__(self, responses):
            self.responses = responses
            self.call_count = 0
            
        def generate(self, system_prompt, user_prompt, model_config):
            # 获取当前测试的响应
            response = self.responses[self.call_count % len(self.responses)]
            self.call_count += 1
            return response
    
    # 模拟响应
    mock_responses = [
        # 响应1: 正常JSON响应
        "{\"is_business_query\": true, \"confidence\": 0.9, \"reasoning\": \"查询与销售相关\"}",
        
        # 响应2: 只有<think>标签，第2次调用成功
        "<think>",
        "{\"is_business_query\": true, \"confidence\": 0.8, \"reasoning\": \"第二次尝试成功\"}",
        
        # 响应3: 带有未闭合的<think>标签，第3次调用成功
        "<think>这是一些思考",
        "第二次尝试的响应还是不完整",
        "{\"is_business_query\": false, \"confidence\": 0.7, \"reasoning\": \"第三次尝试成功\"}",
        
        # 响应4: 使用关键词匹配的情况
        "不是有效的JSON格式",
        "仍然不是有效的JSON",
        "依然不是有效的JSON"
    ]
    
    # 准备测试环境：模拟必要的方法和属性
    # 1. 修改NL2SQLProcessor类，添加临时测试属性
    original_init = NL2SQLProcessor.__init__
    
    def mock_init(self, db_name=None):
        original_init(self, db_name)
        # 添加测试需要的属性
        self.llm_client = MockLLMClient(mock_responses)
        self.model_config = {"temperature": 0.1}
    
    # 2. 临时替换__init__方法
    NL2SQLProcessor.__init__ = mock_init
    
    try:
        # 使用patch模拟log_query_process函数
        with patch('src.utils.nl2sql_processor.log_query_process', side_effect=mock_log_query_process):
            # 创建处理器实例
            processor = NL2SQLProcessor(db_name="test_db")
            
            # 测试用例
            test_cases = [
                {
                    "name": "测试 - 正常JSON响应",
                    "query": "最近销量如何？",
                    "expected_is_business": True,
                    "expected_confidence": 0.9
                },
                {
                    "name": "测试 - 只有<think>标签，需要重试",
                    "query": "本季度利润？",
                    "expected_is_business": True,
                    "expected_confidence": 0.8
                },
                {
                    "name": "测试 - 未闭合标签，需要多次重试",
                    "query": "讲个笑话",
                    "expected_is_business": False,
                    "expected_confidence": 0.7
                },
                {
                    "name": "测试 - 使用关键词匹配",
                    "query": "我们的销售业绩怎么样？",
                    "expected_is_business": True
                }
            ]
            
            # 测试结果统计
            success_count = 0
            total_tests = len(test_cases)
            
            # 运行测试用例
            for i, test_case in enumerate(test_cases):
                logger.info(f"运行测试用例 {i+1}/{total_tests}: {test_case['name']}")
                
                try:
                    result = processor._check_business_query_with_llm(test_case["query"])
                    
                    # 检查结果是否符合预期
                    is_business = result.get("is_business_query", False)
                    confidence = result.get("confidence", 0)
                    
                    logger.info(f"查询: '{test_case['query']}' -> is_business={is_business}, confidence={confidence}")
                    
                    # 验证结果
                    expected_is_business = test_case.get("expected_is_business")
                    if expected_is_business is not None and is_business == expected_is_business:
                        if "expected_confidence" in test_case:
                            expected_confidence = test_case["expected_confidence"]
                            if abs(confidence - expected_confidence) <= 0.1:  # 允许小误差
                                logger.info(f"测试通过: 业务查询判断和置信度都符合预期")
                                success_count += 1
                            else:
                                logger.error(f"测试部分失败: 业务查询判断正确，但置信度不符合预期 (预期: {expected_confidence}, 实际: {confidence})")
                        else:
                            logger.info(f"测试通过: 业务查询判断符合预期")
                            success_count += 1
                    else:
                        logger.error(f"测试失败: 业务查询判断不符合预期 (预期: {expected_is_business}, 实际: {is_business})")
                except Exception as e:
                    logger.error(f"测试执行过程中出错: {str(e)}")
            
            # 输出总结果
            success_rate = success_count / total_tests * 100
            logger.info(f"测试完成: {success_count}/{total_tests} 通过 (成功率: {success_rate:.2f}%)")
            
            return success_count, total_tests
    finally:
        # 恢复原始__init__方法
        NL2SQLProcessor.__init__ = original_init

def main():
    """主测试函数"""
    # 加载环境变量
    load_dotenv()
    
    logger.info("开始测试NL2SQL处理器修复")
    
    # 测试JSON响应解析
    logger.info("\n==== 测试JSON响应解析 ====")
    json_success, json_total = test_parse_llm_json_response()
    
    # 测试多行JSON处理
    logger.info("\n==== 测试多行JSON处理 ====")
    multiline_success, multiline_total = test_handle_multiline_json()
    
    # 测试业务查询检查
    logger.info("\n==== 测试业务查询检查 ====")
    business_success, business_total = test_check_business_query_with_llm()
    
    # 总体结果
    total_success = json_success + multiline_success + business_success
    total_tests = json_total + multiline_total + business_total
    total_success_rate = total_success / total_tests * 100
    
    logger.info("\n==== 测试总结 ====")
    logger.info(f"JSON响应解析: {json_success}/{json_total} 通过")
    logger.info(f"多行JSON处理: {multiline_success}/{multiline_total} 通过")
    logger.info(f"业务查询检查: {business_success}/{business_total} 通过")
    logger.info(f"总计: {total_success}/{total_tests} 通过 (成功率: {total_success_rate:.2f}%)")
    
    # 返回测试是否全部通过
    return total_success == total_tests

if __name__ == "__main__":
    # 设置工作目录为项目根目录
    os.chdir(project_root)
    
    # 运行测试
    all_tests_passed = main()
    
    # 根据测试结果设置退出码
    sys.exit(0 if all_tests_passed else 1) 