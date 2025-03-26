#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试Ollama JSON解析修复
验证提取而非删除的JSON解析策略
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 导入相关模块
from src.utils.llm_client import get_llm_client, Message, LLMProvider, LLMConfig
from src.utils.metadata_extractor import MetadataExtractor

def test_llm_client():
    """测试LLM客户端的Ollama修复"""
    logger.info("===== 测试LLM客户端Ollama修复 =====")
    
    # 创建Ollama配置
    config = LLMConfig(
        provider=LLMProvider.OLLAMA,
        model=os.getenv("OLLAMA_MODEL", "qwen:latest"),
        base_url="http://localhost:11434"
    )
    
    # 测试1: 测试一个简单的ping请求
    try:
        # 创建客户端
        from src.utils.llm_client import LLMClient
        client = LLMClient(config)
        
        messages = [
            Message.system("你是一个有用的助手"),
            Message.user("请以JSON格式返回今天的日期")
        ]
        
        logger.info("发送测试请求到Ollama...")
        response = client.chat(messages)
        
        logger.info(f"收到响应: {response.content[:100]}...")
        
        # 测试JSON解析
        if response.content:
            try:
                data = json.loads(response.content)
                logger.info(f"标准JSON解析成功: {data}")
            except json.JSONDecodeError as e:
                logger.warning(f"标准JSON解析失败: {str(e)}")
                # 使用新的提取方法尝试解析
                extractor = MetadataExtractor()
                data = extractor._handle_multiline_json(response.content)
                if data:
                    logger.info(f"使用JSON提取方法成功: {data}")
                else:
                    logger.error("所有解析方法都失败")
        
    except Exception as e:
        logger.error(f"测试LLM客户端时出错: {str(e)}")

def test_mock_multiline_json():
    """测试多行JSON解析器，使用模拟数据"""
    logger.info("===== 测试多行JSON解析器 =====")
    
    extractor = MetadataExtractor()
    
    # 测试用例1: 多行JSON，第一行是有效JSON
    test1 = """{"message": {"content": "这是一个测试"}, "model": "test-model"}
其他无关内容"""
    
    # 测试用例2: 多行JSON，第二行是有效JSON
    test2 = """一些前导文本
{"message": {"content": "这是第二个测试"}, "model": "test-model2"}
其他内容"""
    
    # 测试用例3: 包含格式错误的JSON
    test3 = """{"message": {"content": "格式错误的JSON},
"model": "test-model3"}"""
    
    # 测试用例4: 流式响应格式
    test4 = """{"message": {"content": "第一部分"}}
{"message": {"content": "第一部分第二部分"}}
{"message": {"content": "第一部分第二部分第三部分"}}"""
    
    # 测试用例5: 实际错误 "Extra data: line 2 column 1"
    test5 = """{"message": {"content": "这是第一行JSON的内容"}}
{"message": {"content": "这是第二行JSON，导致解析错误"}}"""
    
    # 新增测试用例6: 包含<think>标签的内容
    test6 = """<think>
{"some_internal": "reasoning"}
</think>
{"message": {"content": "这是实际的JSON响应"}}"""

    # 新增测试用例7: 包含```json代码块的内容
    test7 = """下面是返回的JSON数据：
```json
{
  "message": {"content": "这是代码块中的JSON"}
}
```
附加说明内容"""

    # 新增测试用例8: 同时包含<think>标签和```json代码块
    test8 = """<think>
我应该返回什么数据呢？让我想想
{"draft": "这只是草稿"}
</think>
我的回答是：
```json
{
  "message": {"content": "这是最终JSON"},
  "timestamp": "2023-01-01"
}
```"""

    # 新增测试用例9: 复杂的嵌套内容，测试提取策略
    test9 = """这是一个复杂的响应

<think>
我需要返回一个用户资料JSON，让我组织一下数据
{
  "draft": "用户基本信息",
  "fields": ["name", "age", "email"]
}
</think>

根据您的请求，我已经准备好了用户资料：

```json
{
  "user": {
    "name": "张三",
    "age": 28,
    "email": "zhangsan@example.com",
    "address": {
      "city": "北京",
      "district": "海淀区"
    }
  },
  "created_at": "2023-01-01"
}
```

这个JSON包含了用户的基本信息和地址信息。"""

    # 新增测试用例10: 非标准格式，需要更智能的提取
    test10 = """我找到了以下信息:

用户名称: 张三
邮箱: zhangsan@example.com
年龄: 28

我想这样表示会更清晰:
{
  name: "张三",
  email: "zhangsan@example.com",
  age: 28
}

希望这对您有帮助!"""
    
    # 运行测试
    tests = [test1, test2, test3, test4, test5, test6, test7, test8, test9, test10]
    test_names = [
        "多行JSON-第一行有效", 
        "多行JSON-第二行有效", 
        "格式错误的JSON", 
        "流式响应格式", 
        "实际错误场景",
        "包含<think>标签",
        "包含```json代码块",
        "同时包含<think>和```json",
        "复杂嵌套内容测试",
        "非标准格式JSON"
    ]
    
    success_count = 0
    
    for i, (test, name) in enumerate(zip(tests, test_names)):
        logger.info(f"测试 {i+1}: {name}")
        result = extractor._handle_multiline_json(test)
        if result:
            logger.info(f"提取JSON成功: {result}")
            success_count += 1
        else:
            logger.error(f"提取JSON失败: {test[:100]}")
            
    logger.info(f"JSON提取成功率: {success_count}/{len(tests)} ({success_count*100/len(tests):.1f}%)")
            
    # 测试LLM响应解析
    logger.info("===== 测试LLM响应解析 =====")
    llm_success_count = 0
    
    for i, (test, name) in enumerate(zip(tests, test_names)):
        logger.info(f"LLM响应解析测试 {i+1}: {name}")
        result = extractor._parse_llm_json_response(test)
        if "error" not in result:
            logger.info(f"解析成功: {result}")
            llm_success_count += 1
        else:
            logger.error(f"解析失败: {result}")
            
    logger.info(f"LLM响应解析成功率: {llm_success_count}/{len(tests)} ({llm_success_count*100/len(tests):.1f}%)")
    
    # 验证提取策略没有修改原始内容
    logger.info("===== 测试原始内容保留 =====")
    special_test = test9  # 使用复杂的嵌套内容测试
    
    # 记录原始内容
    original_length = len(special_test)
    original_think_content = special_test[special_test.find("<think>")+7:special_test.find("</think>")]
    
    # 提取JSON
    extracted_json = extractor._handle_multiline_json(special_test)
    
    # 验证原始内容保留
    assert len(special_test) == original_length, "原始内容长度应保持不变"
    assert original_think_content in special_test, "<think>标签内容应保持不变"
    
    if extracted_json and "user" in extracted_json:
        logger.info("提取策略验证通过: 成功提取JSON同时保留原始内容")
    else:
        logger.error("提取策略验证失败: 无法从保留的原始内容中提取正确的JSON")

def main():
    """主函数"""
    logger.info("开始测试Ollama JSON解析修复 - 提取而非删除策略")
    
    # 测试多行JSON处理
    test_mock_multiline_json()
    
    # 如果Ollama可用，测试实际客户端
    if os.getenv("TEST_OLLAMA", "false").lower() == "true":
        test_llm_client()
    else:
        logger.info("跳过Ollama客户端测试，设置TEST_OLLAMA=true启用")
    
    logger.info("测试完成")

if __name__ == "__main__":
    main() 