#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试多LLM阶段配置的加载
"""

import os
import logging
import sys
from dotenv import load_dotenv

# 添加项目根目录到路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_multi_llm_config():
    """测试多LLM阶段配置的正确加载"""
    
    # 设置测试环境变量
    os.environ.update({
        # 默认LLM配置
        "LLM_PROVIDER": "openai",
        "OPENAI_MODEL": "gpt-3.5-turbo",
        
        # 业务查询检查阶段配置
        "LLM_PROVIDER_BUSINESS_CHECK": "ollama",
        "LLM_MODEL_BUSINESS_CHECK": "qwen:0.5b",
        
        # SQL生成阶段配置
        "LLM_PROVIDER_SQL_GENERATION": "openai",
        "LLM_MODEL_SQL_GENERATION": "gpt-4o",
        
        # 未配置阶段（使用默认值）
        # similar_example阶段无特定配置
    })
    
    # 导入LLM客户端
    from src.utils.llm_client import get_llm_client
    
    # 测试不同阶段的配置加载
    try:
        # 测试业务查询检查阶段
        business_check_client = get_llm_client(stage="business_check")
        logger.info(f"业务查询检查阶段配置 - 供应商: {business_check_client.config.provider.value}, 模型: {business_check_client.config.model}")
        
        # 测试SQL生成阶段
        sql_gen_client = get_llm_client(stage="sql_generation")
        logger.info(f"SQL生成阶段配置 - 供应商: {sql_gen_client.config.provider.value}, 模型: {sql_gen_client.config.model}")
        
        # 测试未配置特定LLM的阶段（应使用默认值）
        similar_example_client = get_llm_client(stage="similar_example")
        logger.info(f"相似示例阶段配置 - 供应商: {similar_example_client.config.provider.value}, 模型: {similar_example_client.config.model}")
        
        # 测试不指定阶段（应使用默认值）
        default_client = get_llm_client()
        logger.info(f"默认配置 - 供应商: {default_client.config.provider.value}, 模型: {default_client.config.model}")
        
        # 验证配置是否符合预期
        assert business_check_client.config.provider.value == "ollama", "业务查询检查阶段应使用ollama供应商"
        assert business_check_client.config.model == "qwen:0.5b", "业务查询检查阶段应使用qwen:0.5b模型"
        
        assert sql_gen_client.config.provider.value == "openai", "SQL生成阶段应使用openai供应商"
        assert sql_gen_client.config.model == "gpt-4o", "SQL生成阶段应使用gpt-4o模型"
        
        assert similar_example_client.config.provider.value == "openai", "相似示例阶段应使用默认openai供应商"
        assert similar_example_client.config.model == "gpt-3.5-turbo", "相似示例阶段应使用默认gpt-3.5-turbo模型"
        
        assert default_client.config.provider.value == "openai", "默认应使用openai供应商"
        assert default_client.config.model == "gpt-3.5-turbo", "默认应使用gpt-3.5-turbo模型"
        
        logger.info("多LLM阶段配置测试通过！")
        return True
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        return False

if __name__ == "__main__":
    print("开始测试多LLM阶段配置...")
    success = test_multi_llm_config()
    if success:
        print("测试成功: 多LLM阶段配置正常工作")
        sys.exit(0)
    else:
        print("测试失败: 请检查配置和代码")
        sys.exit(1) 