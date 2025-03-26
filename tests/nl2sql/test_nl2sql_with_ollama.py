#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试NL2SQLProcessor与Ollama的集成

本脚本用于测试NL2SQLProcessor类使用Ollama本地模型的功能。
"""

import os
import sys
import time
import json
import logging
import traceback
from dotenv import load_dotenv

# 设置项目根目录到系统路径中
# 关键修复: 确保正确找到项目根目录，使src模块可导入
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.insert(0, PROJECT_ROOT)
print(f"设置项目根目录: {PROJECT_ROOT}")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_ollama_environment():
    """设置Ollama环境变量"""
    # 设置环境变量
    os.environ["LLM_PROVIDER"] = "ollama"
    
    # 默认模型名称，如果环境变量中没有指定
    if not os.getenv("OLLAMA_MODEL"):
        os.environ["OLLAMA_MODEL"] = "llama2"
    
    # 默认API基础URL，如果环境变量中没有指定
    if not os.getenv("OLLAMA_BASE_URL"):
        os.environ["OLLAMA_BASE_URL"] = "http://localhost:11434"
    
    logger.info("Ollama环境变量设置完成")

def test_load_nl2sql_processor():
    """测试加载NL2SQLProcessor类"""
    try:
        # 确保使用Ollama
        setup_ollama_environment()
        
        # 尝试导入NL2SQLProcessor
        try:
            from src.utils.nl2sql_processor import NL2SQLProcessor
            logger.info("成功导入NL2SQLProcessor")
            return True
        except ImportError as e:
            logger.error(f"导入NL2SQLProcessor失败: {e}")
            traceback.print_exc()
            return False
    except Exception as e:
        logger.error(f"测试加载NL2SQLProcessor时出错: {e}")
        traceback.print_exc()
        return False

def test_check_ollama_connection():
    """测试Ollama连接情况"""
    try:
        # 确保使用Ollama
        setup_ollama_environment()
        
        # 获取Ollama基础URL
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        
        # 尝试检查Ollama服务是否可用
        try:
            import requests
            logger.info(f"尝试连接Ollama服务: {base_url}")
            
            response = requests.get(f"{base_url}/api/tags")
            if response.status_code == 200:
                models = response.json().get("models", [])
                logger.info(f"Ollama服务连接成功，可用模型: {len(models)}")
                for model in models:
                    logger.info(f"- {model.get('name')}")
                return True
            else:
                logger.error(f"Ollama服务连接失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"连接Ollama服务时出错: {e}")
            traceback.print_exc()
            return False
    except Exception as e:
        logger.error(f"测试Ollama连接时出错: {e}")
        traceback.print_exc()
        return False

def test_initialize_nl2sql_processor():
    """测试初始化NL2SQLProcessor"""
    try:
        # 确保使用Ollama
        setup_ollama_environment()
        
        # 导入并初始化NL2SQLProcessor
        try:
            from src.utils.nl2sql_processor import NL2SQLProcessor
            processor = NL2SQLProcessor()
            logger.info(f"成功初始化NL2SQLProcessor: {processor}")
            
            # 检查数据库名称是否已设置
            db_name = processor.db_name
            logger.info(f"数据库名称: {db_name}")
            
            # 检查元数据提取器是否已初始化
            if hasattr(processor, 'metadata_extractor'):
                logger.info(f"元数据提取器已初始化: {processor.metadata_extractor}")
                return True
            else:
                logger.warning("元数据提取器未初始化")
                return False
        except Exception as e:
            logger.error(f"初始化NL2SQLProcessor时出错: {e}")
            traceback.print_exc()
            return False
    except Exception as e:
        logger.error(f"测试初始化NL2SQLProcessor时出错: {e}")
        traceback.print_exc()
        return False

def test_business_query_check():
    """测试业务查询判断功能"""
    try:
        # 确保使用Ollama
        setup_ollama_environment()
        
        # 导入并初始化NL2SQLProcessor
        try:
            from src.utils.nl2sql_processor import NL2SQLProcessor
            processor = NL2SQLProcessor()
            
            # 测试一个明显的业务查询
            query = "统计近7天每天的销售总额"
            is_business, confidence = processor._check_if_business_query(query)
            
            logger.info(f"查询: {query}")
            logger.info(f"是否是业务查询: {is_business}")
            logger.info(f"置信度: {confidence}")
            
            # 测试一个明显的非业务查询
            non_business_query = "你好，今天天气怎么样？"
            is_business, confidence = processor._check_if_business_query(non_business_query)
            
            logger.info(f"查询: {non_business_query}")
            logger.info(f"是否是业务查询: {is_business}")
            logger.info(f"置信度: {confidence}")
            
            return True
        except Exception as e:
            logger.error(f"测试业务查询判断功能时出错: {e}")
            traceback.print_exc()
            return False
    except Exception as e:
        logger.error(f"测试业务查询判断功能时出错: {e}")
        traceback.print_exc()
        return False

def test_full_process():
    """测试完整的NL2SQL处理过程"""
    try:
        # 确保使用Ollama
        setup_ollama_environment()
        
        # 导入并初始化NL2SQLProcessor
        try:
            from src.utils.nl2sql_processor import NL2SQLProcessor
            processor = NL2SQLProcessor()
            
            # 测试一个简单的业务查询
            query = "查询全部客户信息"
            logger.info(f"执行查询: {query}")
            
            start_time = time.time()
            result = processor.process(query)
            execution_time = time.time() - start_time
            
            logger.info(f"查询处理完成，耗时: {execution_time:.2f}秒")
            
            # 打印结果
            if result.get('success', False):
                logger.info("查询成功")
                logger.info(f"SQL: {result.get('sql', '')}")
                logger.info(f"查询行数: {result.get('row_count', 0)}")
                logger.info(f"执行时间: {result.get('execution_time', 0):.2f}秒")
                
                # 仅显示前5行数据
                data = result.get('data', [])
                if data:
                    logger.info(f"数据结果(前5行): {json.dumps(data[:5], ensure_ascii=False, indent=2)}")
            else:
                logger.warning("查询失败")
                logger.warning(f"错误信息: {result.get('message', '未知错误')}")
            
            return result.get('success', False)
        except Exception as e:
            logger.error(f"测试完整处理过程时出错: {e}")
            traceback.print_exc()
            return False
    except Exception as e:
        logger.error(f"测试完整处理过程时出错: {e}")
        traceback.print_exc()
        return False

def main():
    """主函数"""
    try:
        # 加载环境变量
        load_dotenv()
        
        print("=" * 50)
        print("开始NL2SQL + Ollama集成测试")
        print("=" * 50)
        
        # 测试1: 检查是否能导入NL2SQLProcessor
        print("\n1. 测试导入NL2SQLProcessor:")
        if test_load_nl2sql_processor():
            print("✅ 测试通过: 成功导入NL2SQLProcessor")
        else:
            print("❌ 测试失败: 无法导入NL2SQLProcessor")
            return
        
        # 测试2: 检查Ollama连接情况
        print("\n2. 测试Ollama连接情况:")
        if test_check_ollama_connection():
            print("✅ 测试通过: Ollama服务连接正常")
        else:
            print("❌ 测试失败: Ollama服务连接异常")
            return
        
        # 测试3: 检查是否能初始化NL2SQLProcessor
        print("\n3. 测试初始化NL2SQLProcessor:")
        if test_initialize_nl2sql_processor():
            print("✅ 测试通过: 成功初始化NL2SQLProcessor")
        else:
            print("❌ 测试失败: 无法初始化NL2SQLProcessor")
            return
        
        # 测试4: 测试业务查询判断功能
        print("\n4. 测试业务查询判断功能:")
        if test_business_query_check():
            print("✅ 测试通过: 业务查询判断功能正常")
        else:
            print("❌ 测试失败: 业务查询判断功能异常")
            return
        
        # 测试5: 测试完整的处理过程
        print("\n5. 测试完整的NL2SQL处理过程:")
        if test_full_process():
            print("✅ 测试通过: 完整处理过程正常")
        else:
            print("❌ 测试失败: 完整处理过程异常")
            return
        
        print("\n" + "=" * 50)
        print("所有测试已完成")
        print("=" * 50)
        
    except Exception as e:
        print(f"测试过程中出现未处理的异常: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 