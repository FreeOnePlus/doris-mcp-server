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
# 注意: 确保正确找到项目根目录，使src模块可导入
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
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
        
        # 尝试导入模块，打印导入路径以便调试
        logger.info(f"Python导入路径: {sys.path}")
        
        # 导入NL2SQLProcessor
        from src.utils.nl2sql_processor import NL2SQLProcessor
        
        logger.info("NL2SQLProcessor类导入成功")
        return True, {"success": True, "message": "NL2SQLProcessor类导入成功"}
    except Exception as e:
        logger.error(f"导入NL2SQLProcessor失败: {str(e)}")
        traceback_str = traceback.format_exc()
        logger.error(f"异常堆栈: {traceback_str}")
        return False, {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback_str
        }

def test_initialize_nl2sql_processor():
    """测试初始化NL2SQLProcessor类"""
    try:
        # 设置环境变量，确保使用Ollama
        setup_ollama_environment()
        
        # 导入并初始化NL2SQLProcessor
        from src.utils.nl2sql_processor import NL2SQLProcessor
        
        logger.info("正在初始化NL2SQLProcessor...")
        start_time = time.time()
        processor = NL2SQLProcessor()
        initialization_time = time.time() - start_time
        
        logger.info(f"NL2SQLProcessor初始化成功，耗时: {initialization_time:.2f}秒")
        return True, {
            "success": True,
            "initialization_time": initialization_time
        }
    except Exception as e:
        logger.error(f"初始化NL2SQLProcessor失败: {str(e)}")
        traceback_str = traceback.format_exc()
        logger.error(f"异常堆栈: {traceback_str}")
        return False, {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback_str
        }

def test_business_query_check():
    """测试业务查询判断功能"""
    try:
        # 设置环境变量，确保使用Ollama
        setup_ollama_environment()
        
        # 导入并初始化NL2SQLProcessor
        from src.utils.nl2sql_processor import NL2SQLProcessor
        
        processor = NL2SQLProcessor()
        
        # 测试业务查询判断
        test_query = "查询最近一个月销售额超过1000元的产品"
        logger.info(f"测试业务查询判断，查询: '{test_query}'")
        
        start_time = time.time()
        is_business, confidence = processor._check_if_business_query(test_query)
        execution_time = time.time() - start_time
        
        logger.info(f"业务查询判断完成，耗时: {execution_time:.2f}秒")
        logger.info(f"判断结果: {'是' if is_business else '不是'}业务查询，置信度: {confidence:.2f}")
        
        return True, {
            "success": True,
            "is_business_query": is_business,
            "confidence": confidence,
            "execution_time": execution_time,
            "query": test_query
        }
    except Exception as e:
        logger.error(f"业务查询判断测试失败: {str(e)}")
        traceback_str = traceback.format_exc()
        logger.error(f"异常堆栈: {traceback_str}")
        return False, {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback_str
        }

def test_full_process():
    """测试完整的NL2SQL处理流程"""
    try:
        # 设置环境变量，确保使用Ollama
        setup_ollama_environment()
        
        # 导入并初始化NL2SQLProcessor
        from src.utils.nl2sql_processor import NL2SQLProcessor
        
        processor = NL2SQLProcessor()
        
        # 测试完整处理流程
        test_query = "查询最近30天内，订单金额超过1000元的用户数量"
        logger.info(f"测试完整处理流程，查询: '{test_query}'")
        
        start_time = time.time()
        result = processor.process(test_query)
        execution_time = time.time() - start_time
        
        logger.info(f"处理完成，耗时: {execution_time:.2f}秒")
        logger.info(f"处理结果: {'成功' if result.get('success') else '失败'}")
        
        if result.get('success') and 'sql' in result:
            logger.info(f"生成的SQL: {result.get('sql')}")
        else:
            logger.info(f"消息: {result.get('message', '无消息')}")
        
        return True, {
            "success": True,
            "process_result": result,
            "execution_time": execution_time,
            "query": test_query
        }
    except Exception as e:
        logger.error(f"完整处理流程测试失败: {str(e)}")
        traceback_str = traceback.format_exc()
        logger.error(f"异常堆栈: {traceback_str}")
        return False, {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback_str
        }

def main():
    """主函数，运行所有测试"""
    # 加载环境变量
    load_dotenv()
    
    # 确保使用Ollama
    setup_ollama_environment()
    
    # 获取当前的环境变量
    logger.info(f"LLM_PROVIDER: {os.getenv('LLM_PROVIDER')}")
    logger.info(f"OLLAMA_MODEL: {os.getenv('OLLAMA_MODEL')}")
    logger.info(f"OLLAMA_BASE_URL: {os.getenv('OLLAMA_BASE_URL')}")
    
    results = {}
    
    # 测试1: 加载NL2SQLProcessor
    logger.info("\n===== 测试1: 加载NL2SQLProcessor =====")
    load_success, load_result = test_load_nl2sql_processor()
    results["load_processor"] = load_result
    
    if not load_success:
        logger.error("加载NL2SQLProcessor失败，后续测试中止")
        return results
    
    # 测试2: 初始化NL2SQLProcessor
    logger.info("\n===== 测试2: 初始化NL2SQLProcessor =====")
    init_success, init_result = test_initialize_nl2sql_processor()
    results["initialize_processor"] = init_result
    
    if not init_success:
        logger.error("初始化NL2SQLProcessor失败，后续测试中止")
        return results
    
    # 测试3: 业务查询判断
    logger.info("\n===== 测试3: 业务查询判断 =====")
    business_success, business_result = test_business_query_check()
    results["business_query_check"] = business_result
    
    # 测试4: 完整处理流程
    logger.info("\n===== 测试4: 完整处理流程 =====")
    process_success, process_result = test_full_process()
    results["full_process"] = process_result
    
    # 总结结果
    logger.info("\n===== 测试总结 =====")
    all_tests_passed = load_success and init_success and business_success and process_success
    
    if all_tests_passed:
        logger.info("✅ 所有测试通过! NL2SQLProcessor与Ollama集成功能正常。")
    else:
        logger.info("❌ 部分测试失败，详情请查看上方日志。")
    
    # 输出结果摘要
    logger.info("\n测试结果摘要:")
    for test_name, result in results.items():
        status = "✅ 通过" if result.get("success") else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    return results

if __name__ == "__main__":
    try:
        results = main()
        # 结果保存为JSON文件
        with open("nl2sql_ollama_test_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"测试结果已保存到 nl2sql_ollama_test_results.json")
    except Exception as e:
        logger.error(f"测试运行失败: {str(e)}")
        logger.error(f"异常堆栈: {traceback.format_exc()}") 