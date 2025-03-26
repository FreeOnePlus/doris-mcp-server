#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查MLX-LM库函数参数
"""

import os
import sys
import inspect
import logging
from pathlib import Path

# 设置项目根目录到系统路径中
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.insert(0, PROJECT_ROOT)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_mlx_lm_functions():
    """检查MLX-LM库中的函数参数"""
    try:
        # 导入mlx_lm模块
        from mlx_lm import utils
        
        # 检查generate函数
        if hasattr(utils, 'generate'):
            logger.info("Found generate function")
            sig = inspect.signature(utils.generate)
            logger.info(f"Generate function signature: {sig}")
            logger.info(f"Generate function parameters: {list(sig.parameters.keys())}")
            logger.info(f"Generate function docstring: {utils.generate.__doc__}")
        else:
            logger.warning("generate function not found")
        
        # 检查stream_generate函数
        if hasattr(utils, 'stream_generate'):
            logger.info("Found stream_generate function")
            sig = inspect.signature(utils.stream_generate)
            logger.info(f"Stream_generate function signature: {sig}")
            logger.info(f"Stream_generate function parameters: {list(sig.parameters.keys())}")
            logger.info(f"Stream_generate function docstring: {utils.stream_generate.__doc__}")
        else:
            logger.warning("stream_generate function not found")
        
        # 检查generate_step函数
        if hasattr(utils, 'generate_step'):
            logger.info("Found generate_step function")
            sig = inspect.signature(utils.generate_step)
            logger.info(f"Generate_step function signature: {sig}")
            logger.info(f"Generate_step function parameters: {list(sig.parameters.keys())}")
            logger.info(f"Generate_step function docstring: {utils.generate_step.__doc__}")
        else:
            logger.warning("generate_step function not found but it's referenced in errors")
        
        # 查找所有可能与生成相关的函数
        logger.info("All functions in mlx_lm.utils that might be related to generation:")
        for name, func in inspect.getmembers(utils, inspect.isfunction):
            if 'generate' in name.lower() or 'sample' in name.lower() or 'token' in name.lower():
                logger.info(f"Function: {name}")
                sig = inspect.signature(func)
                logger.info(f"  Signature: {sig}")
                logger.info(f"  Parameters: {list(sig.parameters.keys())}")
        
        return True
    except Exception as e:
        logger.error(f"Error checking MLX-LM functions: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    logger.info("Checking MLX-LM library functions...")
    check_mlx_lm_functions()
    logger.info("Done") 