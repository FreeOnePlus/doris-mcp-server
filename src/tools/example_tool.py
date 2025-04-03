#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
示例工具实现

演示如何使用工具注册装饰器创建工具
"""

from src.tools.tool_registry import register_tool

@register_tool(name="hello", description="一个简单的示例工具")
def hello(name: str = "世界") -> str:
    """
    返回一个问候消息
    
    Args:
        name: 要问候的名字
        
    Returns:
        问候消息
    """
    return f"你好，{name}！"

@register_tool(name="add", description="计算两个数的和")
def add(a: float, b: float) -> float:
    """
    计算两个数的和
    
    Args:
        a: 第一个数
        b: 第二个数
        
    Returns:
        两个数的和
    """
    return a + b 