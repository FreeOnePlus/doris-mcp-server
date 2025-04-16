#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
工具注册中心

集中管理MCP工具，提供自动发现和动态加载机制
避免多处硬编码和多层注册问题
"""

import inspect
import logging
import importlib
import os
import sys
from typing import Dict, Any, Callable, List, Optional, Tuple, Union

# 获取日志记录器
logger = logging.getLogger(__name__)

# 添加Tool类定义
class Tool:
    """Tool类，用于封装工具函数和元数据"""
    
    def __init__(self, name: str, func: Callable, description: str = ""):
        """初始化工具
        
        Args:
            name: 工具名称
            func: 工具函数
            description: 工具描述
        """
        self.name = name
        self.func = func
        self.description = description
        self._signature = inspect.signature(func)
        
    def __call__(self, *args, **kwargs):
        """使Tool对象可调用，直接调用内部函数"""
        return self.func(*args, **kwargs)
    
    @property
    def parameters(self):
        """获取参数信息"""
        return {
            "type": "object",
            "properties": {
                param_name: {"type": "string"}
                for param_name in self._signature.parameters
                if param_name != "self"
            },
            "required": [
                param_name
                for param_name, param in self._signature.parameters.items()
                if param_name != "self" and param.default == inspect.Parameter.empty
            ]
        }
        
    def to_dict(self):
        """将工具转换为字典表示"""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters
        }

class ToolRegistry:
    """工具注册中心，用于集中管理和发现所有MCP工具"""
    
    _instance = None  # 单例模式
    
    def __new__(cls):
        """实现单例模式"""
        if cls._instance is None:
            cls._instance = super(ToolRegistry, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化工具注册中心"""
        if self._initialized:
            return
            
        # 工具字典 {name: Tool实例}
        self._tools: Dict[str, Tool] = {}
        # 已扫描的模块
        self._scanned_modules = set()
        self._initialized = True
        logger.info("工具注册中心初始化完成")
    
    def register(self, name: str, func: Callable, description: str = "") -> None:
        """
        注册工具
        
        Args:
            name: 工具名称
            func: 工具函数
            description: 工具描述
        """
        if name in self._tools:
            logger.warning(f"工具 {name} 已存在，将被覆盖")
        
        # 创建Tool实例
        tool = Tool(name, func, description)
        self._tools[name] = tool
        logger.info(f"注册工具: {name} - {description}")
    
    def register_with_decorator(self, name: str = None, description: str = "") -> Callable:
        """
        工具注册装饰器
        
        Args:
            name: 工具名称，默认使用函数名
            description: 工具描述，默认使用函数文档
            
        Returns:
            装饰器函数
        """
        def decorator(func: Callable) -> Callable:
            # 获取工具名称，优先使用传入的name，其次使用函数名
            tool_name = name if name else func.__name__
            
            # 获取工具描述，优先使用传入的description，其次使用函数文档
            tool_description = description
            if not tool_description and func.__doc__:
                # 提取文档的第一行作为描述
                tool_description = func.__doc__.strip().split('\n')[0]
            
            # 注册工具
            self.register(tool_name, func, tool_description)
            
            # 保存工具信息到函数属性
            func.tool_info = {
                'name': tool_name,
                'description': tool_description,
                'func': func  # 保存函数引用
            }
            
            # 返回原函数，不改变其行为
            return func
        
        return decorator
    
    def get(self, name: str) -> Optional[Callable]:
        """
        获取工具函数
        
        Args:
            name: 工具名称
            
        Returns:
            工具函数，如果不存在返回None
        """
        if name in self._tools:
            # 直接返回Tool对象，它是可调用的
            return self._tools[name]
        return None
    
    def get_all(self) -> Dict[str, Tool]:
        """
        获取所有工具
        
        Returns:
            所有工具的字典 {name: Tool实例}
        """
        return self._tools.copy()
    
    def get_tool_list(self) -> List[Dict[str, str]]:
        """
        获取工具列表，适用于MCP接口
        
        Returns:
            工具列表，每个工具包含name和description
        """
        tools = self.get_all()
        logger.info(f"获取工具列表，当前注册的工具: {list(tools.keys())}")
        return [
            {"name": name, "description": tool.description}
            for name, tool in tools.items()
        ]
    
    def scan_module(self, module_name: str) -> None:
        """
        扫描模块中的工具函数
        
        Args:
            module_name: 模块名称
        """
        if module_name in self._scanned_modules:
            logger.debug(f"模块 {module_name} 已扫描，跳过")
            return
        
        try:
            # 加载模块
            module = importlib.import_module(module_name)
            
            # 扫描模块中的函数和类
            for name, obj in inspect.getmembers(module):
                # 跳过以_开头的私有成员
                if name.startswith('_'):
                    continue
                
                # 如果是函数或方法
                if inspect.isfunction(obj) or inspect.ismethod(obj):
                    # 检查是否有tool_info属性（由装饰器添加）
                    if hasattr(obj, 'tool_info'):
                        tool_name = getattr(obj, 'tool_info').get('name', name)
                        tool_desc = getattr(obj, 'tool_info').get('description', '')
                        self.register(tool_name, obj, tool_desc)
                    # 或者检查函数名是否以特定前缀开头
                    elif name.startswith('tool_'):
                        tool_name = name[5:]  # 去除tool_前缀
                        tool_desc = obj.__doc__.strip().split('\n')[0] if obj.__doc__ else ""
                        self.register(tool_name, obj, tool_desc)
            
            # 标记为已扫描
            self._scanned_modules.add(module_name)
            logger.info(f"扫描模块 {module_name} 完成")
            
        except ImportError as e:
            logger.error(f"扫描模块 {module_name} 失败: {str(e)}")
    
    def scan_directory(self, directory: str, package_prefix: str = "") -> None:
        """
        扫描目录中的所有Python文件
        
        Args:
            directory: 目录路径
            package_prefix: 包前缀
        """
        try:
            # 检查目录是否存在
            if not os.path.isdir(directory):
                logger.error(f"目录 {directory} 不存在")
                return
            
            # 遍历目录中的所有文件和子目录
            for item in os.listdir(directory):
                full_path = os.path.join(directory, item)
                
                # 如果是Python文件
                if os.path.isfile(full_path) and item.endswith('.py') and not item.startswith('_'):
                    # 构建模块名
                    if package_prefix:
                        module_name = f"{package_prefix}.{item[:-3]}"
                    else:
                        module_name = item[:-3]
                    
                    # 扫描模块
                    self.scan_module(module_name)
                
                # 如果是目录且不是特殊目录
                elif os.path.isdir(full_path) and item not in ['__pycache__', '.git', '.venv', 'venv']:
                    # 构建新的包前缀
                    new_prefix = f"{package_prefix}.{item}" if package_prefix else item
                    
                    # 递归扫描子目录
                    self.scan_directory(full_path, new_prefix)
        
        except Exception as e:
            logger.error(f"扫描目录 {directory} 失败: {str(e)}")
    
    def clear(self) -> None:
        """清空注册的工具"""
        self._tools.clear()
        self._scanned_modules.clear()
        logger.info("工具注册表已清空")

def get_registry() -> ToolRegistry:
    """
    获取工具注册中心实例
    
    Returns:
        ToolRegistry: 工具注册中心实例
    """
    return ToolRegistry()

def register_tool(name: str = None, description: str = "") -> Callable:
    """
    工具注册装饰器
    
    Args:
        name: 工具名称，默认使用函数名
        description: 工具描述，默认使用函数文档
        
    Returns:
        装饰器函数
    """
    return get_registry().register_with_decorator(name, description)

def get_tool(name: str) -> Optional[Callable]:
    """
    获取工具函数
    
    Args:
        name: 工具名称
        
    Returns:
        工具函数，如果不存在返回None
    """
    return get_registry().get(name)

def get_all_tools() -> Dict[str, Tool]:
    """
    获取所有工具
    
    Returns:
        所有工具的字典 {name: Tool实例}
    """
    return get_registry().get_all()

def get_tool_list() -> List[Dict[str, str]]:
    """
    获取工具列表，适用于MCP接口
    
    Returns:
        工具列表，每个工具包含name和description
    """
    registry = get_registry()
    tools = registry.get_all()
    logger.info(f"获取工具列表，当前注册的工具: {list(tools.keys())}")
    return [
        {"name": name, "description": tool.description}
        for name, tool in tools.items()
    ]

def auto_discover() -> None:
    """
    自动发现并注册工具
    """
    registry = get_registry()
    
    # 获取项目根目录
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    src_dir = os.path.join(root_dir, 'src')
    
    # 添加项目根目录到Python路径
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)
    
    # 扫描src目录
    logger.info(f"开始扫描目录: {src_dir}")
    for root, dirs, files in os.walk(src_dir):
        # 跳过__pycache__目录
        if '__pycache__' in dirs:
            dirs.remove('__pycache__')
            
        # 扫描Python文件
        for file in files:
            if file.endswith('.py') and not file.startswith('_'):
                # 构建模块路径
                rel_path = os.path.relpath(root, root_dir)
                module_path = os.path.join(rel_path, file[:-3]).replace(os.path.sep, '.')
                logger.info(f"扫描模块: {module_path}")
                try:
                    registry.scan_module(module_path)
                except Exception as e:
                    logger.error(f"扫描模块 {module_path} 失败: {str(e)}")
    
    # 打印已注册的工具
    tools = registry.get_all()
    logger.info(f"工具注册完成，已注册工具: {list(tools.keys())}") 