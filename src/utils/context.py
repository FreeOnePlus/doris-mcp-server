"""
上下文对象模块

提供一个简单的Context类，用于在内部工具调用之间传递参数。
"""

class SimpleContext:
    """
    简单的上下文对象，支持通过属性访问参数。
    用于在mcp_doris_tools.py和metadata_tools.py之间传递参数。
    """
    
    def __init__(self, data=None):
        """
        初始化上下文对象
        
        Args:
            data: 初始数据字典
        """
        if data is None:
            data = {}
        self._data = data
        
        # 如果传入的是{'params': {...}}形式，直接设置params属性
        if 'params' in data:
            self.params = data['params']
        else:
            self.params = {}
    
    def __getattr__(self, name):
        """
        允许通过属性访问字典值
        
        Args:
            name: 属性名
            
        Returns:
            属性值，如果不存在则返回None
        """
        return self._data.get(name)
    
    def __setattr__(self, name, value):
        """
        设置属性
        
        Args:
            name: 属性名
            value: 属性值
        """
        if name.startswith('_'):
            super().__setattr__(name, value)
        else:
            self._data[name] = value
    
    def to_dict(self):
        """
        将上下文转换为字典
        
        Returns:
            字典形式的上下文数据
        """
        return self._data.copy() 