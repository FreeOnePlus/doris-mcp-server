import json
from typing import Dict, List, Any, Optional
import requests

from ..utils.docs_search import doris_docs_search

def search_doris_docs(query: str, top_k: int = 3) -> str:
    """
    在Doris文档库中搜索相关内容
    
    Args:
        query: 搜索查询
        top_k: 返回结果数量
        
    Returns:
        str: 搜索结果
    """
    try:
        # 使用文档搜索工具搜索
        results = doris_docs_search.search(query, top_k=top_k)
        
        if not results:
            return f"未找到与查询 '{query}' 相关的文档。"
        
        # 格式化搜索结果
        output = f"## 查询 '{query}' 的文档搜索结果\n\n"
        
        for i, doc in enumerate(results):
            similarity = doc.get("similarity", 0) * 100
            output += f"### {i+1}. {doc['title']} (相关度: {similarity:.1f}%)\n\n"
            
            # 截取内容片段，避免过长
            content = doc['content'].strip()
            if len(content) > 500:
                content = content[:500] + "..."
                
            output += f"{content}\n\n"
            
        return output
    except Exception as e:
        return f"文档搜索出错: {str(e)}"

def search_doris_online(query: str, top_k: int = 3) -> str:
    """
    在线搜索Doris相关信息
    
    Args:
        query: 搜索查询
        top_k: 返回结果数量
        
    Returns:
        str: 搜索结果
    """
    try:
        # 使用文档搜索工具进行在线搜索
        # 注意：实际实现中，应当连接到真实的在线搜索API
        results = doris_docs_search.online_search(query, top_k=top_k)
        
        if not results:
            return f"在线搜索未找到与查询 '{query}' 相关的信息。"
        
        # 格式化搜索结果
        output = f"## 查询 '{query}' 的在线搜索结果\n\n"
        
        for i, doc in enumerate(results):
            similarity = doc.get("similarity", 0) * 100
            output += f"### {i+1}. {doc['title']} (相关度: {similarity:.1f}%)\n\n"
            
            # 截取内容片段，避免过长
            content = doc['content'].strip()
            if len(content) > 500:
                content = content[:500] + "..."
                
            output += f"{content}\n\n"
            # 如果是实际API，可以添加URL链接
            # output += f"[查看完整文档](https://doris.apache.org/docs/{doc['url']})\n\n"
            
        return output
    except Exception as e:
        return f"在线搜索出错: {str(e)}" 