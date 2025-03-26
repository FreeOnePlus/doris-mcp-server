#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
文档资源处理函数
"""

import os
import logging
import markdown

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 文档根目录
DOCS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../docs'))

def get_docs(topic: str) -> str:
    """
    获取指定主题的文档
    
    Args:
        topic: 文档主题，例如 "usage/api" 表示API使用文档
    
    Returns:
        str: 文档内容（HTML格式）
    """
    try:
        # 构建文档文件路径
        doc_path = os.path.join(DOCS_ROOT, f"{topic}.md")
        
        # 检查文件是否存在
        if not os.path.exists(doc_path):
            logger.warning(f"文档不存在: {topic}")
            return f"<h1>404 - 文档不存在</h1><p>找不到主题为 '{topic}' 的文档</p>"
        
        # 读取文档文件
        with open(doc_path, 'r', encoding='utf-8') as f:
            md_content = f.read()
        
        # 将Markdown转换为HTML
        html_content = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])
        
        logger.info(f"成功获取文档: {topic}")
        return html_content
    except Exception as e:
        logger.error(f"获取文档时出错: {str(e)}")
        return f"<h1>Error</h1><p>获取文档时出错: {str(e)}</p>" 