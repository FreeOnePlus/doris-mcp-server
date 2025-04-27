#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
相似查询工具

负责查找相似的历史查询和示例，计算查询相似度
"""

import os
import json
import logging
import traceback
import re
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Set
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from difflib import SequenceMatcher

# 获取日志记录器
logger = logging.getLogger("doris-mcp.similarity-tools")

# TF-IDF 向量化器（全局变量）
tfidf_vectorizer = None
tfidf_matrix = None
tfidf_examples = None
tfidf_history = None
tfidf_history_matrix = None

# 从环境变量获取数据库名称
DEFAULT_DB = os.getenv("DB_DATABASE", "")

async def find_similar_examples(ctx) -> Dict[str, Any]:
    """
    查找与提供的查询相似的示例
    
    Args:
        ctx: 请求上下文，包含以下参数:
            - query: 用户查询
            - db_name: 数据库名称 (可选，默认使用环境变量)
            - top_k: 返回的相似示例数量 (可选，默认5)
            
    Returns:
        Dict[str, Any]: 包含相似示例的结果
    """
    try:
        query = ctx.params.get("query", "")
        db_name = ctx.params.get("db_name", DEFAULT_DB)
        top_k = ctx.params.get("top_k", 5)
        
        if not query:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少查询参数",
                            "message": "请提供要查找相似示例的查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 加载QA示例
        examples = await _load_qa_examples(db_name)
        if not examples:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "无示例数据",
                            "message": f"无法加载数据库 {db_name} 的QA示例"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 预过滤示例，减少LLM调用
        candidate_examples = await _prefilter_examples(query, examples)
        
        # 如果候选示例数量不足top_k，随机添加一些示例
        if len(candidate_examples) < top_k:
            remaining_examples = [e for e in examples if e not in candidate_examples]
            additional_count = min(top_k - len(candidate_examples), len(remaining_examples))
            if additional_count > 0:
                candidate_examples.extend(random.sample(remaining_examples, additional_count))

        
        # 按相似度排序
        sorted_results = sorted(candidate_examples, key=lambda x: x["similarity"], reverse=True)
        
        # 只返回前k个
        top_results = sorted_results[:top_k]
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "similar_examples": top_results
                    }, ensure_ascii=False)
                }
            ]
        }
        
    except Exception as e:
        logger.error(f"查找相似示例失败: {str(e)}", exc_info=True)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": "查找相似示例失败",
                        "message": str(e)
                    }, ensure_ascii=False)
                }
            ]
        }

async def find_similar_history(ctx) -> Dict[str, Any]:
    """
    查找与提供的查询相似的历史查询
    
    Args:
        ctx: 请求上下文，包含以下参数:
            - query: 用户查询
            - top_k: 返回的相似历史查询数量 (可选，默认5)
            
    Returns:
        Dict[str, Any]: 包含相似历史查询的结果
    """
    try:
        query = ctx.params.get("query", "")
        top_k = ctx.params.get("top_k", 5)
        
        if not query:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少查询参数",
                            "message": "请提供要查找相似历史记录的查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 加载查询历史
        history_records = await _load_query_history()
        if not history_records:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "无历史数据",
                            "message": "无法加载查询历史记录"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 预过滤历史记录，减少LLM调用
        candidate_history = await _prefilter_history(query, history_records)
        
        # 如果候选历史记录不足top_k，随机添加一些记录
        if len(candidate_history) < top_k:
            remaining_history = [h for h in history_records if h not in candidate_history]
            additional_count = min(top_k - len(candidate_history), len(remaining_history))
            if additional_count > 0:
                candidate_history.extend(random.sample(remaining_history, additional_count))
        
        # 使用LLM计算相似度
        similarity_results = await _calculate_history_similarity(query, candidate_history)
        
        # 按相似度排序
        sorted_results = sorted(similarity_results, key=lambda x: x["similarity"], reverse=True)
        
        # 只返回前k个
        top_results = sorted_results[:top_k]
        
        # 确保SQL中包含LIMIT子句
        for result in top_results:
            if "sql" in result and result["sql"]:
                result["sql"] = _ensure_limit_in_sql(result["sql"])
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "similar_history": top_results
                    }, ensure_ascii=False)
                }
            ]
        }
        
    except Exception as e:
        logger.error(f"查找相似历史记录失败: {str(e)}", exc_info=True)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": "查找相似历史记录失败",
                        "message": str(e)
                    }, ensure_ascii=False)
                }
            ]
        }

async def calculate_query_similarity(ctx) -> Dict[str, Any]:
    """
    计算两个查询之间的相似度
    
    Args:
        ctx: 请求上下文，包含以下参数:
            - query1: 第一个查询
            - query2: 第二个查询
            
    Returns:
        Dict[str, Any]: 包含相似度分数的结果
    """
    try:
        query1 = ctx.params.get("query1", "")
        query2 = ctx.params.get("query2", "")
        
        if not query1 or not query2:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "缺少查询参数",
                            "message": "请提供两个要比较的查询"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # 计算文本相似度
        text_similarity = await _calculate_text_similarity(query1, query2)
        
        # 使用LLM计算相似度
        llm_similarity = await _calculate_similarity_with_llm(query1, [{"question": query2, "answer": ""}])
        
        similarity_score = llm_similarity[0]["similarity"] if llm_similarity else text_similarity
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "similarity": similarity_score,
                        "text_similarity": text_similarity,
                        "semantic_similarity": llm_similarity[0]["similarity"] if llm_similarity else 0.0
                    }, ensure_ascii=False)
                }
            ]
        }
        
    except Exception as e:
        logger.error(f"计算查询相似度失败: {str(e)}", exc_info=True)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": False,
                        "error": "计算查询相似度失败",
                        "message": str(e)
                    }, ensure_ascii=False)
                }
            ]
        }

# 辅助函数
async def _load_qa_examples(db_name: str) -> List[Dict[str, Any]]:
    """
    加载问答示例
    
    Args:
        db_name: 数据库名称
        
    Returns:
        List[Dict[str, Any]]: 问答示例列表
    """
    try:
        from pathlib import Path
        import json
        
        # 首先尝试从数据库特定文件加载
        base_path = Path(os.getenv("DATA_DIR", "./data"))
        db_examples_path = base_path / f"examples_{db_name.lower()}.json"
        
        if db_examples_path.exists():
            with open(db_examples_path, "r", encoding="utf-8") as f:
                return json.load(f)
        
        # 如果没有数据库特定文件，尝试加载默认示例
        default_examples_path = base_path / "examples.json"
        if default_examples_path.exists():
            with open(default_examples_path, "r", encoding="utf-8") as f:
                return json.load(f)
        
        # 如果都没有，返回空列表
        logger.warning(f"未找到问答示例文件: {db_examples_path} 或 {default_examples_path}")
        return []
        
    except Exception as e:
        logger.error(f"加载问答示例失败: {str(e)}", exc_info=True)
        return []

async def _load_query_history() -> List[Dict[str, Any]]:
    """
    加载查询历史记录
    
    Returns:
        List[Dict[str, Any]]: 查询历史记录列表
    """
    try:
        from pathlib import Path
        import json
        
        # 尝试从文件加载历史记录
        history_path = Path(os.getenv("DATA_DIR", "./data")) / "query_history.json"
        
        if history_path.exists():
            with open(history_path, "r", encoding="utf-8") as f:
                return json.load(f)
        
        # 如果文件不存在，返回空列表
        logger.warning(f"未找到查询历史记录文件: {history_path}")
        return []
        
    except Exception as e:
        logger.error(f"加载查询历史记录失败: {str(e)}", exc_info=True)
        return []

async def _prefilter_examples(query: str, examples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    预过滤示例，通过关键词匹配找出候选
    
    Args:
        query: 用户查询
        examples: 完整示例列表
        
    Returns:
        List[Dict[str, Any]]: 候选示例列表
    """
    try:
        # 从查询中提取关键词
        keywords = _extract_keywords(query)
        if not keywords:
            # 如果没有提取到关键词，返回原始列表的前20条
            return examples[:20]
        
        # 计算每个示例与查询的关键词匹配度
        scored_examples = []
        for example in examples:
            example_keywords = _extract_keywords(example.get("question", ""))
            # 计算Jaccard相似度
            similarity = _calculate_jaccard_similarity(keywords, example_keywords)
            scored_examples.append((example, similarity))
        
        # 按相似度排序
        scored_examples.sort(key=lambda x: x[1], reverse=True)
        
        # 返回前20个最相似的示例
        return [ex[0] for ex in scored_examples[:20]]
        
    except Exception as e:
        logger.error(f"预过滤示例失败: {str(e)}", exc_info=True)
        # 发生错误时返回原始示例的前10条
        return examples[:10]

async def _prefilter_history(query: str, history_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    预过滤历史记录，通过关键词匹配找出候选
    
    Args:
        query: 用户查询
        history_records: 完整历史记录列表
        
    Returns:
        List[Dict[str, Any]]: 候选历史记录列表
    """
    try:
        # 从查询中提取关键词
        keywords = _extract_keywords(query)
        if not keywords:
            # 如果没有提取到关键词，返回原始列表的前20条
            return history_records[:20]
        
        # 计算每条历史记录与查询的关键词匹配度
        scored_history = []
        for record in history_records:
            record_keywords = _extract_keywords(record.get("query", ""))
            # 计算Jaccard相似度
            similarity = _calculate_jaccard_similarity(keywords, record_keywords)
            scored_history.append((record, similarity))
        
        # 按相似度排序
        scored_history.sort(key=lambda x: x[1], reverse=True)
        
        # 返回前20个最相似的历史记录
        return [rec[0] for rec in scored_history[:20]]
        
    except Exception as e:
        logger.error(f"预过滤历史记录失败: {str(e)}", exc_info=True)
        # 发生错误时返回原始历史记录的前10条
        return history_records[:10]

async def _calculate_history_similarity(query: str, candidate_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    计算查询与候选历史记录的相似度
    
    Args:
        query: 用户查询
        candidate_history: 候选历史记录列表
        
    Returns:
        List[Dict[str, Any]]: 包含相似度分数的结果列表
    """
    try:
        
        # 如果没有候选历史记录，返回空列表
        if not candidate_history:
            return []
        return results
        
    except Exception as e:
        logger.error(f"计算历史记录相似度失败: {str(e)}", exc_info=True)
        
        # 发生错误时使用文本相似度作为备用方法
        results = []
        for record in candidate_history:
            history_query = record.get("query", "")
            similarity = await _calculate_text_similarity(query, history_query)
            
            # 构建结果对象
            result = dict(record)
            result["similarity"] = similarity
            
            results.append(result)
        
        return results

async def _calculate_text_similarity(text1: str, text2: str) -> float:
    """
    计算两个文本之间的相似度
    
    Args:
        text1: 第一个文本
        text2: 第二个文本
        
    Returns:
        float: 相似度分数 (0.0-1.0)
    """
    try:
        # 使用Jaccard相似度
        t1_keywords = _extract_keywords(text1)
        t2_keywords = _extract_keywords(text2)
        jaccard_similarity = _calculate_jaccard_similarity(t1_keywords, t2_keywords)
        
        # 使用序列匹配相似度
        sequence_similarity = SequenceMatcher(None, text1, text2).ratio()
        
        # 综合两种相似度计算方法
        # Jaccard侧重于关键词匹配，序列匹配侧重于文本结构
        combined_similarity = (jaccard_similarity * 0.7) + (sequence_similarity * 0.3)
        
        return min(1.0, max(0.0, combined_similarity))
        
    except Exception as e:
        logger.error(f"计算文本相似度失败: {str(e)}", exc_info=True)
        # 发生错误时返回0
        return 0.0

async def _extract_similarity_values(response_text: str, expected_count: int) -> List[float]:
    """
    从LLM响应中提取相似度值
    
    Args:
        response_text: LLM响应文本
        expected_count: 预期的相似度值数量
        
    Returns:
        List[float]: 相似度值列表
    """
    try:
        # 尝试从JSON响应中提取
        json_pattern = r'\{[\s\S]*\}'
        json_match = re.search(json_pattern, response_text)
        
        if json_match:
            try:
                json_data = json.loads(json_match.group(0))
                if "similarities" in json_data and isinstance(json_data["similarities"], list):
                    # 将所有相似度转换为浮点数
                    return [float(sim) for sim in json_data["similarities"]]
            except Exception as json_error:
                logger.warning(f"无法解析JSON相似度: {str(json_error)}")
        
        # 如果JSON解析失败，使用正则表达式提取
        # 查找形如 "1: 0.85" 或 "示例1: 0.85" 的模式
        similarity_pattern = r'(?:示例|历史记录)?\s*(\d+)\s*[:：]\s*(\d+\.\d+|\d+)'
        similarity_matches = re.findall(similarity_pattern, response_text)
        
        if similarity_matches:
            # 解析匹配结果
            parsed_similarities = {}
            for idx_str, sim_str in similarity_matches:
                idx = int(idx_str) - 1  # 将1-based索引转换为0-based
                sim = float(sim_str)
                parsed_similarities[idx] = sim
            
            # 构建结果列表
            result = [0.0] * expected_count
            for idx, sim in parsed_similarities.items():
                if 0 <= idx < expected_count:
                    result[idx] = sim
            
            return result
        
        # 如果以上方法都失败，尝试直接提取浮点数
        float_pattern = r'(\d+\.\d+)'
        float_matches = re.findall(float_pattern, response_text)
        
        if float_matches and len(float_matches) >= expected_count:
            return [float(m) for m in float_matches[:expected_count]]
        
        # 如果所有方法都失败，返回默认值
        return [0.5] * expected_count
        
    except Exception as e:
        logger.error(f"提取相似度值失败: {str(e)}", exc_info=True)
        # 发生错误时返回默认值
        return [0.5] * expected_count

def _extract_keywords(text: str) -> Set[str]:
    """
    从文本中提取关键词
    
    Args:
        text: 文本
        
    Returns:
        Set[str]: 关键词集合
    """
    # 使用jieba分词（如果可用）
    try:
        import jieba
        
        # 分词并过滤停用词和短词
        words = jieba.cut(text)
        keywords = set()
        
        for word in words:
            word = word.strip()
            if len(word) >= 2 and not _is_stopword(word):
                keywords.add(word.lower())
        
        return keywords
        
    except ImportError:
        # 如果jieba不可用，使用简单的空格分割
        words = re.findall(r'\b\w+\b', text.lower())
        return {word for word in words if len(word) >= 3 and not _is_stopword(word)}

def _is_stopword(word: str) -> bool:
    """
    检查词是否为停用词
    
    Args:
        word: 词
        
    Returns:
        bool: 是否为停用词
    """
    stopwords = {
        "的", "了", "和", "是", "在", "我", "有", "个", "这", "那", "你", "们", "要", "就", "会", "对", "能", "下",
        "子", "看", "用", "去", "把", "来", "与", "说", "做", "好", "得", "吗", "嗯", "吧", "啊", "呢", "哦", "哪",
        "the", "and", "is", "in", "to", "of", "a", "for", "that", "with", "on", "at", "as", "by", "from",
        "an", "be", "this", "are", "or", "it", "was", "i", "you", "he", "she", "we", "they", "what", "how",
        "when", "where", "who", "why", "all", "any", "can", "will", "should", "would", "could", "may", "might"
    }
    return word.lower() in stopwords

def _calculate_jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """
    计算两个集合的Jaccard相似度
    
    Args:
        set1: 第一个集合
        set2: 第二个集合
        
    Returns:
        float: Jaccard相似度 (0.0-1.0)
    """
    if not set1 or not set2:
        return 0.0
    
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    
    if union == 0:
        return 0.0
    
    return intersection / union

def _ensure_limit_in_sql(sql: str, default_limit: int = 200) -> str:
    """
    确保SQL查询包含LIMIT子句
    
    Args:
        sql: SQL查询
        default_limit: 默认限制行数
        
    Returns:
        str: 包含LIMIT子句的SQL查询
    """
    # 如果SQL为空，直接返回
    if not sql:
        return sql
    
    # 检查SQL是否已经包含LIMIT子句
    limit_pattern = r'\bLIMIT\s+\d+'
    if re.search(limit_pattern, sql, re.IGNORECASE):
        return sql
    
    # 添加LIMIT子句
    sql = sql.rstrip(';')
    return f"{sql} LIMIT {default_limit};" 