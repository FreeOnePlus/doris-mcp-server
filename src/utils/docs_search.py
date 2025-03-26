import os
import json
import requests
from typing import List, Dict, Any
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class DorisDocsSearch:
    """Apache Doris 文档搜索类"""
    
    def __init__(self, docs_path=None):
        """
        初始化文档搜索
        
        Args:
            docs_path: 文档路径，如果为None则加载默认样本数据
        """
        if docs_path and os.path.exists(docs_path):
            with open(docs_path, 'r', encoding='utf-8') as f:
                self.docs = json.load(f)
        else:
            # 使用默认示例文档
            self.docs = self._load_sample_docs()
        
        # 初始化TF-IDF向量化器
        self.vectorizer = TfidfVectorizer(
            max_features=5000,
            stop_words='english'
        )
        
        # 准备文档内容
        contents = [doc["content"] for doc in self.docs]
        
        # 构建TF-IDF矩阵
        self.tfidf_matrix = self.vectorizer.fit_transform(contents)
    
    def _load_sample_docs(self) -> List[Dict[str, Any]]:
        """加载样本文档数据"""
        return [
            {
                "id": 1,
                "title": "Apache Doris 简介",
                "content": """Apache Doris 是一个现代化的MPP分析型数据库产品。仅需亚秒级响应时间即可获得查询结果，有效支持实时数据分析。
                Apache Doris 可以轻松地与大数据生态系统中的各种工具集成。Apache Doris 的分布式架构非常简洁，易于运维，具有出色的灵活性。"""
            },
            {
                "id": 2,
                "title": "Doris SQL语法 - SELECT",
                "content": """SELECT语句用于从Doris数据库中查询数据。基本语法如下：
                SELECT 
                    [ALL | DISTINCT | DISTINCTROW ] 
                    select_expr [, select_expr ...] 
                FROM table_reference 
                [WHERE where_condition] 
                [GROUP BY {col_name | expr | position} [ASC | DESC], ... [WITH ROLLUP]] 
                [HAVING where_condition] 
                [ORDER BY {col_name | expr | position} [ASC | DESC], ...] 
                [LIMIT {[offset,] row_count | row_count OFFSET offset}]"""
            },
            {
                "id": 3,
                "title": "Doris 数据类型",
                "content": """Apache Doris支持以下数据类型：
                1. 数值类型：TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
                2. 字符串类型：CHAR, VARCHAR, STRING
                3. 日期类型：DATE, DATETIME
                4. 布尔类型：BOOLEAN
                5. 复合类型：ARRAY, MAP, STRUCT"""
            },
            {
                "id": 4,
                "title": "Doris 聚合函数",
                "content": """Doris支持多种聚合函数：
                1. COUNT(): 计算行数
                2. SUM(): 计算总和
                3. AVG(): 计算平均值
                4. MAX(): 找出最大值
                5. MIN(): 找出最小值
                6. STDDEV(): 计算标准差
                7. PERCENTILE(): 计算百分位数
                8. HLL_UNION_AGG(): HyperLogLog聚合"""
            },
            {
                "id": 5,
                "title": "Doris 表引擎类型",
                "content": """Doris支持多种表引擎类型：
                1. OLAP表：Doris的默认表引擎，提供高性能的分析能力
                2. MySQL表：映射到外部MySQL表的外表
                3. ODBC表：通过ODBC连接外部数据源的外表
                4. ELASTICSEARCH表：映射到ElasticSearch索引的外表
                5. HIVE表：映射到Hive表的外表"""
            }
        ]
    
    def search(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        搜索相关文档
        
        Args:
            query: 搜索查询
            top_k: 返回结果数量
            
        Returns:
            List[Dict]: 搜索结果列表
        """
        # 向量化查询
        query_vec = self.vectorizer.transform([query])
        
        # 计算相似度
        similarities = cosine_similarity(query_vec, self.tfidf_matrix).flatten()
        
        # 获取相似度最高的文档索引
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        # 构建结果
        results = []
        for i in top_indices:
            doc = self.docs[i].copy()
            doc["similarity"] = float(similarities[i])
            results.append(doc)
        
        return results
    
    def online_search(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        在线搜索Doris文档
        
        Args:
            query: 搜索查询
            top_k: 返回结果数量
            
        Returns:
            List[Dict]: 搜索结果列表
        """
        # 这里可以实现实际的在线搜索逻辑，例如调用搜索API
        # 目前使用模拟数据作为示例
        try:
            # 调用搜索API的示例代码
            # response = requests.get(
            #     f"https://api.doris.apache.org/search?q={query}&limit={top_k}"
            # )
            # return response.json()
            
            # 使用离线搜索作为替代
            return self.search(query, top_k)
        except Exception as e:
            print(f"在线搜索失败: {e}")
            # 如果在线搜索失败，回退到离线搜索
            return self.search(query, top_k)

# 创建全局搜索实例
doris_docs_search = DorisDocsSearch() 