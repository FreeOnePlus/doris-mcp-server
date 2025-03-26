import os
import json
from typing import List, Dict, Any
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class SQLSampleManager:
    """SQL样本管理类，用于存储和检索SQL样本"""
    
    def __init__(self, samples_path=None):
        """
        初始化SQL样本管理器
        
        Args:
            samples_path: 样本文件路径，如果为None则加载默认样本
        """
        if samples_path and os.path.exists(samples_path):
            with open(samples_path, 'r', encoding='utf-8') as f:
                self.samples = json.load(f)
        else:
            # 使用默认示例样本
            self.samples = self._load_default_samples()
        
        # 初始化TF-IDF向量化器
        self.vectorizer = TfidfVectorizer(
            max_features=5000,
            stop_words='english'
        )
        
        # 准备自然语言查询和描述
        texts = []
        for sample in self.samples:
            # 组合自然语言查询和描述以提高匹配精度
            text = f"{sample['nl_query']} {sample['description']}"
            texts.append(text)
        
        # 构建TF-IDF矩阵
        self.tfidf_matrix = self.vectorizer.fit_transform(texts)
    
    def _load_default_samples(self) -> List[Dict[str, Any]]:
        """加载默认SQL样本"""
        return [
            {
                "id": 1,
                "description": "统计特定时间段内每日订单量",
                "nl_query": "统计2023年1月每天的订单数量",
                "sql": """SELECT 
                    DATE_FORMAT(create_time, '%Y-%m-%d') as order_date,
                    COUNT(*) as order_count
                FROM orders
                WHERE create_time BETWEEN '2023-01-01' AND '2023-01-31'
                GROUP BY order_date
                ORDER BY order_date""",
                "explanation": "这个查询使用DATE_FORMAT函数将订单创建时间格式化为年-月-日格式，然后按天分组统计订单数量。"
            },
            {
                "id": 2,
                "description": "计算产品销售额排名",
                "nl_query": "查询销售额前10的产品及其销售数量",
                "sql": """SELECT 
                    p.product_name,
                    SUM(oi.price * oi.quantity) as total_sales,
                    SUM(oi.quantity) as total_quantity
                FROM order_items oi
                JOIN products p ON oi.product_id = p.id
                GROUP BY p.product_name
                ORDER BY total_sales DESC
                LIMIT 10""",
                "explanation": "这个查询计算每个产品的总销售额和总销售数量，然后按销售额降序排序并限制返回前10条结果。"
            },
            {
                "id": 3,
                "description": "用户活跃度分析",
                "nl_query": "分析过去30天内每个用户的活跃度",
                "sql": """SELECT 
                    user_id,
                    COUNT(DISTINCT DATE_FORMAT(activity_time, '%Y-%m-%d')) as active_days,
                    COUNT(*) as total_activities
                FROM user_activities
                WHERE activity_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                GROUP BY user_id
                ORDER BY active_days DESC, total_activities DESC""",
                "explanation": "这个查询统计了每个用户在过去30天内的活跃天数和总活动次数，可以用于分析用户活跃度。"
            },
            {
                "id": 4,
                "description": "区域销售同比增长",
                "nl_query": "计算各区域今年与去年同期的销售额对比和增长率",
                "sql": """SELECT 
                    r.region_name,
                    SUM(CASE WHEN YEAR(o.order_date) = YEAR(CURRENT_DATE()) THEN oi.amount ELSE 0 END) as current_year_sales,
                    SUM(CASE WHEN YEAR(o.order_date) = YEAR(CURRENT_DATE()) - 1 THEN oi.amount ELSE 0 END) as previous_year_sales,
                    (SUM(CASE WHEN YEAR(o.order_date) = YEAR(CURRENT_DATE()) THEN oi.amount ELSE 0 END) - 
                     SUM(CASE WHEN YEAR(o.order_date) = YEAR(CURRENT_DATE()) - 1 THEN oi.amount ELSE 0 END)) / 
                    NULLIF(SUM(CASE WHEN YEAR(o.order_date) = YEAR(CURRENT_DATE()) - 1 THEN oi.amount ELSE 0 END), 0) * 100 as growth_rate
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN stores s ON o.store_id = s.store_id
                JOIN regions r ON s.region_id = r.region_id
                WHERE YEAR(o.order_date) IN (YEAR(CURRENT_DATE()), YEAR(CURRENT_DATE()) - 1)
                    AND MONTH(o.order_date) = MONTH(CURRENT_DATE())
                GROUP BY r.region_name
                ORDER BY growth_rate DESC""",
                "explanation": "这个复杂查询计算了每个区域在当前年份和上一年同期的销售额，以及同比增长率。使用CASE语句分别计算不同年份的销售额。"
            },
            {
                "id": 5,
                "description": "客户购买频率分析",
                "nl_query": "分析每个客户的购买频率和平均订单金额",
                "sql": """SELECT 
                    c.customer_id,
                    c.customer_name,
                    COUNT(o.order_id) as order_count,
                    DATEDIFF(MAX(o.order_date), MIN(o.order_date)) / COUNT(o.order_id) as avg_days_between_orders,
                    AVG(o.total_amount) as avg_order_amount
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
                GROUP BY c.customer_id, c.customer_name
                HAVING COUNT(o.order_id) > 1
                ORDER BY avg_days_between_orders""",
                "explanation": "这个查询分析了每个客户在过去一年内的订单数量、平均订单间隔天数和平均订单金额，可用于客户分层分析。"
            }
        ]
    
    def find_similar_samples(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        查找与自然语言查询相似的SQL样本
        
        Args:
            query: 自然语言查询
            top_k: 返回结果数量
            
        Returns:
            List[Dict]: 相似SQL样本列表
        """
        # 向量化查询
        query_vec = self.vectorizer.transform([query])
        
        # 计算相似度
        similarities = cosine_similarity(query_vec, self.tfidf_matrix).flatten()
        
        # 获取相似度最高的样本索引
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        # 构建结果
        results = []
        for i in top_indices:
            sample = self.samples[i].copy()
            sample["similarity"] = float(similarities[i])
            results.append(sample)
        
        return results
    
    def get_sample_by_id(self, sample_id: int) -> Dict:
        """
        通过ID获取SQL样本
        
        Args:
            sample_id: 样本ID
            
        Returns:
            Dict: SQL样本
        """
        for sample in self.samples:
            if sample["id"] == sample_id:
                return sample
        return None

# 创建全局SQL样本管理器实例
sql_sample_manager = SQLSampleManager() 