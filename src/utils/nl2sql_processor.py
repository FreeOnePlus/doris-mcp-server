"""
Apache Doris NL2SQL处理器

实现自然语言到SQL的转换逻辑，包括：
1. 自然语言拆解和关键词匹配
2. 业务问题判断
3. SQL生成
4. SQL执行和错误修正循环
"""

import os
import re
import json
import logging
import time
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union
from dotenv import load_dotenv
from datetime import datetime
import uuid
import pathlib
import traceback
import hashlib

# 获取项目根目录
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

# 添加项目根目录到路径
import sys
sys.path.insert(0, PROJECT_ROOT)

# 导入相关模块
from src.utils.db import execute_query, execute_query_df, get_db_connection, get_db_name
from src.utils.llm_client import get_llm_client, Message
from src.utils.metadata_extractor import MetadataExtractor
from src.prompts.prompts import NL2SQL_PROMPTS, SQL_FIX_PROMPTS, BUSINESS_REASONING_PROMPTS

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 日志保存目录，默认为项目根目录下的log/queries目录
LOG_BASE_DIR = os.path.join(PROJECT_ROOT, "log")
QUERY_LOG_DIR = os.getenv("QUERY_LOG_DIR", os.path.join(LOG_BASE_DIR, "queries"))

def log_query_process(log_data: Dict[str, Any], log_type: str = "query"):
    """
    持久化记录查询过程和结果
    
    Args:
        log_data: 需要记录的数据
        log_type: 日志类型，默认为"query"
    """
    try:
        # 创建日志目录
        log_dir = pathlib.Path(QUERY_LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成日志文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_id = str(uuid.uuid4())[:8]
        filename = f"{timestamp}_{log_type}_{log_id}.json"
        log_path = log_dir / filename
        
        # 添加时间戳
        log_data["timestamp"] = datetime.now().isoformat()
        log_data["log_id"] = log_id
        
        # 处理特殊字符，确保安全保存到JSON
        safe_log_data = log_data.copy()
        for key, value in log_data.items():
            if isinstance(value, str):
                # 对字符串类型的字段进行特殊处理，尤其是可能包含标签和特殊字符的字段
                if "<" in value or ">" in value:
                    # 转义特殊字符，保留完整内容
                    safe_content = value.replace("\\", "\\\\").replace("\"", "\\\"")
                    # 对于标签类内容，进行转义，避免被截断
                    safe_content = safe_content.replace("<", "\\<").replace(">", "\\>")
                    safe_log_data[key] = safe_content
        
        # 写入日志文件
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                json.dump(safe_log_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"查询日志已保存: {log_path}")
            return str(log_path)
        except Exception as json_err:
            logger.error(f"保存JSON日志文件时出错: {str(json_err)}，尝试文本备份")
            
            # 如果JSON保存失败，尝试以文本格式保存
            text_log_path = str(log_path).replace(".json", ".txt")
            with open(text_log_path, 'w', encoding='utf-8') as f:
                f.write(f"时间戳: {datetime.now().isoformat()}\n")
                f.write(f"日志ID: {log_id}\n")
                f.write(f"日志类型: {log_type}\n")
                f.write("==================\n")
                for key, value in log_data.items():
                    f.write(f"{key}: {value}\n")
                    f.write("------------------\n")
            
            logger.info(f"查询日志已保存为文本文件: {text_log_path}")
            return text_log_path
    except Exception as e:
        logger.error(f"保存查询日志时出错: {str(e)}")
        return None

class NL2SQLProcessor:
    """
    NL2SQL处理器
    
    负责将自然语言转换为SQL查询，并处理执行和错误修正
    """
    
    def __init__(self, qa_examples_path: Optional[str] = None):
        """
        初始化

        Args:
            qa_examples_path: QA示例路径，默认使用项目内置示例
        """
        # 初始化LLM客户端
        try:
            # 不再初始化单一的llm_client，而是在每个阶段按需创建
            logger.info("LLM客户端初始化成功")
            
            # 配置热身和缓存
            self.cache_dir = os.path.join(PROJECT_ROOT, "cache", "nl2sql")
            self.cache_ttl = int(os.getenv("CACHE_TTL", "86400"))  # 默认缓存24小时
            os.makedirs(self.cache_dir, exist_ok=True)
            
            # 初始化查询缓存
            self.query_cache = {}
            
            # 设置SQL执行重试次数
            self.max_retries = int(os.getenv("SQL_MAX_RETRIES", "3"))
            
            # 模型参数配置
            self.model_config = {
                "temperature": float(os.getenv("LLM_TEMPERATURE", "0.7")),
                "top_p": float(os.getenv("LLM_TOP_P", "0.95")),
                "max_tokens": int(os.getenv("LLM_MAX_TOKENS", "2048"))
            }
        except Exception as e:
            logger.warning(f"LLM客户端初始化失败: {str(e)}，将使用关键词匹配作为备选")
            # 确保query_cache和max_retries即使在异常时也被初始化
            self.query_cache = {}
            self.max_retries = 3
        
        # 加载QA示例
        try:
            if not qa_examples_path:
                qa_examples_path = os.path.join(PROJECT_ROOT, "data", "qa_examples.json")
                
            if os.path.exists(qa_examples_path):
                with open(qa_examples_path, "r", encoding="utf-8") as f:
                    self.qa_examples = json.load(f)
                logger.info(f"已加载 {len(self.qa_examples)} 个问答示例")
            else:
                logger.warning(f"问答示例文件不存在: {qa_examples_path}")
                self.qa_examples = []
        except Exception as e:
            logger.error(f"加载问答示例出错: {str(e)}")
            self.qa_examples = []
            
        # 相似示例阈值
        self.similar_examples_threshold = float(os.getenv("SIMILAR_EXAMPLES_THRESHOLD", "0.65"))
        
        # 数据库连接
        self.db_name = os.getenv("DB_NAME", "ssb")
        
        # 获取业务元数据提取器
        try:
            from src.utils.metadata_extractor import MetadataExtractor
            self.metadata_extractor = MetadataExtractor()
            
            # 在初始化阶段检查并保存内置关键词
            self._ensure_built_in_keywords_saved()
        except Exception as e:
            logger.error(f"创建元数据提取器出错: {str(e)}")
            self.metadata_extractor = None
            
    def _ensure_built_in_keywords_saved(self):
        """
        确保内置关键词已保存到数据库中
        只在初始化时执行一次，避免每次查询都重复保存
        """
        if not self.metadata_extractor:
            return
            
        try:
            # 检查数据库中是否已有内置关键词
            db_keywords = self.metadata_extractor.get_business_keywords_from_database(self.db_name)
            
            # 获取内置关键词
            strong_business_keywords = self._get_strong_business_keywords()
            auxiliary_keywords = self._get_auxiliary_keywords()
            
            # 如果关键词数量不足，或者数据库中没有足够的内置关键词，执行保存操作
            # 这里使用简单的数量比较作为判断标准，可以根据需要改进判断逻辑
            if len(db_keywords) < (len(strong_business_keywords) + len(auxiliary_keywords)) * 0.9:
                logger.info("数据库中内置关键词不完整，执行保存操作")
                
                # 准备关键词数据
                keywords_to_save = []
                for keyword in strong_business_keywords:
                    keywords_to_save.append({
                        'keyword': keyword,
                        'confidence': 0.9,  # 强业务关键词给予更高置信度
                        'category': '强业务关键词',
                        'source': '系统默认'
                    })
                
                for keyword in auxiliary_keywords:
                    keywords_to_save.append({
                        'keyword': keyword,
                        'confidence': 0.5,  # 辅助关键词给予较低置信度
                        'category': '辅助关键词',
                        'source': '系统默认'
                    })
                    
                # 保存到数据库
                self.metadata_extractor.save_business_keywords(self.db_name, keywords_to_save)
                logger.info(f"已将内置关键词保存到数据库，强业务关键词: {len(strong_business_keywords)}个，辅助关键词: {len(auxiliary_keywords)}个")
            else:
                logger.info("数据库中已存在足够的内置关键词，跳过保存步骤")
        except Exception as e:
            logger.error(f"检查或保存内置关键词到数据库时出错: {str(e)}")
    
    def process(self, query: str) -> Dict[str, Any]:
        """
        处理自然语言查询
        
        Args:
            query: 自然语言查询
        
        Returns:
            Dict[str, Any]: 包含SQL、执行结果和元数据的字典
        """
        # 初始化日志数据
        log_data = {
            "query": query,
            "steps": []
        }
        
        # 检查缓存
        if query in self.query_cache:
            cache_entry = self.query_cache[query]
            cache_age = time.time() - cache_entry.get('timestamp', 0)
            if cache_age < self.cache_ttl:
                log_data["from_cache"] = True
                log_data["cache_age_seconds"] = cache_age
                log_query_process(log_data, "query_cache_hit")
                return cache_entry['result']
        
        try:
            # 步骤1：判断是否是业务查询
            log_data["steps"].append({"step": "check_business_query", "time": datetime.now().isoformat()})
            is_business_query, confidence = self._check_if_business_query(query)
            log_data["is_business_query"] = is_business_query
            log_data["business_confidence"] = confidence
            
            if not is_business_query:
                result = {
                    'success': False,
                    'message': '这不是一个业务查询问题。请提出与数据库业务相关的问题，例如数据分析、统计或报表查询。',
                    'query': query,
                    'confidence': confidence
                }
                log_data["result"] = result
                log_query_process(log_data, "non_business_query")
                # 直接返回错误信息，不继续执行后续步骤
                return result
            
            # 步骤2：查找相似示例
            log_data["steps"].append({"step": "find_similar_example", "time": datetime.now().isoformat()})
            similar_example = self._find_similar_example(query)
            if similar_example:
                log_data["similar_example"] = {
                    "question": similar_example.get("question", ""),
                    "sql": similar_example.get("sql", "")
                }
            
            # 步骤3：获取业务元数据
            log_data["steps"].append({"step": "get_business_metadata", "time": datetime.now().isoformat()})
            business_metadata = self.metadata_extractor.summarize_business_metadata(self.db_name)
            
            # 步骤4：生成SQL
            log_data["steps"].append({"step": "generate_sql", "time": datetime.now().isoformat()})
            sql_result = self._generate_sql(
                query, 
                similar_example=similar_example, 
                business_metadata=business_metadata
            )
            
            # 记录SQL生成结果
            log_data["sql_generation"] = {
                "success": sql_result.get("success", False),
                "sql": sql_result.get("sql", ""),
                "explanation": sql_result.get("explanation", "")
            }
            
            # 步骤5：执行SQL并处理错误
            if sql_result['success']:
                log_data["steps"].append({"step": "execute_sql", "time": datetime.now().isoformat()})
                execution_result = self._execute_sql_with_retry(sql_result['sql'], query)
                
                # 记录执行结果
                log_data["sql_execution"] = {
                    "success": execution_result.get("success", False),
                    "row_count": execution_result.get("row_count", 0),
                    "execution_time": execution_result.get("execution_time", 0),
                    "retries": execution_result.get("retries", 0),
                    "errors": execution_result.get("errors", [])
                }
                
                # 合并结果
                result = {**sql_result, **execution_result}
                
                # 更新缓存
                self.query_cache[query] = {
                    'result': result,
                    'timestamp': time.time()
                }
                
                # 记录完整日志
                log_data["result"] = result
                log_data["success"] = result.get("success", False)
                log_query_process(log_data, "query_complete")
                
                return result
            else:
                # 记录SQL生成失败日志
                log_data["result"] = sql_result
                log_data["success"] = False
                log_query_process(log_data, "sql_generation_failed")
                return sql_result
        except Exception as e:
            error_message = str(e)
            logger.error(f"处理查询时出错: {error_message}")
            
            # 记录错误日志
            log_data["error"] = error_message
            log_data["error_type"] = type(e).__name__
            log_data["success"] = False
            log_query_process(log_data, "query_error")
            
            return {
                'success': False,
                'message': f'处理查询时出错: {error_message}',
                'query': query
            }
    
    def _check_if_business_query(self, query: str) -> Tuple[bool, float]:
        """
        判断是否是业务查询
        
        Args:
            query: 自然语言查询
            
        Returns:
            Tuple[bool, float]: 是否是业务查询及置信度
        """
        # 1. 首先尝试从数据库获取业务关键词并进行匹配
        try:
            # 获取数据库中存储的业务关键词及其置信度
            db_keywords = self.metadata_extractor.get_business_keywords_from_database(self.db_name)

            # 如果数据库中已有关键词，则进行匹配
            if db_keywords:
                for keyword, confidence in db_keywords.items():
                    # 跳过辅助关键词（时间维度、分析维度等非直接业务相关词）
                    if len(keyword) <= 2 or keyword in self._get_auxiliary_keywords():
                        continue
                        
                    if keyword in query:
                        logger.info(f"通过数据库业务关键词'{keyword}'判断查询'{query}'为业务查询，置信度: {confidence}")
                        return True, confidence
                
                logger.info("数据库中的业务关键词匹配失败，使用内置关键词继续匹配")
            else:
                logger.info("数据库中未找到业务关键词，使用内置关键词进行匹配")
        except Exception as e:
            logger.error(f"从数据库获取业务关键词出错: {str(e)}，使用内置关键词进行匹配")

        # 2. 如果数据库匹配失败，使用内置的业务关键词直接进行匹配（不再尝试保存到数据库）
        # 将关键词分为强业务关键词和辅助关键词
        strong_business_keywords = self._get_strong_business_keywords()
        auxiliary_keywords = self._get_auxiliary_keywords()
            
        # 检查查询中是否包含强业务关键词
        for keyword in strong_business_keywords:
            if keyword in query:
                logger.info(f"通过强业务关键词'{keyword}'判断查询'{query}'为业务查询")
                return True, 0.9  # 强业务关键词匹配给予更高的置信度
        
        # 3. 如果强业务关键词没匹配到，再通过表名和列名等元数据提取的关键词匹配
        business_keywords = self._extract_business_keywords()
        
        # 尝试将元数据关键词保存到数据库
        try:
            keywords_to_save = []
            for keyword in business_keywords:
                if keyword and isinstance(keyword, str) and len(keyword) > 2 and keyword not in self._get_auxiliary_keywords():
                    keywords_to_save.append({
                        'keyword': keyword,
                        'confidence': 0.8,
                        'category': '元数据关键词',
                        'source': '数据库元数据'
                    })
            if keywords_to_save:
                self.metadata_extractor.save_business_keywords(self.db_name, keywords_to_save)
                logger.info(f"已将{len(keywords_to_save)}个元数据关键词保存到数据库")
        except Exception as e:
            logger.error(f"保存元数据关键词到数据库出错: {str(e)}")
            
        for keyword in business_keywords:
            # 只匹配长度大于2的关键词，且不在辅助关键词列表中
            if len(keyword) > 2 and keyword not in self._get_auxiliary_keywords() and keyword in query:
                logger.info(f"通过元数据关键词'{keyword}'判断查询'{query}'为业务查询")
                return True, 0.8  # 元数据关键词匹配给予较高置信度
        
        # 4. 如果同时出现多个辅助关键词，也可能是业务查询
        auxiliary_matches = [kw for kw in auxiliary_keywords if kw in query]
        if len(auxiliary_matches) >= 2:
            logger.info(f"通过多个辅助关键词{auxiliary_matches}组合判断查询'{query}'为业务查询")
            return True, 0.7  # 多个辅助关键词组合给予中等置信度
                
        # 5. 如果关键词都没有匹配成功，再尝试使用LLM判断（慢速路径）
        logger.info(f"本地关键词匹配未成功，尝试使用LLM判断查询: '{query}'")
        try:
            llm_result = self._check_business_query_with_llm(query)
            llm_is_business = llm_result.get("is_business_query", False) 
            llm_confidence = llm_result.get("confidence", 0.5)
            
            logger.info(f"LLM判断结果: {query} -> is_business_query={llm_is_business}, confidence={llm_confidence}")
            return llm_is_business, llm_confidence
        except Exception as e:
            logger.error(f"LLM判断业务查询出错: {str(e)}")
            # 如果LLM调用失败，使用保守策略，假设是业务查询并赋予较低置信度
            return True, 0.51
    
    def _get_strong_business_keywords(self) -> List[str]:
        """
        获取强业务关键词列表（直接与业务相关的词）
        
        Returns:
            List[str]: 强业务关键词列表
        """
        return [
            # 销售相关
            "销量", "销售额", "销售量", "营收", "营业额", "交易量", "交易额", "成交量", "订单量", "订单数",
            # 财务相关
            "利润", "利润率", "毛利", "毛利率", "净利", "净利润", "收入", "成本", "费用", "支出", "预算", "投资回报率", "ROI",
            # 库存相关
            "库存", "库存周转", "库存量", "周转率", "存货", "产量",
            # 客户相关
            "客户", "客户数", "用户", "会员", "消费者", "买家", "潜在客户", "新客户", "老客户", "流失", "留存",
            # 市场相关
            "市场份额", "市占率", "竞争力", "品牌", "市场定位", "市场规模", "品牌影响力", "曝光度", "渗透率",
            # 效率相关
            "转化率", "点击率", "购买率", "参与度", "留存率", "流失率", "回购率", "满意度", "复购率", "活跃度", "活跃率",
            # 增长相关
            "增长率", "同比", "环比", "增长", "下降", "上升", "提高", "降低", "波动",
            # 营销相关
            "营销", "促销", "广告", "投放", "宣传", "推广", "引流", "获客成本", "转化效果",
            # 供应链相关
            "供应链", "供应商", "采购", "物流", "配送", "运输", "交货", "原材料", "产能",
            # 生产相关
            "生产", "制造", "加工", "质量", "良品率", "不良率", "生产效率", "产能", "产量", "输出",
            # 人力相关
            "人效", "绩效", "薪资", "薪酬", "培训", "考核",
            # 具体业务类型
            "零售", "批发", "电商", "制造", "服务", "金融", "保险", "银行", "教育", "医疗", "餐饮", "旅游", "房地产",
            # 具体指标
            "GMV", "PV", "UV", "DAU", "MAU", "ARPU", "CPC", "CPM", "CPA", "KPI", "OKR"
        ]
    
    def _get_auxiliary_keywords(self) -> List[str]:
        """
        获取辅助关键词列表（非直接业务相关，但与分析相关的词）
        
        Returns:
            List[str]: 辅助关键词列表
        """
        return [
            # 分析维度相关
            "汇总", "统计", "对比", "分析", "比较", "排名", "排序", "分区", "分组", "分类",
            # 时间维度相关
            "年度", "季度", "月度", "周", "日", "小时", "实时", "历史", "今天", "昨天", "近期", "长期", "趋势", "预测", 
            # 通用词
            "数据", "指标", "情况", "结果", "报表", "图表", "用户数", "人员", "员工", "效率"
        ]
    
    def _extract_business_keywords(self) -> List[str]:
        """
        提取业务关键词
        
        Returns:
            List[str]: 业务关键词列表
        """
        keywords = set()
        
        # 从表名和列名中提取
        tables = self.metadata_extractor.get_database_tables(self.db_name)
        for table in tables:
            # 添加表名
            keywords.add(table)
            
            # 获取表结构
            schema = self.metadata_extractor.get_table_schema(table, self.db_name)
            columns = schema.get("columns", [])
            
            # 添加列名
            for column in columns:
                keywords.add(column.get("name", ""))
                
                # 从列注释中提取关键词
                comment = column.get("comment", "")
                if comment and isinstance(comment, str):
                    # 简单分词（针对中文和英文）
                    words = re.findall(r'[\w\u4e00-\u9fff]+', comment)
                    keywords.update(words)
        
        # 从业务元数据中提取
        try:
            business_metadata = self.metadata_extractor.summarize_business_metadata(self.db_name)
            
            # 提取业务领域关键词
            domain = business_metadata.get("business_domain", "")
            if domain and isinstance(domain, str):
                words = re.findall(r'[\w\u4e00-\u9fff]+', domain)
                keywords.update(words)
            
            # 提取核心实体名称
            core_entities = business_metadata.get("core_entities", [])
            if isinstance(core_entities, list):
                for entity in core_entities:
                    if isinstance(entity, dict):
                        entity_name = entity.get("name", "")
                        if entity_name and isinstance(entity_name, str):
                            keywords.add(entity_name)
                        
                        # 从描述中提取
                        description = entity.get("description", "")
                        if description and isinstance(description, str):
                            words = re.findall(r'[\w\u4e00-\u9fff]+', description)
                            keywords.update(words)
        except Exception as e:
            logger.warning(f"提取业务元数据关键词时出错: {str(e)}")
        
        # 过滤出长度大于1的关键词
        filtered_keywords = [k for k in keywords if k and isinstance(k, str) and len(k) > 1]
        return filtered_keywords
    
    def _check_business_query_with_llm(self, query: str) -> Dict:
        """
        使用LLM判断是否是业务查询
        
        Args:
            query: 自然语言查询
            
        Returns:
            Dict: 包含判断结果和置信度的字典
        """
        # 初始化默认结果
        result = {
            "is_business_query": False,
            "confidence": 0.5,
            "reasoning": "",
            "keywords": []
        }
        
        try:
            # 为这个步骤创建单独的LLM客户端
            llm_client = get_llm_client(stage="business_check")
            
            if not llm_client:
                return result
            
            # 准备LLM请求
            system_prompt = """你是一个专业的业务数据分析师，负责判断用户查询是否是业务查询。
            
业务查询是指与公司运营、销售、财务、库存、客户、市场、产品等业务指标相关的查询。
以下是业务查询的一些例子：
- "上个月的销售额是多少"
- "最畅销的商品有哪些"
- "客户满意度趋势如何"
- "库存周转率是多少"
- "近期的营收情况"
- "各区域销售业绩对比"
- "产品退货率分析"

非业务查询的例子：
- "今天天气怎么样"
- "月球上的重力是多少"
- "世界上最高的山是什么"
- "如何烹饪意大利面"
- "明天是星期几"

请根据提供的查询内容仔细分析是否为业务查询，并提供你的判断、置信度和推理过程。
如果是业务查询，请同时提取出查询中的具体业务关键词（不要包括"今天"、"统计"、"对比"等非业务明确含义的词）。

回答格式应为JSON，包含以下字段：
{
    "is_business_query": true/false,
    "confidence": 0.0-1.0之间的值,
    "reasoning": "你的推理过程",
    "keywords": ["关键词1", "关键词2", ...]
}
"""

            # 用户查询的提示
            user_prompt = f"请判断以下查询是否是业务查询：\n\n{query}"
            
            # 记录LLM请求
            provider = os.getenv("LLM_PROVIDER", "默认")
            logger.info(f"请求LLM提供商 '{provider}' 判断：{query}")
            
            # 创建消息列表
            from src.utils.llm_client import Message
            messages = [
                Message.system(system_prompt),
                Message.user(user_prompt)
            ]
            
            # 向LLM请求判断结果
            response = llm_client.chat(messages)
            llm_response = response.content if response and hasattr(response, 'content') else ""
            logger.debug(f"LLM响应内容: {llm_response}")
            
            # 解析LLM响应
            if llm_response:
                # 使用处理多行JSON的函数解析内容
                parsed = self._parse_llm_json_response(llm_response)
                if parsed:
                    # 更新结果
                    result["is_business_query"] = bool(parsed.get("is_business_query", False))
                    result["confidence"] = float(parsed.get("confidence", 0.5))
                    result["reasoning"] = str(parsed.get("reasoning", ""))
                    
                    # 处理从LLM中获取的关键词，仅保留业务关键词（排除辅助关键词）
                    raw_keywords = parsed.get("keywords", [])
                    auxiliary_keywords = self._get_auxiliary_keywords()
                    result["keywords"] = [kw for kw in raw_keywords if kw and len(kw) > 1 and kw not in auxiliary_keywords]
                    
                    # 如果LLM判断为业务查询并给出了业务关键词，将其保存到数据库
                    try:
                        if result["is_business_query"] and result["keywords"]:
                            keywords_to_save = []
                            # 只保存业务关键词
                            for keyword in result["keywords"]:
                                # 再次确认过滤辅助关键词
                                if keyword not in auxiliary_keywords and len(keyword) > 1:
                                    keywords_to_save.append({
                                        'keyword': keyword,
                                        'confidence': result["confidence"],
                                        'category': 'LLM识别',
                                        'source': 'LLM生成'
                                    })
                            if keywords_to_save:
                                self.metadata_extractor.save_business_keywords(self.db_name, keywords_to_save)
                                logger.info(f"已将LLM识别的{len(keywords_to_save)}个业务关键词保存到数据库")
                    except Exception as e:
                        logger.error(f"保存LLM识别的业务关键词到数据库出错: {str(e)}")
                    
            # 保存查询日志
            query_id = str(uuid.uuid4())[:8]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_data = {
                "timestamp": datetime.now().isoformat(),
                "query": query,
                "is_business_query": result["is_business_query"],
                "confidence": result["confidence"],
                "reasoning": result["reasoning"],
                "keywords": result["keywords"],
                "query_id": query_id
            }
            log_path = log_query_process(log_data, log_type="business_query_check")
            logger.info(f"业务查询判断日志已保存至: {log_path}")
            
        except Exception as e:
            logger.error(f"LLM判断业务查询失败: {str(e)}")
            
        return result
    
    def _find_similar_example(self, query: str) -> Optional[Dict[str, Any]]:
        """
        查找相似的示例问题
        
        Args:
            query: 自然语言查询
            
        Returns:
            Optional[Dict[str, Any]]: 相似的示例问题，如果没有则返回None
        """
        if not self.qa_examples:
            return None
        
        try:
            # 初始化日志数据
            find_log = {
                "query": query,
                "function": "_find_similar_example",
                "examples_count": len(self.qa_examples)
            }
            
            # 准备系统提示
            system_prompt = """你是一个语义比较专家，负责评估两个问题的相似度。
请根据提供的问题和示例，计算它们的语义相似度，并确定它们是否在询问相同或非常相似的内容。

请以JSON格式返回结果，包含以下字段:
- similarity: 0到1之间的数字，表示相似度
- is_similar: 如果相似度大于0.7，则为true，否则为false
- explanation: 简短的解释，说明为什么认为它们相似或不相似

直接返回JSON，不要加任何额外解释。"""

            best_match = None
            best_similarity = 0
            
            # 创建日志目录
            log_dir = pathlib.Path(PROJECT_ROOT) / "log" / "llm_calls"
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 批量处理以提高效率
            batch_size = min(5, len(self.qa_examples))
            for i in range(0, len(self.qa_examples), batch_size):
                batch = self.qa_examples[i:i+batch_size]
                
                # 构建用户提示
                user_prompt = f"当前问题: {query}\n\n示例问题:\n"
                for j, example in enumerate(batch):
                    user_prompt += f"{j+1}. {example['question']}\n"
                
                user_prompt += "\n请逐一评估当前问题与每个示例问题的相似度。"
                
                # 记录LLM请求
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                request_log_path = log_dir / f"{timestamp}_find_similar_request_{i}.json"
                
                llm_request_log = {
                    "function": "_find_similar_example",
                    "query": query,
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                    "batch_index": i,
                    "batch_size": len(batch),
                    "timestamp": datetime.now().isoformat()
                }
                
                with open(request_log_path, 'w', encoding='utf-8') as f:
                    json.dump(llm_request_log, f, ensure_ascii=False, indent=2)
                    
                logger.info(f"相似示例查找请求日志已保存: {request_log_path}")
            
                # 使用特定于相似示例查找的LLM客户端
                llm_client = get_llm_client(stage="similar_example")
                messages = [
                    Message.system(system_prompt),
                    Message.user(user_prompt)
                ]
                response = llm_client.chat(messages)
                
                # 检查响应是否为None或为空
                if not response or not hasattr(response, 'content') or not response.content:
                    logger.warning(f"LLM返回了空响应，跳过此批次")
                    continue
                
                # 检查响应是否只包含<think>标签
                if response.content.strip() == "<think>" or response.content.strip() == "\\<think\\>":
                    logger.warning(f"LLM返回只包含<think>标签，使用简化提示重试")
                    # 简化提示，使用更直接的方式请求相似度评估
                    simplified_prompt = f"当前问题: {query}\n\n示例问题:\n"
                    for j, example in enumerate(batch):
                        simplified_prompt += f"{j+1}. {example['question']}\n"
                    simplified_prompt += "\n请直接计算当前问题与每个示例问题的相似度，返回0-1之间的数值。格式为：\n1. 相似度: 0.X\n2. 相似度: 0.X\n以此类推。"
                    
                    # 尝试使用简化提示重试
                    try:
                        simple_messages = [
                            Message.system("你是一个语义比较专家，负责计算问题的相似度。请直接返回相似度数值，不要添加其他内容。"),
                            Message.user(simplified_prompt)
                        ]
                        simple_response = llm_client.chat(simple_messages)
                        
                        if simple_response and hasattr(simple_response, 'content') and simple_response.content:
                            logger.info(f"简化提示获得响应: {simple_response.content[:100]}...")
                            # 尝试从简化响应中提取相似度数值
                            for j, example in enumerate(batch):
                                pattern = rf"{j+1}[.)].*?相似度[：:]\s*(\d+(\.\d+)?)"
                                matches = re.search(pattern, simple_response.content, re.DOTALL)
                                if matches:
                                    try:
                                        similarity = float(matches.group(1))
                                        logger.info(f"从简化响应中提取到示例 {j+1} 的相似度: {similarity}")
                                        if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                            best_similarity = similarity
                                            best_match = example
                                            logger.info(f"简化提示：找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                                    except ValueError:
                                        pass
                        else:
                            logger.warning("简化提示也返回了空响应，跳过此批次")
                            continue
                    except Exception as simple_error:
                        logger.error(f"尝试简化提示时出错: {str(simple_error)}")
                    
                    # 记录LLM响应
                    response_log_path = log_dir / f"{timestamp}_find_similar_response_{i}.json"
                    llm_response_log = {
                        "function": "_find_similar_example",
                        "query": query,
                        "response_content": response.content,
                        "batch_index": i,
                        "batch_size": len(batch),
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    try:
                        with open(response_log_path, 'w', encoding='utf-8') as f:
                            # 处理特殊字符，保留完整内容
                            safe_log = llm_response_log.copy()
                            for key, value in llm_response_log.items():
                                if isinstance(value, str) and ("<" in value or ">" in value):
                                    # 转义特殊字符
                                    safe_content = value.replace("\\", "\\\\").replace("\"", "\\\"")
                                    # 对于标签类内容，进行转义，避免被截断
                                    safe_content = safe_content.replace("<", "\\<").replace(">", "\\>")
                                    safe_log[key] = safe_content
                                    
                            json.dump(safe_log, f, ensure_ascii=False, indent=2)
                            
                        logger.info(f"相似示例查找响应日志已保存: {response_log_path}")
                    except Exception as log_err:
                        logger.error(f"保存响应日志时出错: {str(log_err)}")
                        # 尝试备份保存为文本文件
                        try:
                            text_log_path = str(response_log_path).replace(".json", ".txt")
                            with open(text_log_path, 'w', encoding='utf-8') as f:
                                f.write(f"Function: {llm_response_log['function']}\n")
                                f.write(f"Query: {llm_response_log['query']}\n")
                                f.write(f"Batch: {i+1}/{len(batch)}\n")
                                f.write(f"Timestamp: {llm_response_log['timestamp']}\n")
                                f.write("=== Response Content ===\n")
                                f.write(response.content)
                            logger.info(f"响应日志已保存为文本文件: {text_log_path}")
                        except Exception as text_err:
                            logger.error(f"保存文本日志也失败: {str(text_err)}")
                    
                    logger.info(f"LLM响应内容前100个字符: {response.content[:100]}...")
                    
                    # 解析结果
                try:
                    # 方法1: 检查是否是代码块格式 ```json {...} ```
                    json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
                    json_block_match = re.search(json_block_pattern, response.content)
                    
                    if json_block_match:
                        # 如果找到了代码块，尝试解析代码块内的内容
                        json_content = json_block_match.group(1).strip()
                        logger.info(f"从代码块中提取到JSON内容: {json_content[:100]}...")
                        
                        try:
                            parsed_result = json.loads(json_content)
                            logger.info(f"成功解析代码块中的JSON")
                            
                            # 处理解析结果
                            if isinstance(parsed_result, list):
                                # 如果是数组，处理每个结果
                                for j, item in enumerate(parsed_result):
                                    similarity = item.get("similarity", 0)
                                    if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                        best_similarity = similarity
                                        best_match = batch[j]
                                        logger.info(f"找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                            elif isinstance(parsed_result, dict):
                                # 如果是单个对象，可能只有一个示例
                                similarity = parsed_result.get("similarity", 0)
                                if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                    best_similarity = similarity
                                    best_match = batch[0]
                                    logger.info(f"找到更好的匹配(单个结果): 相似度 {similarity}")
                        except json.JSONDecodeError as e:
                            logger.warning(f"代码块中的JSON解析失败: {str(e)}")
                            # 继续尝试其他解析方法
                    else:
                        # 方法2: 尝试直接解析
                        try:
                            parsed_result = json.loads(response.content)
                            if isinstance(parsed_result, list):
                                # 如果是数组，处理每个结果
                                for j, item in enumerate(parsed_result):
                                    similarity = item.get("similarity", 0)
                                    if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                        best_similarity = similarity
                                        best_match = batch[j]
                                        logger.info(f"直接解析JSON：找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                            elif isinstance(parsed_result, dict):
                                # 如果是单个对象，可能只有一个示例
                                similarity = parsed_result.get("similarity", 0)
                                if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                    best_similarity = similarity
                                    best_match = batch[0]
                                    logger.info(f"直接解析JSON：找到更好的匹配(单个结果): 相似度 {similarity}")
                        except json.JSONDecodeError as e:
                            logger.warning(f"直接JSON解析失败: {str(e)}")
                            
                            # 方法3: 尝试从文本中提取JSON块
                        try:
                            # 尝试找到最外层的JSON块
                            cleaned_content = re.sub(r'[\r\n\t]', ' ', response.content)
                            json_start = cleaned_content.find('{')
                            json_end = cleaned_content.rfind('}') + 1
                            
                            if json_start >= 0 and json_end > json_start:
                                json_str = cleaned_content[json_start:json_end]
                                logger.info(f"找到JSON块: {json_str[:100]}...")
                                
                                try:
                                    parsed_result = json.loads(json_str)
                                    if isinstance(parsed_result, list):
                                        # 如果是数组，处理每个结果
                                        for j, item in enumerate(parsed_result):
                                            similarity = item.get("similarity", 0)
                                            if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                                best_similarity = similarity
                                                best_match = batch[j]
                                                logger.info(f"JSON块解析：找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                                    elif isinstance(parsed_result, dict):
                                        # 如果是单个对象，可能只有一个示例
                                        similarity = parsed_result.get("similarity", 0)
                                        if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                            best_similarity = similarity
                                            best_match = batch[0]
                                            logger.info(f"JSON块解析：找到更好的匹配(单个结果): 相似度 {similarity}")
                                except json.JSONDecodeError:
                                    logger.warning("提取的JSON块解析失败")
                        except Exception as e:
                            logger.warning(f"尝试提取JSON块时出错: {str(e)}")
                        
                        # 方法4: 从文本中提取相似度信息
                        logger.info(f"尝试从文本中提取相似度信息")
                        for j, example in enumerate(batch):
                            # 尝试多种模式匹配相似度
                            patterns = [
                                rf"{j+1}[.)].*?相似度[：:]\s*(\d+(\.\d+)?)",
                                rf"示例\s*{j+1}.*?相似度[：:]\s*(\d+(\.\d+)?)",
                                rf"问题\s*{j+1}.*?相似度[：:]\s*(\d+(\.\d+)?)"
                            ]
                            
                            for pattern in patterns:
                                matches = re.search(pattern, response.content, re.DOTALL)
                                if matches:
                                    try:
                                        similarity = float(matches.group(1))
                                        logger.info(f"从文本中提取到示例 {j+1} 的相似度: {similarity}")
                                        if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                            best_similarity = similarity
                                            best_match = example
                                            logger.info(f"文本提取：找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                                        break  # 找到一个匹配就跳出内层循环
                                    except ValueError:
                                        pass
                except Exception as e:
                    logger.error(f"处理相似度结果时出错: {str(e)}")
                except Exception as llm_error:
                    error_message = str(llm_error)
                    # 检查是否是连接错误
                    if "connection error" in error_message.lower():
                        logger.error(f"LLM服务连接失败: {error_message}")
                        # 记录错误但继续处理下一批，不中断整个过程
                        error_log = {
                            "query": query,
                            "function": "_find_similar_example",
                            "error": "LLM服务连接失败",
                            "error_details": error_message,
                            "batch_index": i,
                            "error_type": type(llm_error).__name__
                        }
                        log_query_process(error_log, "find_similar_connection_error")
                    else:
                        logger.error(f"LLM调用出错: {error_message}")
                        error_log = {
                            "query": query,
                            "function": "_find_similar_example",
                            "error": error_message,
                            "batch_index": i,
                            "error_type": type(llm_error).__name__
                        }
                        log_query_process(error_log, "find_similar_llm_error")
            
                # 记录最终结果
                if best_match:
                    find_log["found_match"] = True
                    find_log["best_similarity"] = best_similarity
                    find_log["best_match"] = {
                        "question": best_match.get("question", ""),
                        "sql": best_match.get("sql", "")
                    }
                    logger.info(f"找到最佳匹配示例，相似度: {best_similarity}")
                    log_query_process(find_log, "find_similar_example")
                else:
                    find_log["found_match"] = False
                    logger.info("未找到相似度超过阈值的示例")
                    log_query_process(find_log, "find_similar_example_none")
                    
                return best_match
        except Exception as e:
            logger.error(f"查找相似示例时出错: {str(e)}")
            error_log = {
                "query": query,
                "function": "_find_similar_example",
                "error": str(e),
                "error_type": type(e).__name__
            }
            log_query_process(error_log, "find_similar_example_error")
            return None
    
    def _generate_sql(self, query: str, similar_example: Optional[Dict[str, Any]] = None, business_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        生成SQL查询
        
        Args:
            query: 自然语言查询
            similar_example: 相似的示例问题
            business_metadata: 业务元数据
            
        Returns:
            Dict[str, Any]: 生成的SQL和相关信息
        """
        try:
            # 初始化日志数据
            llm_log = {
                "query": query,
                "function": "_generate_sql",
                "has_similar_example": similar_example is not None
            }
            
            # 获取数据库表和列信息
            tables_info = self._get_tables_info()
            logger.info(f"为查询 '{query}' 生成SQL")
            
            # 选择合适的提示词
            if similar_example:
                # 使用带有示例的用户提示
                system_prompt = NL2SQL_PROMPTS["system_with_schema"].format(
                    tables_info=tables_info
                )
                user_prompt = NL2SQL_PROMPTS["user_with_example"].format(
                    query=query,
                    example_query=similar_example['question'],
                    example_sql=similar_example['sql']
                )
                logger.info("使用带有示例的提示模板")
                
                # 记录示例信息
                llm_log["example"] = {
                    "question": similar_example['question'],
                    "sql": similar_example['sql']
                }
            else:
                # 使用标准提示
                system_prompt = NL2SQL_PROMPTS["system_with_schema"].format(
                    tables_info=tables_info
                )
                user_prompt = NL2SQL_PROMPTS["user"].format(query=query)
                logger.info("使用标准提示模板")
            
            # 记录提示信息
            llm_log["system_prompt"] = system_prompt
            llm_log["user_prompt"] = user_prompt
            llm_log["tables_info_length"] = len(tables_info)
            
            # 调用LLM
            start_time = time.time()
            
            # 使用SQL生成阶段特定的LLM客户端
            try:
                llm_client = get_llm_client(stage="sql_generation")
            except Exception as e:
                logger.error(f"获取SQL生成LLM客户端失败: {str(e)}")
                return {
                    "success": False,
                    "message": f"无法获取LLM客户端: {str(e)}",
                    "query": query
                }
                
            messages = [
                Message.system(system_prompt),
                Message.user(user_prompt)
            ]
            
            try:
                response = llm_client.chat(messages)
                execution_time = time.time() - start_time
                
                # 检查响应是否为None或为空
                if not response or not hasattr(response, 'content') or not response.content:
                    raise ValueError("LLM返回了空响应")
                
                # 检查响应是否只包含<think>标签
                if response.content.strip() == "<think>" or response.content.strip() == "\\<think\\>":
                    logger.warning(f"LLM返回只包含<think>标签，使用简化提示重新生成SQL")
                    
                    # 构建更简单直接的提示
                    retry_system_prompt = """你是SQL生成专家。请直接生成Apache Doris SQL代码，不要包含思考过程。
请严格按以下格式输出:
```sql
-- 你的SQL查询代码
```
不要包含任何其他文本、分析或<think>标签。"""

                    retry_user_prompt = f"""为以下问题生成SQL查询:

查询: {query}

数据库表结构:
{tables_info}

直接返回SQL代码，不需要解释或分析。"""

                    # 重试
                    try:
                        retry_messages = [
                            Message.system(retry_system_prompt),
                            Message.user(retry_user_prompt)
                        ]
                        
                        retry_response = llm_client.chat(retry_messages)
                        if retry_response and hasattr(retry_response, 'content') and retry_response.content:
                            logger.info(f"重试生成SQL成功，获得响应: {retry_response.content[:100]}...")
                            
                            # 使用重试结果替换原始响应
                            response = retry_response
                            # 更新日志
                            llm_log["retry_used"] = True
                        else:
                            logger.error("重试生成SQL失败，仍无法获得有效响应")
                    except Exception as e:
                        logger.error(f"重试生成SQL时出错: {str(e)}")
                
                # 记录LLM响应
                llm_log["llm_response"] = response.content
                llm_log["execution_time"] = execution_time
                llm_log["llm_provider"] = os.getenv("LLM_PROVIDER", "unknown")
                
                # 创建日志目录并保存LLM响应日志
                log_dir = pathlib.Path(PROJECT_ROOT) / "log" / "llm_calls"
                log_dir.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                response_log_path = log_dir / f"{timestamp}_generate_sql_response.json"
                llm_response_log = {
                    "function": "_generate_sql",
                    "query": query,
                    "response_content": response.content,
                    "timestamp": datetime.now().isoformat()
                }
                
                try:
                    with open(response_log_path, 'w', encoding='utf-8') as f:
                        # 处理特殊字符，保留完整内容
                        safe_log = llm_response_log.copy()
                        for key, value in llm_response_log.items():
                            if isinstance(value, str) and ("<" in value or ">" in value):
                                # 转义特殊字符
                                safe_content = value.replace("\\", "\\\\").replace("\"", "\\\"")
                                # 对于标签类内容，进行转义，避免被截断
                                safe_content = safe_content.replace("<", "\\<").replace(">", "\\>")
                                safe_log[key] = safe_content
                                
                        json.dump(safe_log, f, ensure_ascii=False, indent=2)
                    
                    logger.info(f"SQL生成响应日志已保存: {response_log_path}")
                except Exception as log_err:
                    logger.error(f"保存响应日志时出错: {str(log_err)}")
                    # 尝试备份保存为文本文件
                    try:
                        text_log_path = str(response_log_path).replace(".json", ".txt")
                        with open(text_log_path, 'w', encoding='utf-8') as f:
                            f.write(f"Function: {llm_response_log['function']}\n")
                            f.write(f"Query: {llm_response_log['query']}\n")
                            f.write(f"Timestamp: {llm_response_log['timestamp']}\n")
                            f.write("=== Response Content ===\n")
                            f.write(response.content)
                        logger.info(f"响应日志已保存为文本文件: {text_log_path}")
                    except Exception as text_err:
                        logger.error(f"保存文本日志也失败: {str(text_err)}")
                
                logger.info(f"LLM响应内容前100个字符: {response.content[:100]}...")
                
                # 提取SQL
                sql = self._extract_sql(response.content)
                llm_log["extracted_sql"] = sql
                
                # 保存LLM调用日志
                log_query_process(llm_log, "llm_generate_sql")
                
                if sql:
                    logger.info(f"成功生成SQL: {sql[:100]}...")
                    return {
                        'success': True,
                        'sql': sql,
                        'explanation': response.content,
                        'query': query
                    }
                else:
                    logger.warning(f"无法从LLM响应中提取SQL: {response.content[:100]}...")
                    return {
                        'success': False,
                        'message': '无法从生成的结果中提取有效的SQL。',
                        'explanation': response.content,
                        'query': query
                    }
            except Exception as llm_error:
                error_message = str(llm_error)
                logger.error(f"LLM调用或处理响应时出错: {error_message}")
                
                # 检查是否是连接错误
                if "connection error" in error_message.lower():
                    error_log = {
                        "query": query,
                        "function": "_generate_sql",
                        "error": "LLM服务连接失败，请稍后重试",
                        "error_details": error_message,
                        "error_type": type(llm_error).__name__
                    }
                    log_query_process(error_log, "llm_connection_error")
                    
                    return {
                        'success': False,
                        'message': 'LLM服务连接失败，请稍后重试。',
                        'query': query
                    }
                
                # 记录其他类型的错误
                error_log = {
                    "query": query,
                    "function": "_generate_sql",
                    "error": error_message,
                    "error_type": type(llm_error).__name__
                }
                log_query_process(error_log, "llm_error")
                
                return {
                    'success': False,
                    'message': f'LLM处理出错: {error_message}',
                    'query': query
                }
        except Exception as e:
            error_message = str(e)
            logger.error(f"生成SQL时出错: {error_message}")
            
            # 记录错误
            error_log = {
                "query": query,
                "function": "_generate_sql",
                "error": error_message,
                "error_type": type(e).__name__
            }
            log_query_process(error_log, "llm_error")
            
            return {
                'success': False,
                'message': f'生成SQL时出错: {error_message}',
                'query': query
            }
    
    def _get_tables_info(self) -> str:
        """
        获取数据库表和列信息的文本表示
        
        Returns:
            str: 表和列信息的文本表示
        """
        tables_info = ""
        
        try:
            tables = self.metadata_extractor.get_database_tables(self.db_name)
            
            for table in tables:
                # 获取表结构
                schema = self.metadata_extractor.get_table_schema(table, self.db_name)
                table_comment = schema.get("table_comment", "")
                columns = schema.get("columns", [])
                
                # 添加表信息
                tables_info += f"表名: {table}" + (f" (说明: {table_comment})" if table_comment else "") + "\n"
                tables_info += "列:\n"
                
                # 添加列信息
                for column in columns:
                    name = column.get("name", "")
                    type = column.get("type", "")
                    comment = column.get("comment", "")
                    
                    tables_info += f"  - {name} ({type})" + (f" # {comment}" if comment else "") + "\n"
                
                tables_info += "\n"
        except Exception as e:
            logger.error(f"获取表信息时出错: {str(e)}")
        
        return tables_info
    
    def _extract_sql(self, text: str) -> Optional[str]:
        """
        从文本中提取SQL语句，支持多种格式
        
        Args:
            text: 包含SQL的文本
            
        Returns:
            Optional[str]: 提取的SQL语句，如果没有则返回None
        """
        if not text:
            return None
        
        # 记录原始文本前100个字符，用于调试
        logger.info(f"尝试提取SQL，原始文本: {text[:100]}...")
            
        # 尝试从JSON格式中提取
        try:
            # 方法1: 检查是否是代码块格式 ```json {...} ```
            json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
            json_block_match = re.search(json_block_pattern, text)
            
            if json_block_match:
                # 如果找到了代码块，尝试解析代码块内的内容
                json_content = json_block_match.group(1).strip()
                logger.info(f"从代码块中提取到JSON内容: {json_content[:100]}...")
                
                try:
                    json_data = json.loads(json_content)
                    if isinstance(json_data, dict) and 'sql' in json_data:
                        sql = json_data.get('sql')
                        if sql and isinstance(sql, str):
                            logger.info(f"从JSON代码块中提取到SQL: {sql[:100]}...")
                            return sql.strip()
                except json.JSONDecodeError as e:
                    logger.warning(f"代码块中的JSON解析失败: {str(e)}")
            
            # 方法2: 尝试直接解析JSON
            try:
                json_data = json.loads(text)
                if isinstance(json_data, dict) and 'sql' in json_data:
                    sql = json_data.get('sql')
                    if sql and isinstance(sql, str):
                        logger.info(f"从JSON中提取到SQL: {sql[:100]}...")
                        return sql.strip()
            except json.JSONDecodeError as e:
                logger.warning(f"直接JSON解析失败: {str(e)}")
                
                # 方法3: 检查是否缺少大括号的情况
                if ('"sql"' in text or "'sql'" in text) and '{' not in text:
                    logger.info("检测到特殊格式: 包含sql字段但缺少大括号，尝试修复")
                    # 为内容添加大括号
                    fixed_content = '{' + text + '}'
                    try:
                        json_data = json.loads(fixed_content)
                        if isinstance(json_data, dict) and 'sql' in json_data:
                            sql = json_data.get('sql')
                            if sql and isinstance(sql, str):
                                logger.info(f"修复后从JSON中提取到SQL: {sql[:100]}...")
                                return sql.strip()
                    except json.JSONDecodeError:
                        logger.warning("添加大括号后仍然无法解析JSON")
                
                # 方法4: 尝试从文本中提取JSON块
                try:
                    # 尝试找到最外层的JSON块
                    cleaned_content = re.sub(r'[\r\n\t]', ' ', text)
                    json_start = cleaned_content.find('{')
                    json_end = cleaned_content.rfind('}') + 1
                    
                    if json_start >= 0 and json_end > json_start:
                        json_str = cleaned_content[json_start:json_end]
                        logger.info(f"找到JSON块: {json_str[:100]}...")
                        
                        try:
                            json_data = json.loads(json_str)
                            if isinstance(json_data, list):
                                # 如果是数组，处理每个结果
                                for j, item in enumerate(json_data):
                                    similarity = item.get("similarity", 0)
                                    if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                        best_similarity = similarity
                                        best_match = item
                                        logger.info(f"直接解析JSON：找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                            elif isinstance(json_data, dict):
                                # 如果是单个对象，可能只有一个示例
                                similarity = json_data.get("similarity", 0)
                                if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                    best_similarity = similarity
                                    best_match = json_data
                                    logger.info(f"直接解析JSON：找到更好的匹配(单个结果): 相似度 {similarity}")
                        except json.JSONDecodeError:
                            logger.warning("提取的JSON块解析失败")
                except Exception as e:
                    logger.warning(f"尝试提取JSON块时出错: {str(e)}")
                
                # 方法5: 直接使用正则表达式提取sql字段
                try:
                    sql_pattern = r'["\']sql["\']\s*:\s*["\'](.*?)["\']'
                    sql_match = re.search(sql_pattern, text, re.DOTALL)
                    if sql_match:
                        sql_value = sql_match.group(1)
                        logger.info(f"使用正则表达式直接提取到SQL值: {sql_value[:100]}...")
                        return sql_value.strip()
                except Exception as e:
                    logger.warning(f"使用正则表达式提取SQL时出错: {str(e)}")
        except Exception as e:
            logger.warning(f"从JSON提取SQL时出错: {str(e)}")

        # 方法6: 从```sql```块中提取
        sql_pattern = r"```sql\s*(.*?)\s*```"
        matches = re.search(sql_pattern, text, re.DOTALL)
        
        if matches:
            sql = matches.group(1).strip()
            logger.info(f"从SQL代码块中提取到SQL: {sql[:100]}...")
            return sql
        
        # 方法7: 从任何代码块中提取
        code_pattern = r"```(?:sql)?\s*(.*?)\s*```"
        matches = re.search(code_pattern, text, re.DOTALL)
        if matches:
            sql = matches.group(1).strip()
            # 检查这是否看起来像SQL语句
            if re.search(r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|SHOW|DESCRIBE)\b', sql, re.IGNORECASE):
                logger.info(f"从任意代码块中提取到SQL: {sql[:100]}...")
                return sql
        
        # 方法8: 如果没有找到```sql```块，尝试其他格式
        patterns = [
            # SQL标记
            r"SQL:\s*(SELECT.*?)(\n\n|\Z)",         # SQL: 标记后的SELECT语句
            r"查询语句:\s*(SELECT.*?)(\n\n|\Z)",    # 查询语句: 标记后的SELECT语句
            r"SQL语句:\s*(SELECT.*?)(\n\n|\Z)",     # SQL语句: 标记后的SELECT语句
            r"Query:\s*(SELECT.*?)(\n\n|\Z)",       # Query: 标记后的SELECT语句
            r"最终SQL:\s*(SELECT.*?)(\n\n|\Z)",     # 最终SQL: 标记后的语句
            r"生成的SQL:\s*(SELECT.*?)(\n\n|\Z)",   # 生成的SQL: 标记后的语句
            
            # 其他SQL类型
            r"SQL:\s*(INSERT.*?)(\n\n|\Z)",         # INSERT语句
            r"SQL:\s*(UPDATE.*?)(\n\n|\Z)",         # UPDATE语句
            r"SQL:\s*(DELETE.*?)(\n\n|\Z)",         # DELETE语句
            r"SQL:\s*(CREATE.*?)(\n\n|\Z)",         # CREATE语句
            r"SQL:\s*(DROP.*?)(\n\n|\Z)",           # DROP语句
            r"SQL:\s*(ALTER.*?)(\n\n|\Z)",          # ALTER语句
            r"SQL:\s*(SHOW.*?)(\n\n|\Z)",           # SHOW语句
            r"SQL:\s*(DESCRIBE.*?)(\n\n|\Z)",       # DESCRIBE语句
            
            # 独立行的SQL语句
            r"(\n|^)(SELECT.*?)(;\s*\n|\Z)",        # 独立行的SELECT语句
            r"(\n|^)(INSERT.*?)(;\s*\n|\Z)",        # 独立行的INSERT语句
            r"(\n|^)(UPDATE.*?)(;\s*\n|\Z)",        # 独立行的UPDATE语句
            r"(\n|^)(DELETE.*?)(;\s*\n|\Z)",        # 独立行的DELETE语句
            r"(\n|^)(CREATE.*?)(;\s*\n|\Z)",        # 独立行的CREATE语句
            r"(\n|^)(DROP.*?)(;\s*\n|\Z)",          # 独立行的DROP语句
            r"(\n|^)(ALTER.*?)(;\s*\n|\Z)",         # 独立行的ALTER语句
            r"(\n|^)(SHOW.*?)(;\s*\n|\Z)",          # 独立行的SHOW语句
            r"(\n|^)(DESCRIBE.*?)(;\s*\n|\Z)"       # 独立行的DESCRIBE语句
        ]
        
        for pattern in patterns:
            matches = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if matches:
                group_index = 1
                if "(\n|^)" in pattern:  # 对于带有前缀的模式，SQL在第2个组
                    group_index = 2
                sql = matches.group(group_index).strip()
                logger.info(f"从模式 '{pattern[:20]}...' 中提取到SQL: {sql[:100]}...")
                return sql
        
        # 方法9: 最后的尝试：查找任何看起来像SQL的文本
        sql_keywords = r"\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|LIMIT)\b"
        parts = re.split(r"\n\s*\n", text)  # 按空行分割文本
        
        for part in parts:
            # 如果一个段落包含多个SQL关键字，可能是SQL语句
            if len(re.findall(sql_keywords, part, re.IGNORECASE)) >= 3:
                # 尝试提取一个完整的SQL语句
                sql_match = re.search(r"(SELECT.*?)(;\s*$|\Z)", part, re.DOTALL | re.IGNORECASE)
                if sql_match:
                    sql = sql_match.group(1).strip()
                    logger.info(f"从文本中提取到SQL语句: {sql[:100]}...")
                    return sql
        
        # 如果无法提取SQL语句，记录警告并返回None
        logger.warning(f"无法从LLM响应中提取SQL: {text[:100]}...")
        return None
    
    def _execute_sql_with_retry(self, sql: str, query: str) -> Dict[str, Any]:
        """
        执行SQL查询，并在失败时尝试修正
        
        Args:
            sql: SQL查询
            query: 原始自然语言查询
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        # 执行查询
        retry_count = 0
        sql_query = sql
        errors = []
        
        while retry_count < self.max_retries:
            try:
                # 执行SQL
                start_time = time.time()
                result_df = execute_query_df(sql_query)
                execution_time = time.time() - start_time
                
                # 处理结果
                result = {
                    'success': True,
                    'data': result_df.to_dict(orient='records'),
                    'columns': result_df.columns.tolist(),
                    'row_count': len(result_df),
                    'execution_time': execution_time,
                    'sql': sql_query,
                    'retries': retry_count,
                    'errors': errors,
                    'query': query
                }
                
                return result
            except Exception as e:
                # 记录错误
                error_msg = str(e)
                errors.append(error_msg)
                logger.warning(f"SQL执行错误 (尝试 {retry_count+1}/{self.max_retries}): {error_msg}")
                
                # 尝试修正SQL
                sql_query = self._fix_sql(sql_query, error_msg, query)
                
                # 如果无法修正，则退出循环
                if not sql_query:
                    break
                
                retry_count += 1
        
        # 如果所有尝试都失败
        return {
            'success': False,
            'message': f'SQL执行失败 ({retry_count} 次尝试后)',
            'errors': errors,
            'sql': sql,
            'query': query
        }
    
    def _fix_sql(self, sql: str, error_msg: str, query: str) -> Optional[str]:
        """
        修正SQL查询
        
        Args:
            sql: 原始SQL查询
            error_msg: 错误信息
            query: 原始自然语言查询
            
        Returns:
            Optional[str]: 修正后的SQL查询，如果无法修正则返回None
        """
        try:
            # 初始化日志数据
            fix_log = {
                "query": query,
                "function": "_fix_sql",
                "original_sql": sql,
                "error_message": error_msg
            }
            
            # 获取表结构信息
            tables_info = self._get_tables_info()
            
            # 分析错误类型
            error_type = "未知错误"
            if "syntax error" in error_msg.lower():
                error_type = "语法错误"
            elif "table not found" in error_msg.lower() or "table doesn't exist" in error_msg.lower():
                error_type = "表不存在"
            elif "field not found" in error_msg.lower() or "column not found" in error_msg.lower():
                error_type = "字段不存在"
            elif "ambiguous" in error_msg.lower():
                error_type = "字段名称冲突"
            
            logger.info(f"尝试修复SQL错误，类型: {error_type}")
            fix_log["error_type"] = error_type
            
            # 使用提示词模板
            system_prompt = SQL_FIX_PROMPTS["system"].format(
                error_type=error_type,
                error_message=error_msg
            )
            
            # 拼接用户提示
            user_prompt = SQL_FIX_PROMPTS["user"].format(sql=sql)
            # 添加表结构信息
            user_prompt += f"\n\n数据库表结构:\n{tables_info}"
            # 添加原始查询
            user_prompt += f"\n\n原始问题: {query}"
            
            # 记录提示信息
            fix_log["system_prompt"] = system_prompt
            fix_log["user_prompt"] = user_prompt
            fix_log["tables_info_length"] = len(tables_info)
            
            # 调用LLM
            start_time = time.time()
            llm_client = get_llm_client()
            messages = [
                Message.system(system_prompt),
                Message.user(user_prompt)
            ]
            
            response = llm_client.chat(messages)
            execution_time = time.time() - start_time
            
            # 记录LLM响应
            fix_log["llm_response"] = response.content
            fix_log["execution_time"] = execution_time
            fix_log["llm_provider"] = os.getenv("LLM_PROVIDER", "unknown")
            
            # 提取SQL
            fixed_sql = self._extract_sql(response.content)
            
            # 如果没有找到```sql```块，尝试直接使用内容
            if not fixed_sql and response.content.strip().startswith('SELECT'):
                fixed_sql = response.content.strip()
            
            fix_log["fixed_sql"] = fixed_sql
            
            # 保存SQL修复日志
            log_query_process(fix_log, "llm_fix_sql")
            
            if fixed_sql:
                logger.info(f"SQL修复成功: {fixed_sql[:100]}...")
            else:
                logger.warning("无法从LLM响应中提取修复后的SQL")
                
            return fixed_sql
        except Exception as e:
            error_message = str(e)
            logger.error(f"修复SQL时出错: {error_message}")
            
            # 记录错误
            error_log = {
                "query": query,
                "function": "_fix_sql",
                "original_sql": sql,
                "error": error_message,
                "error_type": type(e).__name__
            }
            log_query_process(error_log, "sql_fix_error")
            
            return None 
    
    def _handle_multiline_json(self, content: str) -> Dict:
        """
        处理可能包含多行JSON的内容，提取第一个有效的JSON对象
        采用提取而非删除策略，保留原始响应的完整性
        
        Args:
            content: 可能包含多行JSON的文本内容
            
        Returns:
            Dict: 提取的JSON对象，如果没有则返回空字典
        """
        # 如果内容为空，直接返回空字典
        if not content or not content.strip():
            return {}
        
        # 1. 优先提取```json块中的内容
        json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
        json_block_match = re.search(json_block_pattern, content)
        
        if json_block_match:
            # 如果找到了代码块，尝试解析代码块内的内容
            json_content = json_block_match.group(1).strip()
            try:
                # 尝试解析JSON内容
                json_data = json.loads(json_content)
                # 如果成功解析，返回结果
                return json_data
            except json.JSONDecodeError:
                # 如果解析失败，继续尝试其他方法
                pass
        
        # 2. 检查是否存在<think>标签，如果有则优先处理标签后的内容
        if "<think>" in content and "</think>" in content:
            # 提取</think>后的内容
            post_think_content = content.split("</think>", 1)[1].strip()
            
            # 先尝试对</think>后的内容直接解析
            try:
                return json.loads(post_think_content)
            except json.JSONDecodeError:
                # 如果直接解析失败，尝试其他方法
                pass
                
            # 在</think>后内容中查找可能的JSON块
            try:
                json_start = post_think_content.find('{')
                json_end = post_think_content.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_part = post_think_content[json_start:json_end]
                    # 先处理可能的转义字符
                    json_part = json_part.replace('\\\\', '\\').replace('\\"', '"')
                    # 如果有转义的尖括号，还原它们
                    json_part = json_part.replace('\\<', '<').replace('\\>', '>')
                    return json.loads(json_part)
            except json.JSONDecodeError:
                # 继续尝试其他方法
                pass
                
        # 3. 尝试从全文提取第一个完整的JSON对象
        try:
            # 简单情况：整个内容就是一个JSON对象
            return json.loads(content)
        except json.JSONDecodeError:
            # 如果不是完整JSON，尝试提取第一个可能的JSON对象
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                try:
                    json_part = content[json_start:json_end]
                    # 处理可能的转义问题
                    json_part = json_part.replace('\\\\', '\\').replace('\\"', '"')
                    # 如果有转义的尖括号，还原它们
                    json_part = json_part.replace('\\<', '<').replace('\\>', '>')
                    return json.loads(json_part)
                except json.JSONDecodeError:
                    # 继续尝试
                    pass
        
        # 如果以上方法都失败，尝试按行解析
        lines = content.strip().split('\n')
        for line in lines:
            line = line.strip()
            if line and line.startswith('{') and line.endswith('}'):
                try:
                    # 尝试解析当前行
                    return json.loads(line)
                except json.JSONDecodeError:
                    # 如果解析失败，继续下一行
                    continue
        
        # 所有方法都失败，返回空字典
        return {}
        
    def _parse_llm_json_response(self, content: str) -> Dict:
        """
        解析LLM生成的JSON内容，处理各种格式问题
        采用提取而非删除策略，保留原始内容完整性
        
        Args:
            content: LLM生成的可能包含JSON的文本
            
        Returns:
            Dict: 解析后的结果，如果解析失败则包含error字段
        """
        if not content or not content.strip():
            logger.error("LLM返回内容为空或只包含空白字符")
            return {"error": "Empty or whitespace-only content", "content": content}
        
        # 记录原始内容的前100个字符，用于调试
        logger.info(f"原始LLM响应(前100字符): {content[:100]}")
        
        # 方法0: 尝试使用专门的多行JSON处理函数
        try:
            json_data = self._handle_multiline_json(content)
            if json_data:
                return json_data
        except Exception as e:
            logger.warning(f"多行JSON处理时出错: {str(e)}")
        
        # 方法1: 尝试提取```json代码块
        try:
            json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
            json_block_matches = re.findall(json_block_pattern, content)
            
            for json_block in json_block_matches:
                try:
                    return json.loads(json_block.strip())
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            logger.warning(f"提取JSON代码块时出错: {str(e)}")
        
        # 方法2: 尝试直接解析整个内容
        try:
            # 处理转义字符问题，使用字符串替代直接解析
            processed_content = content
            # 处理常见的转义问题
            processed_content = processed_content.replace('\\\\', '\\').replace('\\"', '"')
            # 如果有转义的尖括号，还原它们
            processed_content = processed_content.replace('\\<', '<').replace('\\>', '>')
            
            return json.loads(processed_content)
        except json.JSONDecodeError as e:
            logger.warning(f"直接解析JSON时出错: {str(e)}")
        
        # 方法3: 尝试清理内容后解析
        try:
            # 去除<think>标签及其内容
            if "<think>" in content and "</think>" in content:
                cleaned_content = content.split("</think>", 1)[1].strip()
            else:
                cleaned_content = content
                
            # 清理特殊字符和换行
            cleaned_content = re.sub(r'[\r\n\t]', ' ', cleaned_content)
            
            # 查找JSON块
            json_start = cleaned_content.find('{')
            json_end = cleaned_content.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = cleaned_content[json_start:json_end]
                # 处理转义问题
                json_text = json_text.replace('\\\\', '\\').replace('\\"', '"')
                # 如果有转义的尖括号，还原它们
                json_text = json_text.replace('\\<', '<').replace('\\>', '>')
                
                return json.loads(json_text)
        except (json.JSONDecodeError, IndexError) as e:
            logger.warning(f"清理后解析JSON时出错: {str(e)}")
        
        # 方法4: 处理可能的SQL字段内容，但缺少大括号的情况
        try:
            if ("fields" in content or "field_names" in content) and "{" not in content:
                # 包装内容到大括号中
                wrapped_content = "{" + content + "}"
                return json.loads(wrapped_content)
        except json.JSONDecodeError as e:
            logger.warning(f"处理SQL字段内容时出错: {str(e)}")
        
        # 方法5: 尝试修复常见的JSON格式错误
        try:
            # 替换单引号为双引号
            fixed_content = content.replace("'", "\"")
            # 修复没有引号的键
            fixed_content = re.sub(r'(\s*)(\w+)(\s*):', r'\1"\2"\3:', fixed_content)
            # 处理末尾可能的多余逗号
            fixed_content = re.sub(r',\s*}', '}', fixed_content)
            # 处理转义问题
            fixed_content = fixed_content.replace('\\\\', '\\').replace('\\"', '"')
            # 如果有转义的尖括号，还原它们
            fixed_content = fixed_content.replace('\\<', '<').replace('\\>', '>')
            
            # 提取JSON块
            json_start = fixed_content.find('{')
            json_end = fixed_content.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                return json.loads(fixed_content[json_start:json_end])
        except json.JSONDecodeError as e:
            logger.warning(f"修复JSON格式错误时出错: {str(e)}")
        
        # 所有JSON解析方法都失败，尝试提取SQL值
        if "field_names" in content or "fields" in content:
            try:
                # 尝试提取字段名
                field_names_match = re.search(r'field_names["\']?\s*:\s*\[(.*?)\]', content, re.DOTALL)
                fields_match = re.search(r'fields["\']?\s*:\s*\[(.*?)\]', content, re.DOTALL)
                
                field_names = []
                if field_names_match:
                    field_names_text = field_names_match.group(1).strip()
                    field_names = [name.strip(' \'"') for name in field_names_text.split(',')]
                elif fields_match:
                    fields_text = fields_match.group(1).strip()
                    # 处理可能的复杂字段定义
                    field_pattern = r'["\'](.*?)["\']'
                    field_matches = re.findall(field_pattern, fields_text)
                    field_names = field_matches
                
                if field_names:
                    return {"field_names": field_names}
            except Exception as e:
                logger.warning(f"提取SQL值时出错: {str(e)}")
        
        # 所有方法都失败
        logger.error(f"所有方法都无法提取有效JSON: {content[:100]}")
        return {"error": "Failed to parse JSON response", "content": content}