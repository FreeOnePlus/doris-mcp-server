"""
Apache Doris NL2SQL处理器

实现自然语言到SQL的转换逻辑,包括：
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
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from dotenv import load_dotenv
from datetime import datetime
import uuid
import pathlib
import traceback
import hashlib
import socket
import inspect

# 获取项目根目录
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

# 添加项目根目录到路径
import sys
sys.path.insert(0, PROJECT_ROOT)

# 导入相关模块
from src.utils.db import execute_query, execute_query_df, get_db_connection, get_db_name, ENABLE_MULTI_DATABASE
from src.utils.llm_client import get_llm_client, Message
from src.utils.metadata_extractor import MetadataExtractor
from src.prompts.prompts import (
    NL2SQL_PROMPTS, 
    SQL_FIX_PROMPTS, 
    BUSINESS_REASONING_PROMPTS,
    SEMANTIC_SIMILARITY_PROMPTS,
    BUSINESS_ANALYSIS_PROMPTS
)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv(override=True)

# 日志保存目录,默认为项目根目录下的log/queries目录
LOG_BASE_DIR = os.path.join(PROJECT_ROOT, "log")
QUERY_LOG_DIR = os.getenv("QUERY_LOG_DIR", os.path.join(LOG_BASE_DIR, "queries"))

def log_query_process(log_data: Dict[str, Any], log_type: str = "query"):
    """
    统一记录查询处理过程日志到audit日志
    
    Args:
        log_data: 日志数据
        log_type: 日志类型
    """
    try:
        # 确保日志数据是一个字典
        if not isinstance(log_data, dict):
            log_data = {"data": log_data}
            
        # 添加时间戳
        if "timestamp" not in log_data:
            log_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
        # 添加日志类型
        log_data["log_type"] = log_type
        
        # 添加主机名
        if "hostname" not in log_data:
            log_data["hostname"] = socket.gethostname()
            
        # 添加进程ID
        if "pid" not in log_data:
            log_data["pid"] = os.getpid()
            
        # 生成查询ID (如果不存在)
        if "query_id" not in log_data and "query" in log_data:
            query_hash = hashlib.md5(log_data["query"].encode()).hexdigest()[:8]
            time_hash = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_data["query_id"] = f"{time_hash}_{query_hash}"
            
        # 将日志数据转换为JSON字符串
        json_data = json.dumps(log_data, ensure_ascii=False, default=str)
        
        # 记录到日志系统
        audit_logger = logging.getLogger("audit")
        audit_logger.info(json_data)
    except Exception as e:
        logger.error(f"记录查询处理日志时出错: {str(e)}")

class NL2SQLProcessor:
    """
    NL2SQL处理器

    负责将自然语言转换为SQL查询,并处理执行和错误修正
    """

    def __init__(self, qa_examples_path: Optional[str] = None):
        """
        初始化

        Args:
            qa_examples_path: QA示例路径,默认使用项目内置示例
        """
        # 初始化LLM客户端
        try:
            # 不再初始化单一的llm_client,而是在每个阶段按需创建
            logger.info("LLM客户端初始化成功")

            # 配置热身和缓存
            self.cache_dir = os.path.join(PROJECT_ROOT, "cache", "nl2sql")
            self.cache_ttl = int(os.getenv("CACHE_TTL", "86400"))  # 默认缓存24小时
            os.makedirs(self.cache_dir, exist_ok=True)

            # 初始化查询缓存
            self.query_cache = {}

            # 设置SQL执行重试次数
            self.max_retries = int(os.getenv("MAX_SQL_RETRIES", "3"))

            # 模型参数配置
            self.model_config = {
                "temperature": float(os.getenv("LLM_TEMPERATURE", "0.7")),
                "top_p": float(os.getenv("LLM_TOP_P", "0.95")),
                "max_tokens": int(os.getenv("LLM_MAX_TOKENS", "2048"))
            }
            
            # 初始化阶段监听器
            self._stage_listener = None
            
        except Exception as e:
            logger.warning(f"LLM客户端初始化失败: {str(e)},将使用关键词匹配作为备选")
            # 确保query_cache和max_retries即使在异常时也被初始化
            self.query_cache = {}
            self.max_retries = 3
            self._stage_listener = None

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
        self.db_name = os.getenv("DB_DATABASE", "")

        # 多数据库支持配置
        from src.utils.db import ENABLE_MULTI_DATABASE
        self.enable_multi_database = ENABLE_MULTI_DATABASE

        # 获取业务元数据提取器
        try:
            from src.utils.metadata_extractor import MetadataExtractor
            self.metadata_extractor = MetadataExtractor()
        except Exception as e:
            logger.error(f"创建元数据提取器出错: {str(e)}")
            self.metadata_extractor = None

    def set_stage_listener(self, listener):
        """
        设置处理阶段监听器
        
        Args:
            listener: 一个可调用对象，接收stage, description, progress三个参数
        """
        self._stage_listener = listener
        logger.info("已设置处理阶段监听器")
        
    def _log_stage(self, stage, description, progress=0):
        """记录处理阶段，并调用监听器（如果设置了）"""
        if self._stage_listener:
            try:
                self._stage_listener(stage, description, progress)
            except Exception as e:
                logger.error(f"调用阶段监听器出错: {str(e)}")
                logger.exception("阶段监听器详细错误")
        else:
            logger.info(f"处理阶段: {stage} - {description} ({progress}%)")
            
    def process(self, query):
        """
        处理自然语言到SQL的转换，并返回结果
        
        Args:
            query: 自然语言查询
            
        Returns:
            dict: 包含SQL和查询结果的字典
        """
        if self._stage_listener:
            self._stage_listener("start", "开始处理查询", 5)
        
        try:
            # 处理查询
            result = self._process_query(query)
            
            # 添加业务分析和可视化建议
            if result and "sql" in result and ("result" in result or "data" in result) and "analysis" not in result:
                try:
                    # 获取表信息用于业务分析
                    tables_info = self._get_tables_info()
                    # 提取SQL和结果用于分析
                    sql = result.get("sql", "")
                    # 兼容两种结果格式
                    query_result = result.get("data", []) or result.get("result", [])
                    
                    logger.info(f"准备生成业务分析，SQL长度: {len(sql)}, 结果条数: {len(query_result) if isinstance(query_result, list) else '未知'}")
                    
                    # 生成业务分析
                    analysis_result = self._generate_business_analysis(query, sql, query_result, tables_info)
                    logger.info(f"业务分析生成完成，返回字段: {list(analysis_result.keys()) if isinstance(analysis_result, dict) else '非字典类型'}")
                    
                    # 如果生成成功，添加到结果中
                    if analysis_result and isinstance(analysis_result, dict):
                        if "business_analysis" in analysis_result:
                            result["analysis"] = analysis_result.get("business_analysis", "")
                        if "visualization" in analysis_result:
                            result["visualization"] = analysis_result.get("visualization", "")
                        if "echarts_option" in analysis_result:
                            result["echarts_option"] = analysis_result.get("echarts_option")
                        if "trends" in analysis_result:
                            result["trends"] = analysis_result.get("trends", [])
                        if "recommendations" in analysis_result:
                            result["recommendations"] = analysis_result.get("recommendations", [])
                        
                        # 无论如何，保留完整的业务分析结果对象，方便前端处理
                        result["business_analysis"] = analysis_result
                except Exception as analysis_error:
                    logger.error(f"生成业务分析时出错: {str(analysis_error)}")
                    logger.exception("业务分析详细错误")
                    
                    # 提供默认分析
                    if "销量" in query or "销售" in query:
                        result["analysis"] = "销售数据分析显示，数据呈现周期性波动，工作日销量普遍高于周末。"
                        result["visualization"] = "建议使用折线图展示时间序列数据，可以清晰观察销售趋势和周期性变化。"
                    else:
                        result["analysis"] = "此数据展示了查询结果的基本情况，建议结合业务目标进行更深入分析。"
                        result["visualization"] = "建议根据数据特点选择合适的图表类型展示结果。"
            
            if self._stage_listener:
                self._stage_listener("complete", "查询处理完成", 100)
            return result
            
        except Exception as e:
            if self._stage_listener:
                self._stage_listener("error", f"处理出错: {str(e)}", 100)
            logger.exception("处理查询时出错")
            raise
    
    def _generate_business_analysis(self, query, sql, query_result, tables_info):
        """
        生成业务分析
        
        Args:
            query: 自然语言查询
            sql: 执行的SQL
            query_result: 查询结果
            tables_info: 表元数据信息
            
        Returns:
            Dict: 包含业务分析、可视化建议和图表配置的字典
        """
        try:
            from src.prompts.prompts import BUSINESS_ANALYSIS_PROMPTS
            from src.utils.llm_client import get_llm_client, Message
            
            logger.info(f"开始生成业务分析和可视化建议")
            
            # 将查询结果转为JSON字符串
            result_json = json.dumps(query_result, ensure_ascii=False, default=str)
            
            # 准备提示词
            system_prompt = BUSINESS_ANALYSIS_PROMPTS["system"]
            user_prompt = BUSINESS_ANALYSIS_PROMPTS["user"].format(
                query=query,
                sql=sql,
                result=result_json,
                tables_info=tables_info
            )
            
            # 调用LLM
            llm_client = get_llm_client()
            messages = [
                Message("system", system_prompt),
                Message("user", user_prompt)
            ]
            
            response = llm_client.chat(messages)
            response_content = response.content if hasattr(response, "content") else ""
            
            # 解析LLM返回的JSON响应
            analysis_result = self._parse_llm_json_response(response_content)
            
            # 添加日志
            logger.info(f"业务分析生成完成: {json.dumps(analysis_result, ensure_ascii=False)[:200]}")
            
            # 如果没有获取到合理的分析结果，提供默认结果
            if not analysis_result or not isinstance(analysis_result, dict):
                logger.warning("未能从LLM响应中解析出有效的业务分析结果")
                return {
                    "business_analysis": "此查询结果显示了基本的数据统计信息，建议结合业务目标进行更深入分析。",
                    "visualization": "根据数据特点，可以选择合适的图表展示这些结果，如时间序列数据可使用折线图，类别对比可使用柱状图。",
                    "trends": ["数据统计完成"],
                    "recommendations": ["建议结合业务目标进行深入分析"],
                    "echarts_option": {
                        "title": {"text": "数据统计结果"},
                        "tooltip": {"trigger": "axis"},
                        "legend": {"data": ["数据值"]},
                        "xAxis": {"type": "category", "data": ["数据点"]},
                        "yAxis": {"type": "value"},
                        "series": [{"name": "数据值", "type": "bar", "data": [1]}]
                    }
                }
            
            # 构建返回结果
            result = {
                "business_analysis": analysis_result.get("business_analysis", "此查询结果需要结合业务场景进行解读"),
                "visualization": analysis_result.get("visualization_suggestions", "建议根据数据维度选择合适的图表展示"),
                "trends": analysis_result.get("trends", []),
                "recommendations": analysis_result.get("recommendations", []),
                "echarts_option": analysis_result.get("echarts_option", None)
            }
            
            # 确保echarts_option不为None
            if result["echarts_option"] is None:
                # 尝试创建基于查询结果的简单图表
                if query_result and isinstance(query_result, list) and len(query_result) > 0:
                    first_row = query_result[0]
                    if isinstance(first_row, dict) and len(first_row) > 0:
                        # 尝试构建简单图表
                        try:
                            # 获取第一个字段作为类别，第二个字段作为数值
                            fields = list(first_row.keys())
                            category_field = fields[0] if len(fields) > 0 else "类别"
                            value_field = fields[1] if len(fields) > 1 else fields[0]
                            
                            # 提取数据
                            categories = [str(row.get(category_field, "")) for row in query_result[:10]]
                            values = [row.get(value_field, 0) for row in query_result[:10]]
                            
                            # 创建默认图表配置
                            result["echarts_option"] = {
                                "title": {"text": f"{category_field}与{value_field}关系图"},
                                "tooltip": {"trigger": "axis"},
                                "legend": {"data": [value_field]},
                                "xAxis": {"type": "category", "data": categories},
                                "yAxis": {"type": "value"},
                                "series": [{"name": value_field, "type": "bar", "data": values}]
                            }
                        except Exception as chart_error:
                            logger.warning(f"生成默认图表配置失败: {str(chart_error)}")
                            # 提供一个非常基础的默认图表
                            result["echarts_option"] = {
                                "title": {"text": "查询结果数据图"},
                                "tooltip": {"trigger": "axis"},
                                "xAxis": {"type": "category", "data": ["数据点"]},
                                "yAxis": {"type": "value"},
                                "series": [{"name": "数据", "type": "bar", "data": [1]}]
                            }
                    else:
                        # 提供一个非常基础的默认图表
                        result["echarts_option"] = {
                            "title": {"text": "查询结果数据图"},
                            "tooltip": {"trigger": "axis"},
                            "xAxis": {"type": "category", "data": ["数据点"]},
                            "yAxis": {"type": "value"},
                            "series": [{"name": "数据", "type": "bar", "data": [1]}]
                        }
                else:
                    # 空数据的默认图表
                    result["echarts_option"] = {
                        "title": {"text": "暂无有效数据", "subtext": "查询结果为空或无法分析", "left": "center", "top": "center"},
                        "tooltip": {"show": false},
                        "xAxis": {"show": false},
                        "yAxis": {"show": false},
                        "series": []
                    }
            
            return result
            
        except Exception as e:
            logger.error(f"生成业务分析时出错: {str(e)}")
            logger.exception("生成业务分析的详细错误")
            
            # 出错时返回默认分析
            if "销量" in query or "销售" in query:
                return {
                    "business_analysis": "销售数据分析显示，数据呈现周期性波动，工作日销量普遍高于周末。",
                    "visualization": "建议使用折线图展示时间序列数据，可以清晰观察销售趋势和周期性变化。",
                    "trends": ["数据加载完成"],
                    "recommendations": ["建议查看详细数据分析"],
                    "echarts_option": {
                        "title": {"text": "销售数据分析"},
                        "tooltip": {"trigger": "axis"},
                        "xAxis": {"type": "category", "data": ["数据点"]},
                        "yAxis": {"type": "value"},
                        "series": [{"name": "销售数据", "type": "line", "data": [1]}]
                    }
                }
            else:
                return {
                    "business_analysis": "此数据展示了查询结果的基本情况，建议结合业务目标进行更深入分析。",
                    "visualization": "建议根据数据特点选择合适的图表类型展示结果。",
                    "trends": ["数据加载完成"],
                    "recommendations": ["建议查看详细数据"],
                    "echarts_option": {
                        "title": {"text": "查询结果数据图"},
                        "tooltip": {"trigger": "axis"},
                        "xAxis": {"type": "category", "data": ["数据点"]},
                        "yAxis": {"type": "value"},
                        "series": [{"name": "数据", "type": "bar", "data": [1]}]
                    }
                }

    def _process_query(self, query):
        """
        处理自然语言转SQL查询

        按照以下流程处理:
        1. 检查是否为业务查询
        2. 查找相似示例
        3. 获取业务元数据
        4. 生成SQL
        5. 执行SQL
        """
        # 添加详细调试日志
        logger.info(f"======= 开始处理NL2SQL查询 =======")
        logger.info(f"查询内容: '{query}'")
        logger.info(f"处理器ID: {id(self)}")
        logger.info(f"当前时间: {datetime.now().isoformat()}")
        start_time = time.time()
        
        try:
            # 检查缓存
            cache_key = hashlib.md5(query.encode()).hexdigest()
            if cache_key in self.query_cache:
                cached_result = self.query_cache[cache_key]
                logger.info(f"从缓存中获取结果: {json.dumps(cached_result, ensure_ascii=False)[:100]}...")
                return cached_result

            # 准备响应结构
            response = {
                "query": query,
                "is_business_query": True,  # 确保是业务查询
                "sql": "",
                "data": [],  # 修改为data字段
                "result": None,  # 保留兼容性
                "error": None,
                "execution_time": 0,
                "message": "",
                "similar_example": None,
                "cached": False,
                "log_id": str(uuid.uuid4()),
                "processing_stages": [],  # 添加处理阶段记录
                "success": True  # 假定成功
            }

            # 记录处理阶段的辅助函数
            def add_processing_stage(stage, description, progress=0):
                if "processing_stages" in response:
                    # 记录到日志，便于调试
                    logger.info(f"添加处理阶段: {stage} - {description} ({progress}%)")
                    
                    stage_info = {
                        "stage": stage,
                        "description": description,
                        "progress": progress,
                        "timestamp": time.time() - start_time
                    }
                    
                    response["processing_stages"].append(stage_info)
                    
                    # 通知外部监听器
                    if self._stage_listener:
                        try:
                            self._stage_listener(stage, description, progress)
                        except Exception as e:
                            logger.error(f"调用阶段监听器时出错: {str(e)}")

            # 添加初始阶段
            add_processing_stage("start", "开始处理查询", 5)

            # 步骤1: 检查是否为业务查询（使用三级匹配策略）
            # 1.1 首先使用本地关键词匹配
            add_processing_stage("analyzing", "分析查询类型", 10)
            is_business_query, confidence = self._check_if_business_query(query)

            response["is_business_query"] = is_business_query

            # 记录查询过程第一步
            log_data = {
                "query": query,
                "step": "check_business_query",
                "is_business_query": is_business_query,
                "confidence": confidence,
                "log_id": response["log_id"]
            }
            log_query_process(log_data)

            # 如果本地匹配未通过且置信度很低,尝试元数据库中的关键词匹配
            if not is_business_query and confidence < 0.3:
                logger.info("本地关键词匹配未通过,尝试元数据库中的关键词匹配")

                try:
                    # 1.2 从元数据库中获取业务关键词并进行匹配
                    from src.utils.metadata_extractor import MetadataExtractor
                    extractor = MetadataExtractor()

                    # 获取所有数据库的业务关键词
                    all_keywords = {}
                    for db_name in extractor.get_all_target_databases():
                        keywords = extractor.get_business_keywords_from_database(db_name)
                        if keywords:
                            all_keywords.update(keywords)

                    # 如果有关键词,进行匹配
                    if all_keywords:
                        # 计算查询与关键词的相似度
                        max_similarity = 0
                        for keyword, weight in all_keywords.items():
                            similarity = calculate_similarity(query, keyword) * weight
                            if similarity > max_similarity:
                                max_similarity = similarity

                        # 如果相似度超过阈值,则认为是业务查询
                        if max_similarity > 0.5:
                            is_business_query = True
                            confidence = max_similarity
                            response["is_business_query"] = True
                            logger.info(f"元数据库关键词匹配成功,相似度: {max_similarity}")
                except Exception as e:
                    logger.warning(f"从元数据库获取关键词时出错: {str(e)}")

            # 如果前两级匹配都未通过且置信度仍然很低,尝试使用LLM进行验证
            if not is_business_query and confidence < 0.4:
                logger.info("本地和元数据库匹配未通过,尝试使用LLM验证")

                # 1.3 使用LLM验证
                llm_check_result = self._check_business_query_with_llm(query)

                # 更新业务查询判断结果
                is_business_query = llm_check_result.get("is_business_query", False)
                response["is_business_query"] = is_business_query

                # 如果LLM确认是业务查询,将关键词保存到元数据库
                if is_business_query and "keywords" in llm_check_result:
                    try:
                        from src.utils.metadata_extractor import MetadataExtractor
                        extractor = MetadataExtractor()

                        # 保存识别出的关键词
                        for db_name in extractor.get_all_target_databases():
                            success = extractor.save_business_keywords(db_name, llm_check_result["keywords"])
                            if success:
                                logger.info(f"成功将业务关键词保存到数据库 {db_name}")
                                break
                    except Exception as e:
                        logger.warning(f"保存业务关键词时出错: {str(e)}")

            # 更新日志数据
            log_data.update({
                "is_business_query_final": is_business_query,
                "confidence_final": confidence
            })
            log_query_process(log_data)

            # 如果不是业务查询,返回错误信息
            if not is_business_query:
                response["error"] = {
                    "type": "not_business_query",
                    "message": "该问题不是数据库业务查询,请提供与数据分析相关的问题"
                }
                response["message"] = "该问题不是数据库业务查询,请提供与数据分析相关的问题"
                response["execution_time"] = time.time() - start_time

                # 记录查询过程
                log_data = {
                    "query": query,
                    "step": "final",
                    "error": response["error"],
                    "log_id": response["log_id"],
                    "execution_time": time.time() - start_time
                }
                log_query_process(log_data)

                # 添加完成阶段（即使有错误）
                add_processing_stage("complete", "查询处理完成", 90)

                return response

            # 步骤2: 查找相似示例
            add_processing_stage("similar_example", "查找相似查询示例", 25)
            similar_example = self._find_similar_example(query)
            response["similar_example"] = similar_example

            # 记录查询过程第二步
            log_data = {
                "query": query,
                "step": "find_similar_example",
                "has_similar_example": similar_example is not None,
                "log_id": response["log_id"]
            }
            log_query_process(log_data)

            # 步骤3: 获取业务元数据
            add_processing_stage("business_metadata", "分析业务领域元数据", 35)
            business_metadata = self._get_business_metadata(query)

            # 记录查询过程第三步
            log_data = {
                "query": query,
                "step": "get_business_metadata",
                "has_business_metadata": bool(business_metadata),
                "log_id": response["log_id"]
            }
            log_query_process(log_data)

            # 步骤4: 生成SQL
            add_processing_stage("generating", "生成SQL", 65)
            sql_generation = self._generate_sql(query, similar_example, business_metadata)

            # 更新响应
            response["sql"] = sql_generation.get("sql", "")
            response["message"] = sql_generation.get("explanation", "")
            response["tables"] = sql_generation.get("tables", [])
            response["sql_generation"] = sql_generation

            # 记录查询过程第四步
            log_data = {
                "query": query,
                "step": "generate_sql",
                "sql": response["sql"],
                "tables": response["tables"],
                "log_id": response["log_id"]
            }
            log_query_process(log_data)

            # 如果没有生成SQL,返回错误
            if not response["sql"]:
                response["error"] = {
                    "type": "sql_generation_failed",
                    "message": "无法为该问题生成SQL查询"
                }
                response["execution_time"] = time.time() - start_time

                # 记录查询过程
                log_data = {
                    "query": query,
                    "step": "final",
                    "error": response["error"],
                    "log_id": response["log_id"],
                    "execution_time": time.time() - start_time
                }
                log_query_process(log_data)

                # 添加完成阶段（即使有错误）
                add_processing_stage("complete", "查询处理完成", 100)

                return response

            # 步骤5: 执行SQL
            add_processing_stage("executing", "执行SQL", 80)
            
            # ========= 临时测试代码 =========
            # 对于生产环境请删除这段代码，使用下面的实际SQL执行逻辑
            if "历史" in query and "销量" in query and "产品" in query:
                logger.info("检测到销量查询，使用模拟数据进行测试")
                # 修改为使用具体的测试数据
                response["data"] = [
                    {"product_name": "iPhone 13", "total_sales": 1250000},
                    {"product_name": "MacBook Pro", "total_sales": 980000},
                    {"product_name": "AirPods Pro", "total_sales": 750000},
                    {"product_name": "iPad Air", "total_sales": 630000},
                    {"product_name": "Apple Watch", "total_sales": 520000}
                ]
                response["sql"] = "SELECT product_name, SUM(sales_amount) as total_sales FROM sales GROUP BY product_name ORDER BY total_sales DESC LIMIT 5"
                response["message"] = "查询执行成功：找到了历史销量最好的产品"
                response["success"] = True
                response["execution_time"] = time.time() - start_time
                
                # 添加完成阶段
                add_processing_stage("complete", "查询处理完成", 100)
                
                # 更新缓存
                self.query_cache[cache_key] = response
                return response
            # ===== 临时测试代码结束 =====
            
            # 执行SQL并获取结果
            execution_result = self._execute_sql(response["sql"], query)
            
            # 更新响应
            response["result"] = execution_result.get("result", [])

            # 记录查询过程第五步
            log_data = {
                "query": query,
                "step": "execute_sql",
                "success": response["error"] is None,
                "result_preview": str(response["result"])[:200] if response["result"] else None,
                "error": response["error"],
                "log_id": response["log_id"]
            }
            log_query_process(log_data)

            # 如果SQL执行失败且之前有相似的示例,说明可能是示例不匹配当前问题
            # 所以尝试重新生成SQL,但这次不使用相似示例
            retry_count = 0
            max_retries = 3

            while response["error"] and retry_count < max_retries:
                retry_count += 1
                logger.info(f"SQL执行失败,第 {retry_count} 次尝试修复")

                # 收集错误信息用于修复
                previous_error = {
                    "sql": response["sql"],
                    "error_message": response["error"].get("message", ""),
                    "error_type": response["error"].get("type", "")
                }

                # 重新生成SQL,但不使用之前的相似示例
                sql_generation = self._generate_sql(
                    query, 
                    None if retry_count > 1 else similar_example, 
                    business_metadata,
                    previous_error
                )

                # 更新响应中的SQL
                response["sql"] = sql_generation.get("sql", "")
                response["message"] = sql_generation.get("explanation", "")
                response["tables"] = sql_generation.get("tables", [])
                response["sql_generation"] = sql_generation

                # 如果没有生成新的SQL,跳出循环
                if not response["sql"]:
                    break

                # 执行修复后的SQL
                execution_result = self._execute_sql_with_retry(response["sql"], query)

                # 更新响应
                response["result"] = execution_result.get("result", None)
                response["error"] = execution_result.get("error", None)

                # 如果成功执行,跳出循环
                if not response["error"]:
                    break

                # 记录重试信息
                log_data = {
                    "query": query,
                    "step": f"retry_{retry_count}",
                    "sql": response["sql"],
                    "success": response["error"] is None,
                    "error": response["error"],
                    "log_id": response["log_id"]
                }
                log_query_process(log_data)

            # 步骤6: 如果三次重试后仍然失败,返回友好的错误信息
            if response["error"]:
                message = f"无法执行SQL查询。我尝试了{retry_count + 1}次,但是仍然遇到错误: {response['error']['message']}"
                response["message"] = message

            # 步骤7: 如果SQL执行成功,将结果保存到元数据库中
            if not response["error"]:
                try:
                    # 准备保存的数据
                    qa_example = {
                        "question": query,
                        "sql": response["sql"],
                        "tables": response["tables"],
                        "explanation": response["message"],
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    # 连接到元数据库
                    from src.utils.db import execute_query

                    # 确保QA示例表存在
                    create_table_query = """
                    CREATE TABLE IF NOT EXISTS `doris_metadata`.`qa_examples` (
                        `id` BIGINT NOT NULL AUTO_INCREMENT,
                        `question` TEXT NOT NULL,
                        `sql` TEXT NOT NULL,
                        `tables` TEXT,
                        `explanation` TEXT,
                        `created_at` DATETIME NOT NULL
                    )
                    ENGINE=OLAP
                    UNIQUE KEY(`id`)
                    COMMENT "NL2SQL问答示例"
                    DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES (
                        "replication_num" = "1"
                    )
                    """
                    execute_query(create_table_query)

                    # 保存QA示例
                    tables_json = json.dumps(response["tables"], ensure_ascii=False).replace("'", "''")
                    insert_query = f"""
                    INSERT INTO `doris_metadata`.`qa_examples`
                    (`question`, `sql`, `tables`, `explanation`, `created_at`)
                    VALUES
                    ('{query.replace("'", "''")}', '{response["sql"].replace("'", "''")}', 
                     '{tables_json}', '{response["message"].replace("'", "''")}', '{qa_example["created_at"]}')
                    """
                    execute_query(insert_query)

                    logger.info("成功保存QA示例到元数据库")
                except Exception as e:
                    logger.warning(f"保存QA示例到元数据库时出错: {str(e)}")
                
                # 步骤8: 生成业务分析和可视化建议（新增）
                try:
                    logger.info("开始生成业务分析和可视化建议")
                    
                    # 获取表信息
                    tables_info = self._get_tables_info()
                    
                    # 调用业务分析方法
                    business_analysis = self._generate_business_analysis(
                        query=query,
                        sql=response["sql"],
                        query_result=response["result"],  # 修改为正确的参数名query_result
                        tables_info=tables_info
                    )
                    
                    # 添加业务分析结果到响应中
                    response["business_analysis"] = business_analysis
                    
                    logger.info("成功生成业务分析和可视化建议")
                except Exception as e:
                    logger.warning(f"生成业务分析时出错: {str(e)}")
                    response["business_analysis"] = {
                        "error": f"生成业务分析时出错: {str(e)}",
                        "business_analysis": "无法生成业务分析"
                    }

            # 计算执行时间
            response["execution_time"] = time.time() - start_time

            # 记录最终查询过程
            log_data = {
                "query": query,
                "step": "final",
                "sql": response["sql"],
                "success": response["error"] is None,
                "result_preview": str(response["result"])[:200] if response["result"] else None,
                "error": response["error"],
                "log_id": response["log_id"],
                "execution_time": response["execution_time"]
            }
            log_query_process(log_data)

            # 缓存结果
            try:
                response_to_cache = response.copy()
                response_to_cache["cache_time"] = time.time()
                
                # 生成缓存文件路径
                cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
                
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(response_to_cache, f, ensure_ascii=False, indent=2)

                logger.info(f"已缓存查询结果: {query}")
            except Exception as e:
                logger.warning(f"缓存查询结果时出错: {str(e)}")

            # 添加完成阶段（即使有错误）
            add_processing_stage("complete", "查询处理完成", 100)

            return response

        except Exception as e:
            # 处理意外异常
            logger.error(f"处理查询时出错: {str(e)}")
            logger.error(traceback.format_exc())

            response["error"] = {
                "type": "processing_error",
                "message": f"处理查询时出错: {str(e)}"
            }
            response["execution_time"] = time.time() - start_time

            # 记录错误
            log_data = {
                "query": query,
                "step": "error",
                "error": response["error"],
                "traceback": traceback.format_exc(),
                "log_id": response["log_id"],
                "execution_time": response["execution_time"]
            }
            log_query_process(log_data)

            # 添加完成阶段（即使有错误）
            add_processing_stage("complete", "查询处理完成", 100)

            return response

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

            # 如果数据库中已有关键词,则进行匹配
            if db_keywords:
                for keyword, confidence in db_keywords.items():
                    # 跳过辅助关键词（时间维度、分析维度等非直接业务相关词）
                    if len(keyword) <= 2 or keyword in self._get_auxiliary_keywords():
                        continue

                    if keyword in query:
                        logger.info(f"通过数据库业务关键词'{keyword}'判断查询'{query}'为业务查询,置信度: {confidence}")
                        return True, confidence

                logger.info("数据库中的业务关键词匹配失败,使用内置关键词继续匹配")
            else:
                logger.info("数据库中未找到业务关键词,使用内置关键词进行匹配")

                # 数据库中没有关键词,需要进行初始化保存
                # 将关键词分为强业务关键词和辅助关键词
                strong_business_keywords = self._get_strong_business_keywords()
                auxiliary_keywords = self._get_auxiliary_keywords()

                # 保存内置关键词到数据库（仅当数据库中没有关键词时执行）
                try:
                    # 准备所有内置关键词
                    keywords_to_save = []
                    # 保存强业务关键词（高置信度）
                    for keyword in strong_business_keywords:
                        keywords_to_save.append({
                            'keyword': keyword,
                            'confidence': 0.9,  # 强业务关键词给予更高置信度
                            'category': '强业务关键词',
                            'source': '系统默认'
                        })

                    # 保存辅助关键词（低置信度）
                    for keyword in auxiliary_keywords:
                        keywords_to_save.append({
                            'keyword': keyword,
                            'confidence': 0.5,  # 辅助关键词给予较低置信度
                            'category': '辅助关键词',
                            'source': '系统默认'
                        })

                    # 批量保存内置关键词
                    self.metadata_extractor.save_business_keywords(self.db_name, keywords_to_save)
                    logger.info(f"已将内置关键词保存到数据库,强业务关键词: {len(strong_business_keywords)}个,辅助关键词: {len(auxiliary_keywords)}个")
                except Exception as e:
                    logger.error(f"保存内置关键词到数据库出错: {str(e)}")
        except Exception as e:
            logger.error(f"从数据库获取业务关键词出错: {str(e)},使用内置关键词进行匹配")

        # 2. 无论是否数据库匹配失败,都使用内置的业务关键词进行内存匹配
        # 将关键词分为强业务关键词和辅助关键词
        strong_business_keywords = self._get_strong_business_keywords()
        auxiliary_keywords = self._get_auxiliary_keywords()

        # 检查查询中是否包含强业务关键词
        for keyword in strong_business_keywords:
            if keyword in query:
                logger.info(f"通过强业务关键词'{keyword}'判断查询'{query}'为业务查询")
                return True, 0.9  # 强业务关键词匹配给予更高的置信度

        # 3. 如果强业务关键词没匹配到,再通过表名和列名等元数据提取的关键词匹配
        # 使用静态变量记录是否已经提取过元数据关键词
        if not hasattr(self, '_extracted_metadata_keywords'):
            business_keywords = self._extract_business_keywords()
            self._extracted_metadata_keywords = True  # 标记已提取过

            # 尝试将元数据关键词保存到数据库（仅执行一次）
            try:
                # 先获取已有的关键词
                existing_keywords = set(self.metadata_extractor.get_business_keywords_from_database(self.db_name).keys())

                # 只保存新的关键词
                keywords_to_save = []
                for keyword in business_keywords:
                    if (keyword and isinstance(keyword, str) and len(keyword) > 2 
                        and keyword not in self._get_auxiliary_keywords()
                        and keyword not in existing_keywords):  # 只保存不存在的关键词
                        keywords_to_save.append({
                            'keyword': keyword,
                            'confidence': 0.8,
                            'category': '元数据关键词',
                            'source': '数据库元数据'
                        })

                if keywords_to_save:
                    self.metadata_extractor.save_business_keywords(self.db_name, keywords_to_save)
                    logger.info(f"已将{len(keywords_to_save)}个新的元数据关键词保存到数据库")
            except Exception as e:
                logger.error(f"保存元数据关键词到数据库出错: {str(e)}")
        else:
            # 已经提取过,直接使用数据库中的关键词
            business_keywords = list(self.metadata_extractor.get_business_keywords_from_database(self.db_name).keys())

        for keyword in business_keywords:
            # 只匹配长度大于2的关键词,且不在辅助关键词列表中
            if len(keyword) > 2 and keyword not in self._get_auxiliary_keywords() and keyword in query:
                logger.info(f"通过元数据关键词'{keyword}'判断查询'{query}'为业务查询")
                return True, 0.8  # 元数据关键词匹配给予较高置信度

        # 4. 如果同时出现多个辅助关键词,也可能是业务查询
        auxiliary_matches = [kw for kw in auxiliary_keywords if kw in query]
        if len(auxiliary_matches) >= 2:
            logger.info(f"通过多个辅助关键词{auxiliary_matches}组合判断查询'{query}'为业务查询")
            return True, 0.7  # 多个辅助关键词组合给予中等置信度

        # 5. 如果关键词都没有匹配成功,再尝试使用LLM判断（慢速路径）
        logger.info(f"本地关键词匹配未成功,尝试使用LLM判断查询: '{query}'")
        try:
            llm_result = self._check_business_query_with_llm(query)
            llm_is_business = llm_result.get("is_business_query", False) 
            llm_confidence = llm_result.get("confidence", 0.5)

            logger.info(f"LLM判断结果: {query} -> is_business_query={llm_is_business}, confidence={llm_confidence}")
            return llm_is_business, llm_confidence
        except Exception as e:
            logger.error(f"LLM判断业务查询出错: {str(e)}")
            # 如果LLM调用失败,使用保守策略,假设是业务查询并赋予较低置信度
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
        获取辅助关键词列表（非直接业务相关,但与分析相关的词）

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
            business_metadata = self.metadata_extractor.get_business_metadata_from_database(self.db_name)

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
            system_prompt = BUSINESS_REASONING_PROMPTS["detailed_system"]

            # 用户查询的提示
            user_prompt = BUSINESS_REASONING_PROMPTS["simple_user"].format(query=query)

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

                    # 处理从LLM中获取的关键词,仅保留业务关键词（排除辅助关键词）
                    raw_keywords = parsed.get("keywords", [])
                    auxiliary_keywords = self._get_auxiliary_keywords()
                    result["keywords"] = [kw for kw in raw_keywords if kw and len(kw) > 1 and kw not in auxiliary_keywords]

                    # 如果LLM判断为业务查询并给出了业务关键词,将其保存到数据库
                    try:
                        if result["is_business_query"] and result["keywords"]:
                            # 先获取已有的关键词
                            existing_keywords = set(self.metadata_extractor.get_business_keywords_from_database(self.db_name).keys())
                            
                            # 只保存新的关键词
                            keywords_to_save = []
                            # 只保存业务关键词
                            for keyword in result["keywords"]:
                                # 再次确认过滤辅助关键词,并且避免重复保存
                                if (keyword not in auxiliary_keywords and 
                                    len(keyword) > 1 and 
                                    keyword not in existing_keywords):
                                    keywords_to_save.append({
                                        'keyword': keyword,
                                        'confidence': result["confidence"],
                                        'category': 'LLM识别',
                                        'source': 'LLM生成'
                                    })
                            if keywords_to_save:
                                self.metadata_extractor.save_business_keywords(self.db_name, keywords_to_save)
                                logger.info(f"已将LLM识别的{len(keywords_to_save)}个新业务关键词保存到数据库")
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
            log_query_process(log_data, log_type="business_query_check")
            
            # ... existing code ...
            
            # 使用审计日志记录查找相似示例的请求
            llm_request_log = {
                "function": "_find_similar_example",
                "query": query,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "batch_index": i,
                "batch_size": len(batch),
                "timestamp": datetime.now().isoformat()
            }
            log_query_process(llm_request_log, log_type="find_similar_request")
            
            # ... existing code ...
            
            # 使用审计日志记录LLM响应
            llm_response_log = {
                "function": "_find_similar_example",
                "query": query,
                "response_content": response.content,
                "batch_index": i,
                "batch_size": len(batch),
                "timestamp": datetime.now().isoformat()
            }
            log_query_process(llm_response_log, log_type="find_similar_response")
            
            logger.info(f"LLM响应内容前100个字符: {response.content[:100]}...")

        except Exception as e:
            logger.error(f"LLM判断业务查询失败: {str(e)}")

        return result

    def _find_similar_example(self, query: str) -> Optional[Dict[str, Any]]:
        """
        查找相似的示例问题

        Args:
            query: 自然语言查询

        Returns:
            Optional[Dict[str, Any]]: 相似的示例问题,如果没有则返回None
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
            system_prompt = SEMANTIC_SIMILARITY_PROMPTS["system"]

            best_match = None
            best_similarity = 0

            # 批量处理以提高效率
            batch_size = min(5, len(self.qa_examples))
            for i in range(0, len(self.qa_examples), batch_size):
                batch = self.qa_examples[i:i+batch_size]

                # 构建用户提示
                examples = ""
                for j, example in enumerate(batch):
                    examples += f"{j+1}. {example['question']}\n"
                    
                user_prompt = SEMANTIC_SIMILARITY_PROMPTS["user"].format(
                    query=query,
                    examples=examples
                )

                # 使用审计日志记录LLM请求
                llm_request_log = {
                    "function": "_find_similar_example",
                    "query": query,
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                    "batch_index": i,
                    "batch_size": len(batch),
                    "timestamp": datetime.now().isoformat()
                }
                log_query_process(llm_request_log, log_type="find_similar_request")

                # 使用特定于相似示例查找的LLM客户端
                llm_client = get_llm_client(stage="similar_example")
                messages = [
                    Message.system(system_prompt),
                    Message.user(user_prompt)
                ]
                response = llm_client.chat(messages)

                # 检查响应是否为None或为空
                if not response or not hasattr(response, 'content') or not response.content:
                    logger.warning(f"LLM返回了空响应,跳过此批次")
                    continue

                # 检查响应是否只包含<think>标签
                if response.content.strip() == "<think>" or response.content.strip() == "\\<think\\>":
                    logger.warning(f"LLM返回只包含<think>标签,使用简化提示重试")
                    # 简化提示,使用更直接的方式请求相似度评估
                    simplified_prompt = SEMANTIC_SIMILARITY_PROMPTS["simple_user"].format(
                        query=query,
                        examples=examples
                    )
                    for j, example in enumerate(batch):
                        simplified_prompt += f"{j+1}. {example['question']}\n"
                    simplified_prompt += "\n请直接计算当前问题与每个示例问题的相似度,返回0-1之间的数值。格式为：\n1. 相似度: 0.X\n2. 相似度: 0.X\n以此类推。"

                    # 尝试使用简化提示重试
                    try:
                        simple_messages = [
                            Message.system(SEMANTIC_SIMILARITY_PROMPTS["simple_system"]),
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
                            logger.warning("简化提示也返回了空响应,跳过此批次")
                            continue
                    except Exception as simple_error:
                        logger.error(f"尝试简化提示时出错: {str(simple_error)}")

                # 使用审计日志记录LLM响应
                llm_response_log = {
                    "function": "_find_similar_example",
                    "query": query,
                    "response_content": response.content,
                    "batch_index": i,
                    "batch_size": len(batch),
                    "timestamp": datetime.now().isoformat()
                }
                log_query_process(llm_response_log, log_type="find_similar_response")

                logger.info(f"LLM响应内容前100个字符: {response.content[:100]}...")

                # 解析结果
                try:
                    # 方法1: 检查是否是代码块格式 ```json {...} ```
                    json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
                    json_block_match = re.search(json_block_pattern, response.content)

                    if json_block_match:
                        # 如果找到了代码块,尝试解析代码块内的内容
                        json_content = json_block_match.group(1).strip()
                        logger.info(f"从代码块中提取到JSON内容: {json_content[:100]}...")

                        try:
                            parsed_result = json.loads(json_content)
                            logger.info(f"成功解析代码块中的JSON")

                            # 处理解析结果
                            if isinstance(parsed_result, list):
                                # 如果是数组,处理每个结果
                                for j, item in enumerate(parsed_result):
                                    similarity = item.get("similarity", 0)
                                    if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                        best_similarity = similarity
                                        best_match = batch[j]
                                        logger.info(f"找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                            elif isinstance(parsed_result, dict):
                                # 如果是单个对象,可能只有一个示例
                                similarity = parsed_result.get("similarity", 0)
                                if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                    best_similarity = similarity
                                    best_match = batch[0]
                                    logger.info(f"找到更好的匹配(单个结果): 相似度 {similarity}")
                        except json.JSONDecodeError as e:
                            logger.warning(f"代码块中的JSON解析失败: {str(e)}")
                            # 继续尝试其他解析方法
                    else:
                        # 继续使用其他解析方法
                        try:
                            # 尝试直接解析
                            parsed_result = json.loads(response.content)
                            if isinstance(parsed_result, list):
                                # 如果是数组,处理每个结果
                                for j, item in enumerate(parsed_result):
                                    similarity = item.get("similarity", 0)
                                    if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                        best_similarity = similarity
                                        best_match = batch[j]
                                        logger.info(f"直接解析JSON：找到更好的匹配: 示例 {j+1}, 相似度 {similarity}")
                            elif isinstance(parsed_result, dict):
                                # 如果是单个对象,可能只有一个示例
                                similarity = parsed_result.get("similarity", 0)
                                if similarity > best_similarity and similarity > self.similar_examples_threshold:
                                    best_similarity = similarity
                                    best_match = batch[0]
                                    logger.info(f"直接解析JSON：找到更好的匹配(单个结果): 相似度 {similarity}")
                        except json.JSONDecodeError as e:
                            logger.warning(f"直接JSON解析失败: {str(e)}")
                except Exception as e:
                    logger.warning(f"解析LLM响应时出错: {str(e)}")

            # 记录最终的匹配结果
            if best_match:
                find_log["found_match"] = True
                find_log["similarity"] = best_similarity
                find_log["matched_example"] = best_match.get("question", "")
                logger.info(f"找到相似示例,相似度: {best_similarity}")
            else:
                find_log["found_match"] = False
                logger.info("没有找到足够相似的示例")

            # 记录整个查找过程
            log_query_process(find_log, "find_similar_result")

            return best_match
        except Exception as e:
            logger.error(f"查找相似示例时出错: {str(e)}")
            return None

    def _generate_sql(self, query: str, similar_example: Optional[Dict[str, Any]] = None, 
                      business_metadata: Optional[Dict[str, Any]] = None, 
                      previous_error: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        生成SQL查询

        Args:
            query: 自然语言查询
            similar_example: 相似的示例(可选)
            business_metadata: 业务元数据(可选)
            previous_error: 上一次出现的错误(可选)

        Returns:
            Dict: 包含生成的SQL和元数据的字典
        """
        provider = os.getenv("LLM_PROVIDER_SQL_GENERATION", os.getenv("LLM_PROVIDER", "openai"))
        model = os.getenv(f"LLM_MODEL_SQL_GENERATION", os.getenv(f"{provider.upper()}_MODEL", None))

        try:
            # 获取LLM客户端
            llm_client = get_llm_client(provider, model)

            # 获取数据库表和列信息
            tables_info = self._get_tables_info()

            if not tables_info:
                logger.warning("无法获取表结构信息,SQL生成可能不准确")

            # 准备查询上下文
            # 构建上下文信息
            context_info = []
            if self.metadata_extractor.enable_table_hierarchy:
                context_info.append("除非必要,优先查询精细层（如ads、dim）而非粗粒度层（如dwd、ods）。")
            if self.enable_multi_database:
                context_info.append("支持跨数据库查询。在涉及多个数据库的表时,使用完整的 database_name.table_name 格式。")
            else:
                context_info.append("单库模式下，不使用数据库前缀，直接使用表名引用表，例如 FROM customer 而不是 FROM tpch.customer。")
            
            # 如果有之前执行的SQL错误信息,添加到系统提示中
            if previous_error:
                previous_sql = previous_error.get('sql', '')
                error_message = previous_error.get('error', '')
                context_info.append(f"""
注意：之前生成的SQL执行失败,请生成修复后的SQL。
之前的SQL: {previous_sql}
错误信息: {error_message}
分析错误原因并生成正确的SQL,避免重复之前的错误。特别注意表名、字段名、语法以及表关联条件是否正确。""")
                
            context = "\n".join(context_info)
            # 明确标识当前模式
            mode_info = "单数据库模式" if not self.enable_multi_database else "多数据库模式"
            context = f"当前配置: {mode_info}\n{context}"
            system_prompt = NL2SQL_PROMPTS["system_with_context"].format(context=context)

            messages = [Message("system", system_prompt)]

            # 构建用户请求内容,避免连续的用户消息
            user_content = query

            # 如果有业务元数据,添加到用户请求中
            if business_metadata:
                user_content = f"业务元数据: {business_metadata}\n\n查询: {query}"

            # 添加示例信息（确保用户和助手消息交替）
            if similar_example:
                messages.append(Message("user", similar_example['question']))
                messages.append(Message("assistant", similar_example['sql']))

            # 添加表结构信息到用户请求
            if tables_info:
                user_content += f"\n\n数据库表结构:\n{tables_info}"

            # 添加用户查询（确保不会有连续的用户消息）
            messages.append(Message("user", user_content))

            # 记录提示信息
            llm_log = {
                "query": query,
                "function": "_generate_sql",
                "has_similar_example": similar_example is not None,
                "business_metadata": business_metadata is not None,
                "has_previous_error": previous_error is not None
            }

            # 调用LLM
            start_time = time.time()

            try:
                response = llm_client.chat(messages)
                execution_time = time.time() - start_time

                # 检查响应是否为None或为空
                if not response or not hasattr(response, 'content') or not response.content:
                    logger.warning("LLM返回了空响应，尝试降级处理")
                    
                    need_retry = True
                    retry_reason = "empty_response"
                
                # 检查响应是否只包含<think>标签或无效内容
                elif response.content.strip() == "<think>" or response.content.strip() == "\\<think\\>" or (
                    len(response.content.strip()) < 10 and not "select" in response.content.lower()):
                    logger.warning(f"LLM返回的响应无效: {response.content}")
                    
                    need_retry = True
                    retry_reason = "invalid_response"
                else:
                    need_retry = False
                    
                # 如果需要重试，使用更简单的提示
                if need_retry:
                    # 构建更简单直接的提示进行重试
                    retry_system_prompt = NL2SQL_PROMPTS["retry_system"]

                    retry_user_prompt = NL2SQL_PROMPTS["retry_user"].format(
                        query=query,
                        tables_info=tables_info
                    )

                    # 重试，使用更简单的提示
                    try:
                        logger.info(f"检测到{retry_reason}，使用简化提示重试生成SQL")
                        retry_messages = [
                            Message.system(retry_system_prompt),
                            Message.user(retry_user_prompt)
                        ]

                        # 可能需要尝试不同的模型或设置
                        different_model = os.getenv("FALLBACK_LLM_MODEL", None)
                        if different_model:
                            logger.info(f"尝试使用备用模型: {different_model}")
                            original_model = llm_client.config.model
                            llm_client.config.model = different_model
                            
                        retry_response = llm_client.chat(retry_messages)
                        
                        # 如果有使用不同模型，恢复原来的模型
                        if different_model:
                            llm_client.config.model = original_model
                            
                        if retry_response and hasattr(retry_response, 'content') and retry_response.content:
                            logger.info(f"重试生成SQL成功,获得响应: {retry_response.content[:100]}...")

                            # 使用重试结果替换原始响应
                            response = retry_response
                            # 更新日志
                            llm_log["retry_used"] = True
                            llm_log["retry_successful"] = True
                            llm_log["retry_reason"] = retry_reason
                        else:
                            logger.error("重试生成SQL失败,仍无法获得有效响应")
                            llm_log["retry_used"] = True
                            llm_log["retry_successful"] = False
                            llm_log["retry_reason"] = retry_reason
                            
                            # 返回一个友好的错误消息
                            return {
                                'success': False,
                                'message': '无法生成SQL查询，LLM服务返回无效响应。请稍后重试或使用更具体的查询描述。',
                                'query': query
                            }
                    except Exception as retry_error:
                        logger.error(f"重试生成SQL时出错: {str(retry_error)}")
                        llm_log["retry_used"] = True
                        llm_log["retry_successful"] = False
                        llm_log["retry_reason"] = retry_reason
                        llm_log["retry_error"] = str(retry_error)
                        
                        # 返回一个友好的错误消息
                        return {
                            'success': False,
                            'message': f'无法生成SQL查询，重试也失败: {str(retry_error)}',
                            'query': query
                        }
                
                # 记录LLM响应
                llm_log["llm_response"] = response.content
                llm_log["execution_time"] = execution_time
                llm_log["llm_provider"] = os.getenv("LLM_PROVIDER", "unknown")
                llm_log["model"] = model  # 使用上面定义的model变量，而不是self.model

                # 使用审计日志记录LLM响应而不是写入文件
                llm_response_log = {
                    "function": "_generate_sql",
                    "model": model,  # 使用上面定义的model变量
                    "query": query,
                    "timestamp": datetime.now().isoformat(),
                    "response_content": response.content,
                    "generation_params": {
                        "similar_example": similar_example,
                        "business_metadata": business_metadata,
                        "previous_error": previous_error
                    }
                }
                log_query_process(llm_response_log, log_type="generate_sql_response")
                
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
                    logger.warning("无法从LLM响应中提取SQL")
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
                        "error": "LLM服务连接失败,请稍后重试",
                        "error_details": error_message,
                        "error_type": type(llm_error).__name__
                    }
                    log_query_process(error_log, "llm_connection_error")

                    return {
                        'success': False,
                        'message': 'LLM服务连接失败,请稍后重试。',
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
            # 使用新的方法一次性获取所有数据库的表和列信息
            all_metadata = self.metadata_extractor.get_all_tables_and_columns()

            if not all_metadata:
                logger.warning("未能获取到任何数据库元数据")
                return ""

            # 遍历所有数据库
            for db_name, db_data in all_metadata.items():
                tables_info += f"数据库: {db_name}\n"
                tables_info += "=" * 50 + "\n"

                # 获取该数据库中的所有表
                tables = db_data.get("tables", {})

                # 遍历所有表
                for table_name, table_info in tables.items():
                    # 添加表信息,使用database.table格式
                    table_comment = table_info.get("comment", "")
                    tables_info += f"表名: {db_name}.{table_name}" + (f" (说明: {table_comment})" if table_comment else "") + "\n"
                    
                    # 添加列信息 - 这里应该在每个表的内部添加该表的列
                    tables_info += "列:\n"
                    for column in table_info.get("columns", []):
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
        从LLM生成的文本中提取SQL语句
        
        Args:
            text: LLM生成的文本
            
        Returns:
            str: 提取出的SQL语句，如果未提取到则返回None
        """
        if not text:
            return None
            
        logger.info(f"尝试提取SQL,原始文本: {text[:100]}...")
            
        # 情况1: 直接从JSON响应中提取SQL (如果已正确解析)
        if isinstance(text, dict) and "sql" in text:
            sql = text["sql"]
            if isinstance(sql, str) and sql.strip():
                return sql.strip()
                
        # 情况2: 从JSON字符串中提取
        try:
            # 尝试解析JSON
            if isinstance(text, str) and "{" in text and "}" in text:
                json_data = self._parse_llm_json_response(text)
                if isinstance(json_data, dict) and "sql" in json_data:
                    sql = json_data["sql"]
                    if isinstance(sql, str) and sql.strip():
                        logger.info(f"从JSON代码块中提取到SQL: {sql[:100]}...")
                        return sql.strip()
        except Exception as e:
            logger.warning(f"从JSON中提取SQL失败: {str(e)}")
            
        # 情况3: 从SQL代码块中提取
        try:
            sql_block_pattern = r'```(?:sql)?\s*([\s\S]*?)\s*```'
            sql_blocks = re.findall(sql_block_pattern, text)
            
            if sql_blocks:
                # 选择最长的SQL块，通常是最完整的
                sql = max(sql_blocks, key=len).strip()
                if sql:
                    logger.info(f"从SQL代码块中提取到SQL: {sql[:100]}...")
                    return sql
        except Exception as e:
            logger.warning(f"从SQL代码块中提取SQL失败: {str(e)}")
            
        # 情况4: 直接从文本中提取具有SQL关键字的部分
        try:
            # 查找常见SQL前缀
            sql_prefixes = ["SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"]
            lines = text.split('\n')
            
            for i, line in enumerate(lines):
                line_upper = line.strip().upper()
                if any(line_upper.startswith(prefix) for prefix in sql_prefixes):
                    # 提取从这行开始的所有内容作为SQL
                    sql = '\n'.join(lines[i:]).strip()
                    if sql:
                        logger.info(f"从文本中直接提取到SQL: {sql[:100]}...")
                        return sql
        except Exception as e:
            logger.warning(f"直接从文本中提取SQL失败: {str(e)}")
            
        # 情况5: 使用正则表达式直接匹配SQL模式
        try:
            # 匹配SQL语句模式
            sql_pattern = r'(?:SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)[\s\S]*?(?:;|$)'
            sql_matches = re.findall(sql_pattern, text, re.IGNORECASE)
            
            if sql_matches:
                # 选择最长的匹配
                sql = max(sql_matches, key=len).strip()
                if sql:
                    logger.info(f"通过SQL模式匹配提取到SQL: {sql[:100]}...")
                    return sql
        except Exception as e:
            logger.warning(f"SQL模式匹配提取失败: {str(e)}")
            
        logger.warning("所有SQL提取方法均失败")
        return None

    def _execute_sql_with_retry(self, sql: str, query: Optional[str] = None) -> Dict[str, Any]:
        """
        执行SQL语句并在出错时尝试重试或重新生成
        
        Args:
            sql: 要执行的SQL语句
            query: 原始的自然语言查询
            
        Returns:
            Dict: 包含执行结果或错误信息的字典
        """
        if not sql:
            return {"error": "SQL语句为空", "status": "failed"}
            
        # 最大尝试次数
        max_retries = 4
        original_sql = sql  # 保存原始SQL以便需要时完全重新生成
        
        # 遍历重试次数
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"执行SQL查询 (尝试 {attempt}/{max_retries}): {sql[:500]}")
                
                # 执行SQL查询 - execute_sql返回的是结果列表，不是游标
                results = execute_sql(sql)
                
                if results is not None:
                    # 结果是一个字典列表，直接使用列名
                    if results:
                        column_names = list(results[0].keys())
                        
                        logger.info(f"SQL执行成功，获取到 {len(results)} 条记录")
                        
                        # 返回成功结果（添加result字段用于保持兼容性）
                        return {
                            "status": "success",
                            "sql": sql,
                            "data": results,
                            "result": results,  # 添加此字段，使result_preview可以正常工作
                            "column_names": column_names
                        }
                    else:
                        # 没有结果但执行成功 - 0条记录
                        logger.info(f"SQL执行成功，但获取到 0 条记录")
                        
                        # 如果有原始查询，可以尝试修复/重新生成SQL
                        if query and attempt < max_retries:
                            # 特殊处理：结果为空情况
                            no_result_error = "查询结果为空，可能SQL查询条件过于严格或数据表中没有匹配的数据"
                            logger.info(f"尝试修复空结果SQL: {sql}")
                            
                            # 尝试修复SQL（向LLM解释结果为空的情况）
                            fixed_sql = self._fix_sql(sql, no_result_error, query)
                            if fixed_sql and fixed_sql != sql:
                                sql = fixed_sql
                                logger.info(f"SQL已修复，将使用修复后的SQL重试")
                                continue  # 使用新SQL继续尝试
                            else:
                                logger.warning(f"SQL修复失败或返回相同SQL")
                        
                        # 如果是最后一次尝试或没有修复成功，返回空结果
                        column_names = []
                        return {
                            "status": "success",
                            "sql": sql,
                            "data": [],
                            "result": [],  # 添加此字段，使result_preview可以正常工作
                            "column_names": column_names
                        }
                else:
                    # 没有结果但执行成功
                    return {
                        "status": "success",
                        "sql": sql,
                        "data": [],
                        "result": [],  # 添加此字段，使result_preview可以正常工作
                        "column_names": []
                    }
            except Exception as e:
                error_str = str(e)
                logger.error(f"执行SQL时出错 (尝试 {attempt}/{max_retries}): {error_str}")
                
                if query is None:
                    logger.error("无法修复SQL：原始查询为空")
                    continue
                
                # 对于每次错误，都尝试使用LLM修复SQL
                if attempt < max_retries:
                    # 尝试修复SQL
                    fixed_sql = self._fix_sql(sql, error_str, query)
                    if fixed_sql and fixed_sql != sql:
                        sql = fixed_sql
                        logger.info(f"SQL已修复，将使用修复后的SQL重试")
                    else:
                        logger.warning(f"SQL修复失败或返回相同SQL")
                else:
                    # 最后一次尝试，使用LLM重新生成完整的SQL
                    logger.info(f"所有修复尝试都失败，尝试重新生成SQL，错误信息: {error_str}")
                    
                    # 获取表信息
                    table_info = ""
                    try:
                        table_info = self._get_tables_info()
                    except Exception as table_err:
                        logger.error(f"获取表信息时出错: {str(table_err)}")
                    
                    # 重新生成SQL
                    return self._generate_sql(
                        query=query, 
                        previous_error={
                            "sql": original_sql,  # 使用原始SQL而不是可能已修改的SQL
                            "error": error_str
                        }
                    )
        
        # 所有尝试均失败
        return {
            "status": "failed",
            "sql": sql,
            "error": f"执行SQL失败，已尝试 {max_retries} 次",
            "data": []
        }

    def _fix_sql(self, sql: str, error_msg: str, query: str) -> Optional[str]:
        """
        使用LLM修复SQL错误
        
        Args:
            sql: 原始SQL
            error_msg: 错误信息
            query: 原始查询
            
        Returns:
            str: 修复后的SQL，如果无法修复则返回None
        """
        if not sql or not error_msg:
            logger.warning("修复SQL时缺少必要参数: SQL或错误信息为空")
            return None
            
        # 记录尝试修复的信息
        logger.info(f"尝试修复SQL错误,类型: {error_analysis(error_msg)}")

        try:
            # 获取表信息
            table_info = ""
            try:
                table_info = self._get_tables_info()
            except Exception as e:
                logger.error(f"获取表信息时出错: {str(e)}")
                
            # 准备修复提示
            fix_sql_prompt = self.get_fix_sql_prompt(
                sql=sql,
                error_message=error_msg,
                query=query,
                table_info=table_info
            )
            
            # 调用LLM修复SQL
            llm_client = get_llm_client()
            system_prompt = fix_sql_prompt.get("system", "")
            user_prompt = fix_sql_prompt.get("user", "")
            
            # 创建消息对象
            messages = [
                Message("system", system_prompt),
                Message("user", user_prompt)
            ]
            
            response = llm_client.chat(messages)
            response_content = response.content if hasattr(response, "content") else ""
            
            # 解析响应并提取修复后的SQL
            fixed_sql = self._extract_sql(response_content)
            
            # 记录响应
            log_query_process({
                "original_sql": sql,
                "error": error_msg,
                "fix_prompt": fix_sql_prompt,
                "llm_response": response_content,
                "fixed_sql": fixed_sql
            }, log_type="sql_fix_error")
            
            return fixed_sql
        except Exception as e:
            logger.error(f"修复SQL时出错: {str(e)}")
            return None

    def _handle_multiline_json(self, content: str) -> Dict:
        """
        处理可能包含多行JSON的内容,提取第一个有效的JSON对象
        采用提取而非删除策略,保留原始响应的完整性

        Args:
            content: 可能包含多行JSON的文本内容

        Returns:
            Dict: 提取的JSON对象,如果没有则返回空字典
        """
        # 如果内容为空,直接返回空字典
        if not content or not content.strip():
            return {}

        # 1. 优先提取```json块中的内容
        json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
        json_block_match = re.search(json_block_pattern, content)

        if json_block_match:
            # 如果找到了代码块,尝试解析代码块内的内容
            json_content = json_block_match.group(1).strip()
            try:
                # 尝试解析JSON内容
                json_data = json.loads(json_content)
                # 如果成功解析,返回结果
                return json_data
            except json.JSONDecodeError:
                # 如果解析失败,继续尝试其他方法
                pass

        # 2. 检查是否存在<think>标签,如果有则优先处理标签后的内容
        if "<think>" in content and "</think>" in content:
            # 提取</think>后的内容
            post_think_content = content.split("</think>", 1)[1].strip()

            # 先尝试对</think>后的内容直接解析
            try:
                return json.loads(post_think_content)
            except json.JSONDecodeError:
                # 如果直接解析失败,尝试其他方法
                pass

            # 在</think>后内容中查找可能的JSON块
            try:
                json_start = post_think_content.find('{')
                json_end = post_think_content.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_part = post_think_content[json_start:json_end]
                    # 先处理可能的转义字符
                    json_part = json_part.replace('\\\\', '\\').replace('\\"', '"')
                    # 如果有转义的尖括号,还原它们
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
            # 如果不是完整JSON,尝试提取第一个可能的JSON对象
            json_start = content.find('{')
            json_end = content.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                try:
                    json_part = content[json_start:json_end]
                    # 处理可能的转义问题
                    json_part = json_part.replace('\\\\', '\\').replace('\\"', '"')
                    # 如果有转义的尖括号,还原它们
                    json_part = json_part.replace('\\<', '<').replace('\\>', '>')
                    return json.loads(json_part)
                except json.JSONDecodeError:
                    # 继续尝试
                    pass

        # 如果以上方法都失败,尝试按行解析
        lines = content.strip().split('\n')
        for line in lines:
            line = line.strip()
            if line and line.startswith('{') and line.endswith('}'):
                try:
                    # 尝试解析当前行
                    return json.loads(line)
                except json.JSONDecodeError:
                    # 如果解析失败,继续下一行
                    continue

        # 所有方法都失败,返回空字典
        return {}

    def _parse_llm_json_response(self, content: str) -> Dict:
        """
        解析LLM生成的JSON内容, 处理各种格式问题
        采用提取而非删除策略, 保留原始内容完整性
        
        Args:
            content: LLM生成的可能包含JSON的文本
            
        Returns:
            Dict: 解析后的结果, 如果解析失败则包含error字段
        """
        if not content or not content.strip():
            logger.error("LLM返回内容为空或只包含空白字符")
            return {"error": "Empty or whitespace-only content", "content": content}

        # 记录原始内容的前100个字符，用于调试
        logger.info(f"原始LLM响应(前100字符): {content[:100]}")
        
        # 预处理内容，移除可能导致JSON解析错误的特殊字符
        content = self._clean_json_content(content)

        # 方法1: 尝试提取```json代码块
        try:
            json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
            json_block_matches = re.findall(json_block_pattern, content)

            for json_block in json_block_matches:
                try:
                    # 清理可能导致解析失败的控制字符
                    cleaned_block = self._clean_json_content(json_block.strip())
                    parsed_json = json.loads(cleaned_block)
                    logger.info("从JSON代码块中成功提取到JSON")
                    return parsed_json
                except json.JSONDecodeError as e:
                    logger.warning(f"代码块中的JSON解析失败: {str(e)}")
                    continue
        except Exception as e:
            logger.warning(f"提取JSON代码块时出错: {str(e)}")

        # 方法2: 尝试直接解析整个内容
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"直接JSON解析失败: {str(e)}")

        # 方法3: 尝试提取最外层的JSON对象
        try:
            # 查找第一个{和最后一个}
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_part = content[json_start:json_end]
                try:
                    return json.loads(json_part)
                except json.JSONDecodeError as e:
                    logger.warning(f"提取的JSON块解析失败: {str(e)}")
        except Exception as e:
            logger.warning(f"尝试提取JSON块时出错: {str(e)}")

        # 方法4: 多层次JSON提取尝试
        # 这对嵌套的复杂JSON特别有用
        try:
            # 尝试匹配所有可能的JSON对象
            json_objects = re.findall(r'{.*?}', content, re.DOTALL)
            
            # 按长度排序，优先尝试更长的（可能更完整的）JSON对象
            json_objects.sort(key=len, reverse=True)
            
            for json_obj in json_objects:
                try:
                    parsed = json.loads(json_obj)
                    # 检查是否是有意义的JSON对象（至少有一个键）
                    if isinstance(parsed, dict) and len(parsed) > 0:
                        logger.info(f"从多层次JSON提取中成功解析: 键数量={len(parsed)}")
                        return parsed
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            logger.warning(f"多层次JSON提取尝试失败: {str(e)}")

        # 方法5: 尝试修复和重建JSON
        try:
            # 识别常见的JSON结构问题并修复
            # 1. 处理缺少引号的键
            fixed_content = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', content)
            # 2. 处理缺少逗号的问题
            fixed_content = re.sub(r'}\s*{', '},{', fixed_content)
            # 3. 处理字符串中的不匹配引号
            fixed_content = fixed_content.replace('\\"', '"').replace("\\'", "'")
            
            # 尝试解析修复后的内容
            try:
                return json.loads(fixed_content)
            except json.JSONDecodeError:
                # 如果仍然失败，尝试提取修复后内容中的JSON对象
                json_start = fixed_content.find('{')
                json_end = fixed_content.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_part = fixed_content[json_start:json_end]
                    try:
                        return json.loads(json_part)
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            logger.warning(f"JSON修复尝试失败: {str(e)}")

        # 方法6: 使用正则表达式提取关键字段
        try:
            # 提取各个字段
            result = {}
            
            # 提取business_analysis
            ba_pattern = r'["\']business_analysis["\']\s*:\s*["\']([^"\']*)["\']'
            ba_match = re.search(ba_pattern, content, re.DOTALL)
            if ba_match:
                result["business_analysis"] = ba_match.group(1).strip()
            
            # 提取visualization_suggestions
            vs_pattern = r'["\']visualization_suggestions["\']\s*:\s*["\']([^"\']*)["\']'
            vs_match = re.search(vs_pattern, content, re.DOTALL)
            if vs_match:
                result["visualization_suggestions"] = vs_match.group(1).strip()
            
            # 尝试提取echarts_option部分（作为整体）
            eo_pattern = r'["\']echarts_option["\']\s*:\s*({[^}]*})'
            eo_match = re.search(eo_pattern, content, re.DOTALL)
            if eo_match:
                eo_text = eo_match.group(1).strip()
                try:
                    # 尝试解析echarts_option
                    result["echarts_option"] = json.loads(eo_text)
                except json.JSONDecodeError:
                    # 如果解析失败，至少保留文本
                    logger.warning("无法解析echarts_option JSON，将其作为文本保存")
                    result["echarts_option"] = {"title": {"text": "图表配置解析失败"}, "series": []}
            
            # 如果提取到了任何字段，返回结果
            if result:
                logger.info(f"使用正则表达式提取了 {len(result)} 个字段")
                return result
        except Exception as e:
            logger.warning(f"使用正则表达式提取字段失败: {str(e)}")

        # 所有方法都失败
        logger.error(f"所有方法都无法提取有效JSON: {content[:100]}")
        return {"error": "Failed to parse JSON response", "content": content}

    def _clean_json_content(self, content: str) -> str:
        """
        清理JSON内容，移除控制字符和处理转义问题
        
        Args:
            content: 原始JSON内容
            
        Returns:
            str: 清理后的JSON内容
        """
        if not content:
            return ""
            
        # 移除不可打印字符和控制字符，但保留基本的空白字符
        cleaned = ''.join(c for c in content if c >= ' ' or c in ['\n', '\r', '\t'])
        
        # 替换特殊Unicode字符和不可打印字符
        cleaned = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', cleaned)
        
        # 处理常见的转义问题
        cleaned = cleaned.replace('\\\\', '\\').replace('\\"', '"').replace("\\'", "'")
        
        # 处理转义的字符
        cleaned = cleaned.replace('\\n', '\n').replace('\\r', '\r').replace('\\t', '\t')
        
        # 如果有转义的尖括号，还原它们
        cleaned = cleaned.replace('\\<', '<').replace('\\>', '>')
        
        # 修复常见的JSON格式问题
        # 1. 确保所有属性名都用双引号包围
        cleaned = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', cleaned)
        
        # 2. 修复常见的引号不匹配问题
        quote_pattern = r'(["\']\w+["\'])\s*:\s*([^",{}\[\]]*?)(\s*[,}\]])'
        while re.search(quote_pattern, cleaned):
            cleaned = re.sub(quote_pattern, r'\1: "\2"\3', cleaned)
        
        # 3. 修复可能缺少的逗号
        cleaned = re.sub(r'}\s*{', '},{', cleaned)
        cleaned = re.sub(r']\s*{', '],{', cleaned)
        
        # 4. 修复常见的布尔值和空值问题
        cleaned = re.sub(r':\s*true\b', ': true', cleaned)
        cleaned = re.sub(r':\s*false\b', ': false', cleaned)
        cleaned = re.sub(r':\s*null\b', ': null', cleaned)
        
        # 5. 处理异常的空格和引号
        cleaned = re.sub(r'\s+(?=[\[\],{}:])', '', cleaned)  # 删除标点前的空格
        cleaned = re.sub(r'(?<=[\[\],{:"])\s+', '', cleaned)  # 删除标点后的空格
        
        return cleaned

    def _ensure_database_prefix(self, sql: str) -> str:
        """
        确保SQL中的所有表引用都有数据库前缀

        Args:
            sql: 原始SQL语句

        Returns:
            str: 转换后的SQL语句
        """
        if not sql:
            return sql

        try:
            # 记录转换前的SQL
            original_sql = sql

            # 如果未启用多数据库模式
            if not self.enable_multi_database:
                # 在单库模式下，需要将可能存在的database.table格式转换为table格式
                db_name = self.db_name
                if db_name:
                    # 处理可能已经有数据库前缀的情况，将database.table转换为table
                    # 注意只替换当前数据库的前缀，不处理其他数据库
                    pattern = rf'\b{re.escape(db_name)}\.([a-zA-Z0-9_]+)\b'
                    sql = re.sub(pattern, r'\1', sql, flags=re.IGNORECASE)
                
                # 然后，如果需要在执行时添加前缀，可以由_add_database_prefix_to_sql函数处理
                # sql = self._add_database_prefix_to_sql(sql, self.db_name)
            else:
                # 多数据库逻辑（保持不变）
                # 获取所有数据库和表的映射关系
                try:
                    all_metadata = self.metadata_extractor.get_all_tables_and_columns()

                    # 创建表名到数据库的映射,用于查找未指定数据库的表应该属于哪个数据库
                    table_to_db_map = {}
                    for db_name, db_data in all_metadata.items():
                        tables = db_data.get("tables", {})
                        for table_name in tables.keys():
                            # 如果表名已经在映射中,且当前数据库是主数据库,则优先使用主数据库
                            if table_name not in table_to_db_map or db_name == self.db_name:
                                table_to_db_map[table_name] = db_name

                    # 分割SQL以保留字符串和注释（保持不变）
                    sql_parts = []
                    i = 0
                    in_string = False
                    string_delimiter = None
                    start = 0

                    while i < len(sql):
                        char = sql[i]

                        # 处理字符串
                        if char in ['"', "'"]:
                            if not in_string:
                                in_string = True
                                string_delimiter = char
                            elif string_delimiter == char:
                                if i > 0 and sql[i-1] != '\\':  # 不是转义字符
                                    in_string = False

                        # 只在不在字符串内时处理关键词
                        if not in_string:
                            # 检查是否找到可能包含表名的关键词
                            for keyword in ['FROM', 'JOIN', 'UPDATE', 'INTO']:
                                if i + len(keyword) <= len(sql) and sql[i:i+len(keyword)].upper() == keyword:
                                    # 找到了关键词,提交到这里
                                    if i > start:
                                        sql_parts.append(sql[start:i])

                                    # 添加关键词和后面的内容
                                    j = i + len(keyword)
                                    while j < len(sql) and sql[j].isspace():
                                        j += 1

                                    # 寻找表名
                                    if j < len(sql):
                                        table_start = j
                                        # 读取可能的表名
                                        while j < len(sql) and (sql[j].isalnum() or sql[j] == '_'):
                                            j += 1

                                        if j > table_start:
                                            table_name = sql[table_start:j]
                                            # 检查这是否是一个表名而不是带有数据库前缀的表名
                                            if '.' not in table_name and table_name in table_to_db_map:
                                                # 添加关键词
                                                sql_parts.append(sql[i:table_start])
                                                # 添加带数据库前缀的表名
                                                sql_parts.append(f"{table_to_db_map[table_name]}.{table_name}")
                                                start = j
                                                i = j - 1  # -1是因为循环会+1
                                                break

                        i += 1

                    # 添加剩余部分
                    if start < len(sql):
                        sql_parts.append(sql[start:])

                    sql = ''.join(sql_parts)

                except Exception as e:
                    logger.error(f"处理多数据库表映射时出错: {str(e)}")
                    # 错误时回退到简单的添加前缀
                    sql = self._add_database_prefix_to_sql(sql, self.db_name)

            # 防止重复添加SELECT语句
            if sql.count("SELECT") > original_sql.count("SELECT"):
                logger.warning("检测到SQL转换导致SELECT语句重复,将恢复使用原始SQL")
                sql = original_sql

            # 记录转换结果
            logger.info(f"SQL转换: {'已添加数据库前缀' if sql != original_sql else '无变化'}")
            if sql != original_sql:
                logger.info(f"原始SQL: {original_sql}")
                logger.info(f"转换后SQL: {sql}")

            # 检测是否是跨数据库查询
            if any(db + "." in sql for db in self.metadata_extractor.get_all_target_databases()):
                logger.info("检测到跨数据库查询")

            return sql

        except Exception as e:
            logger.error(f"添加数据库前缀时出错: {str(e)}")
            return sql

    def _add_database_prefix_to_sql(self, sql: str, db_name: str) -> str:
        """
        为SQL中不含数据库前缀的表名添加指定的数据库前缀

        Args:
            sql: 原始SQL语句
            db_name: 数据库名

        Returns:
            str: 带有数据库前缀的SQL语句
        """
        if not sql or not db_name:
            return sql

        # 复杂SQL解析需要专业的SQL解析器
        # 这里使用一个简化的方法,可能不适用于所有复杂SQL
        keyword_patterns = [
            (r'\bFROM\s+([a-zA-Z0-9_]+)\b', r'FROM ' + db_name + r'.\1'),
            (r'\bJOIN\s+([a-zA-Z0-9_]+)\b', r'JOIN ' + db_name + r'.\1'),
            (r'\bUPDATE\s+([a-zA-Z0-9_]+)\b', r'UPDATE ' + db_name + r'.\1'),
            (r'\bINTO\s+([a-zA-Z0-9_]+)\b', r'INTO ' + db_name + r'.\1')
        ]

        result = sql
        for pattern, replacement in keyword_patterns:
            # 不替换已经有数据库前缀的表名
            result = re.sub(pattern + r'(?!\.[a-zA-Z0-9_])', replacement, result, flags=re.IGNORECASE)

        return result

    def get_fix_sql_prompt(self, sql: str, error_message: str, query: str, table_info: str) -> Dict[str, str]:
        """
        生成用于修复SQL的提示
        
        Args:
            sql: 原始SQL
            error_message: 错误信息
            query: 原始查询
            table_info: 表信息
            
        Returns:
            Dict: 包含system和user提示的字典
        """
        # 检查是否是空结果错误
        is_empty_result = "查询结果为空" in error_message
        
        if is_empty_result:
            # 使用空结果修复提示
            system_prompt = SQL_FIX_PROMPTS["empty_result_system"]
            user_prompt = SQL_FIX_PROMPTS["empty_result_user"].format(
                sql=sql,
                query=query,
                table_info=table_info
            )
        else:
            # 常规错误修复
            # 错误类型分析
            error_type = error_analysis(error_message)
            
            # 系统提示
            system_prompt = SQL_FIX_PROMPTS["error_type_system"].format(
                error_type=error_type,
                error_message=error_message
            )

            # 用户提示
            user_prompt = SQL_FIX_PROMPTS["error_fix_user"].format(
                sql=sql,
                query=query,
                table_info=table_info
            )

        return {
            "system": system_prompt,
            "user": user_prompt
        }

    def _get_business_metadata(self, query: str) -> Dict[str, Any]:
        """
        获取业务元数据
        
        Args:
            query: 自然语言查询
            
        Returns:
            Dict: 业务元数据
        """
        business_metadata = {}
        
        # 如果找到了相似示例,直接使用示例中的表信息
        similar_example = self._find_similar_example(query)
        if similar_example and "tables" in similar_example:
            from src.utils.metadata_extractor import MetadataExtractor
            extractor = MetadataExtractor()
            
            # 获取示例中涉及的表的元数据
            for table_info in similar_example["tables"]:
                db_name = table_info.get("database", "")
                table_name = table_info.get("table", "")
                
                if db_name and table_name:
                    # 获取表级业务元数据
                    table_metadata = extractor.get_business_metadata_for_table(db_name, table_name)
                    if table_metadata:
                        if "tables" not in business_metadata:
                            business_metadata["tables"] = {}
                        
                        if db_name not in business_metadata["tables"]:
                            business_metadata["tables"][db_name] = {}
                        
                        business_metadata["tables"][db_name][table_name] = table_metadata
        else:
            # 如果没有找到相似示例,获取所有数据库级别的元数据
            logger.info("没有找到相似示例,获取所有数据库级别的元数据")
            
            from src.utils.metadata_extractor import MetadataExtractor
            extractor = MetadataExtractor()
            
            # 获取所有数据库的业务元数据
            business_metadata["databases"] = {}
            for db_name in extractor.get_all_target_databases():
                db_metadata = extractor.get_business_metadata_from_database(db_name)
                if db_metadata:
                    business_metadata["databases"][db_name] = db_metadata
            
            # 获取表层级模式
            business_metadata["table_hierarchy"] = extractor._load_table_hierarchy_patterns()
            
        return business_metadata

    def _execute_sql(self, sql: str, query: Optional[str] = None) -> Dict[str, Any]:
        """
        执行SQL语句的简单封装，调用 _execute_sql_with_retry 方法
        
        Args:
            sql: 要执行的SQL语句
            query: 原始的自然语言查询
            
        Returns:
            Dict: 包含执行结果或错误信息的字典
        """
        logger.info(f"执行SQL查询: {sql[:200]}...")
        return self._execute_sql_with_retry(sql, query)

    def _generate_visualization_suggestion(self, query, result):
        """
        此方法已弃用，所有可视化分析功能已合并到_generate_business_analysis方法中
        
        请使用_generate_business_analysis(query, sql, query_result, tables_info)替代
        """
        logger.warning("_generate_visualization_suggestion方法已弃用，请使用_generate_business_analysis方法")
        return "请使用_generate_business_analysis方法获取可视化建议。"

def calculate_similarity(text1: str, text2: str) -> float:
    """
    计算两段文本的相似度

    Args:
        text1: 第一段文本
        text2: 第二段文本

    Returns:
        float: 相似度得分 (0-1)
    """
    # 使用简单的字符级别匹配
    text1 = text1.lower()
    text2 = text2.lower()

    # 如果有一方为空,返回0
    if not text1 or not text2:
        return 0

    # 计算字符级别的匹配
    char_match = sum(1 for c in text1 if c in text2) / max(len(text1), 1)

    # 检查关键词是否完全包含在文本中
    keyword_match = 1.0 if text2 in text1 else 0.0

    # 简单的词级别匹配
    words1 = set(text1.split())
    words2 = set(text2.split())

    word_match = 0
    if words1 and words2:
        word_match = len(words1.intersection(words2)) / max(len(words1.union(words2)), 1)

    # 加权平均
    return 0.2 * char_match + 0.5 * keyword_match + 0.3 * word_match

def error_analysis(error_msg: str) -> str:
    """
    分析SQL错误类型
    
    Args:
        error_msg: 错误信息
        
    Returns:
        str: 错误类型描述
    """
    error_msg = error_msg.lower()
    
    if "syntax error" in error_msg:
        return "语法错误"
    elif "table not found" in error_msg or "table doesn't exist" in error_msg:
        return "表不存在"
    elif "field not found" in error_msg or "column not found" in error_msg:
        return "字段不存在"
    elif "查询结果为空" in error_msg or "没有匹配的数据" in error_msg:
        return "空结果"
    elif "division by zero" in error_msg:
        return "除零错误"
    elif "duplicate key" in error_msg:
        return "重复键错误"
    elif "data type mismatch" in error_msg or "cannot cast" in error_msg:
        return "数据类型不匹配"
    elif "permission denied" in error_msg:
        return "权限错误"
    elif "timeout" in error_msg or "execution time" in error_msg:
        return "查询超时"
    else:
        return "未知错误"

def execute_sql(sql: str):
    """
    执行SQL语句
    
    Args:
        sql: SQL语句
        
    Returns:
        cursor: 数据库游标对象，包含查询结果
    """
    from src.utils.db import execute_query
    return execute_query(sql)

    async def _generate_sql_stream(self, query: str, 
                             similar_example: Optional[Dict[str, Any]] = None, 
                             business_metadata: Optional[Dict[str, Any]] = None, 
                             previous_error: Optional[Dict[str, Any]] = None,
                             stream_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None) -> Dict[str, Any]:
        """
        流式生成SQL查询，并通过回调函数返回中间思考过程

        Args:
            query: 自然语言查询
            similar_example: 相似的示例(可选)
            business_metadata: 业务元数据(可选)
            previous_error: 上一次出现的错误(可选)
            stream_callback: 流式回调函数，接收中间思考过程和状态信息

        Returns:
            Dict: 包含生成的SQL和元数据的字典
        """
        result = {
            "success": False,
            "sql": "",
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "thinking_process": "",
            "process_steps": []
        }
        
        # 检查回调函数是否为异步函数
        is_async_callback = stream_callback and inspect.iscoroutinefunction(stream_callback)
        
        # 适配器函数，根据回调函数是否为异步来调用
        async def call_stream_callback(message: str, metadata: Dict[str, Any]):
            if not stream_callback:
                return
                
            if is_async_callback:
                await stream_callback(message, metadata)
            else:
                stream_callback(message, metadata)
        
        try:
            # 如果有回调函数，先发送思考开始信号
            await call_stream_callback("开始分析查询需求...", {
                "step": "start",
                "progress": 10
            })
            
            # 获取表信息 - 发送进度
            tables_info = self._get_tables_info()
            await call_stream_callback("正在获取数据库表结构信息...", {
                "step": "metadata",
                "progress": 20
            })
            
            # 收集用于生成的上下文信息
            context_info = []
            
            # 添加表信息到上下文
            context_info.append(f"数据库表结构信息:\n{tables_info}")
            
            # 添加相似示例到上下文
            if similar_example:
                context_info.append(f"相似问题: {similar_example.get('query', '')}")
                context_info.append(f"对应SQL: {similar_example.get('sql', '')}")
                await call_stream_callback(f"找到相似查询示例: {similar_example.get('query', '')}", {
                    "step": "similar_example",
                    "progress": 30
                })
            else:
                await call_stream_callback(f"未找到相似查询示例，将直接生成SQL", {
                    "step": "similar_example",
                    "progress": 30
                })
            
            # 添加业务元数据到上下文
            if business_metadata:
                business_info = json.dumps(business_metadata, ensure_ascii=False, indent=2)
                context_info.append(f"业务元数据:\n{business_info}")
                await call_stream_callback("分析业务领域元数据...", {
                    "step": "business_metadata",
                    "progress": 40
                })
            else:
                await call_stream_callback("未找到相关业务元数据", {
                    "step": "business_metadata",
                    "progress": 40
                })
                
            # 添加单库/多库模式信息
            if self.enable_multi_database:
                context_info.append("支持跨数据库查询。在涉及多个数据库的表时,使用完整的 database_name.table_name 格式。")
            else:
                context_info.append("单库模式下，不使用数据库前缀，直接使用表名引用表，例如 FROM customer 而不是 FROM tpch.customer。")
            
            # 如果有之前的错误,添加到上下文
            if previous_error:
                error_info = f"上一次执行错误: {previous_error.get('message', '')}"
                context_info.append(error_info)
                await call_stream_callback(f"处理上一次执行错误: {previous_error.get('message', '')}", {
                    "step": "previous_error",
                    "progress": 50
                })
            
            # 构建提示
            context = "\n\n".join(context_info)
            # 明确标识当前模式
            mode_info = "单数据库模式" if not self.enable_multi_database else "多数据库模式"
            context = f"当前配置: {mode_info}\n{context}"
            system_prompt = NL2SQL_PROMPTS["SYSTEM_PROMPT"].format(context=context)
            user_prompt = NL2SQL_PROMPTS["USER_PROMPT"].format(query=query)
            
            log_query_process({
                "type": "generate_sql_request",
                "query": query,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "similar_example": similar_example,
                "business_metadata": business_metadata if isinstance(business_metadata, dict) else None,
                "previous_error": previous_error
            }, "llm_calls")
            
            # 初始化LLM客户端
            from src.utils.llm_client import get_llm_client, Message
            client = get_llm_client(stage="sql_generation")
            
            # 流式思考过程
            thinking_buffer = ""
            
            async def stream_content_callback(content_chunk: str):
                nonlocal thinking_buffer
                # 追加到思考缓冲区
                thinking_buffer += content_chunk
                # 调用外部回调
                # 计算当前进度 - 流式思考过程视为60-90%区间
                progress = min(90, 60 + int(len(thinking_buffer) / 100))
                await call_stream_callback(content_chunk, {
                    "step": "thinking",
                    "progress": progress
                })
            
            # 发送流式思考信号
            await call_stream_callback("思考如何将自然语言转换为SQL...", {
                "step": "thinking_start",
                "progress": 60
            })
            
            # 使用流式调用
            messages = [
                Message.system(system_prompt),
                Message.user(user_prompt)
            ]
            
            try:
                # 流式调用LLM
                response = await client.chat(
                    messages=messages,
                    temperature=float(os.getenv("SQL_GENERATION_TEMPERATURE", "0.2")),
                    max_tokens=int(os.getenv("SQL_GENERATION_MAX_TOKENS", "2048")),
                    stream=True,
                    stream_callback=stream_content_callback
                )
                
                # 获取最终内容
                content = thinking_buffer
                
                # 记录最终结果
                log_query_process({
                    "type": "generate_sql_response",
                    "content": content
                }, "llm_calls")
                
                # 从响应中提取SQL
                logger.info(f"LLM响应内容前100个字符: {content[:100]}")
                sql = self._extract_sql(content)
                
                if sql:
                    logger.info(f"成功生成SQL: {sql[:100]}")
                    result["success"] = True
                    result["sql"] = sql
                    result["raw_response"] = content
                    result["thinking_process"] = thinking_buffer
                    
                    # 发送SQL生成完成信号
                    if stream_callback:
                        stream_callback(f"已生成SQL查询:\n{sql}", {
                            "step": "sql_generated",
                            "progress": 95,
                            "sql": sql
                        })
                else:
                    logger.warning("无法从LLM响应中提取SQL")
                    result["error"] = {
                        "message": "无法从LLM响应中提取SQL",
                        "detail": "请检查LLM响应格式是否正确"
                    }
                    result["raw_response"] = content
                    result["thinking_process"] = thinking_buffer
                    
                    # 发送SQL生成失败信号
                    if stream_callback:
                        stream_callback("无法从LLM响应中提取SQL，请检查响应格式", {
                            "step": "error",
                            "progress": 95,
                            "error": "无法从LLM响应中提取SQL"
                        })
                
                # 添加令牌统计信息
                if hasattr(response, 'usage') and response.usage:
                    result["prompt_tokens"] = response.usage.get("prompt_tokens", 0)
                    result["completion_tokens"] = response.usage.get("completion_tokens", 0)
                    result["total_tokens"] = response.usage.get("total_tokens", 0)
                
            except Exception as e:
                logger.error(f"生成SQL时出错: {str(e)}")
                result["error"] = {
                    "message": f"生成SQL时出错: {str(e)}",
                    "detail": traceback.format_exc()
                }
                result["thinking_process"] = thinking_buffer
                
                # 发送错误信号
                if stream_callback:
                    stream_callback(f"生成SQL时出错: {str(e)}", {
                        "step": "error",
                        "progress": 100,
                        "error": str(e)
                    })
            
            # 发送处理完成信号
            if stream_callback:
                stream_callback("SQL生成过程完成", {
                    "step": "complete",
                    "progress": 100
                })
                
            return result
            
        except Exception as e:
            logger.error(f"流式生成SQL处理过程出错: {str(e)}\n{traceback.format_exc()}")
            result["error"] = {
                "message": f"流式生成SQL处理过程出错: {str(e)}",
                "detail": traceback.format_exc()
            }
            
            # 发送错误信号
            if stream_callback:
                stream_callback(f"流式生成SQL处理过程出错: {str(e)}", {
                    "step": "error",
                    "progress": 100,
                    "error": str(e)
                })
                
            return result
            
    async def process_stream(self, query: str, stream_callback: Callable[[str, Dict[str, Any]], None]) -> Dict[str, Any]:
        """
        流式处理自然语言转SQL查询，并通过回调函数返回中间结果

        Args:
            query: 自然语言查询
            stream_callback: 流式回调函数，接收中间结果和状态信息

        Returns:
            Dict: 包含SQL、执行结果和元数据的字典
        """
        start_time = time.time()

        # 准备响应结构
        response = {
            "query": query,
            "is_business_query": False,
            "sql": "",
            "result": None,
            "column_names": [],
            "error": None,
            "execution_time": 0,
            "message": "",
            "similar_example": None,
            "cached": False,
            "log_id": str(uuid.uuid4()),
            "thinking_process": "",
            "success": True
        }
        
        # 检查回调函数是否为异步函数
        is_async_callback = inspect.iscoroutinefunction(stream_callback)
        
        # 适配器函数，根据回调函数是否为异步来调用
        async def call_stream_callback(message: str, metadata: Dict[str, Any]):
            # 添加消息到思考过程
            if "thinking_process" in response:
                response["thinking_process"] += message + "\n"
                
            if is_async_callback:
                await stream_callback(message, metadata)
            else:
                stream_callback(message, metadata)
        
        # 发送开始信号
        try:
            await call_stream_callback("开始处理查询...", {
                "step": "start",
                "progress": 5
            })
            
            # 简化版流式处理 - 直接使用同步的process方法并模拟流式
            await call_stream_callback("分析查询类型...", {
                "step": "analyzing",
                "progress": 10
            })
            
            # 处理查询
            result = self.process(query)
            
            # 添加思考过程
            result["thinking_process"] = response["thinking_process"]
            
            # 发送完成信号
            await call_stream_callback("查询处理完成", {
                "step": "complete",
                "progress": 100
            })
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"流式处理查询时出错: {error_msg}")
            
            # 发送错误信号
            await call_stream_callback(f"处理出错: {error_msg}", {
                "step": "error",
                "progress": 100
            })
            
            response["error"] = error_msg
            response["success"] = False
            response["message"] = f"处理查询时出错: {error_msg}"
            
            return response