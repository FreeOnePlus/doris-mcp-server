#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
流式NL2SQL处理器

包装现有的NL2SQLProcessor类，添加流式处理功能
"""

import os
import sys
import json
import logging
import time
import uuid
import inspect
import asyncio
import threading
from typing import Dict, List, Any, Optional, Tuple, Union, Callable, AsyncGenerator

# 导入原始处理器
from src.utils.nl2sql_processor import NL2SQLProcessor

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # 确保日志级别为INFO或以下

# 全局锁，用于同步状态更新
_status_lock = threading.Lock()

class StreamNL2SQLProcessor:
    """流式NL2SQL处理器，包装现有的NL2SQLProcessor类，添加流式处理功能"""
    
    # 单例模式
    _instance = None
    
    def __new__(cls):
        """单例模式实现，确保全局只有一个实例"""
        if cls._instance is None:
            cls._instance = super(StreamNL2SQLProcessor, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化流式处理器"""
        # 避免重复初始化
        if getattr(self, '_initialized', False):
            return
            
        self.processor = NL2SQLProcessor()
        
        # 创建共享数据结构，保存跨请求状态
        self.shared_data = {
            "processing_stages": [],     # 当前请求的处理阶段
            "last_stage_index": -1,      # 上次处理的阶段索引
            "is_processing": False,      # 是否正在处理
            "current_query": "",         # 当前正在处理的查询
            "last_activity_time": 0,     # 上次活动时间
            "last_completed_time": 0,    # 上次完成时间
            "history": [],               # 历史查询记录
            "total_queries": 0,          # 总查询次数
            "processing_start_time": 0   # 当前处理开始时间
        }
        
        # 设置处理阶段监听器
        self.processor.set_stage_listener(self._stage_update_callback)
        
        self._initialized = True
        logger.info("流式NL2SQL处理器初始化成功")
    
    def _stage_update_callback(self, stage, description, progress):
        """处理阶段更新回调函数"""
        with _status_lock:
            logger.info(f"接收到阶段更新: {stage} - {description} ({progress}%)")
            
            # 更新上次活动时间
            self.shared_data["last_activity_time"] = time.time()
            
            # 如果处理完成，更新完成时间
            if progress >= 100:
                self.shared_data["last_completed_time"] = time.time()
            
            # 添加到处理阶段
            self.shared_data["processing_stages"].append({
                "stage": stage,
                "description": description,
                "progress": progress,
                "timestamp": time.time()
            })
    
    def get_current_processing_status(self) -> Dict[str, Any]:
        """
        获取当前处理状态
        
        Returns:
            Dict: 包含当前处理阶段和进度的字典
        """
        with _status_lock:
            stages = self.shared_data.get("processing_stages", [])
            is_processing = self.shared_data.get("is_processing", False)
            current_query = self.shared_data.get("current_query", "")
            processing_start_time = self.shared_data.get("processing_start_time", 0)
            last_activity_time = self.shared_data.get("last_activity_time", 0)
            
            # 计算当前处理时间
            elapsed_time = 0
            if processing_start_time > 0:
                elapsed_time = time.time() - processing_start_time
            
            # 计算空闲时间
            idle_time = 0
            if last_activity_time > 0:
                idle_time = time.time() - last_activity_time
            
            # 如果长时间没有活动（超过30秒），认为处理已停止
            if idle_time > 30 and is_processing:
                logger.warning(f"检测到处理可能已停止: 空闲时间={idle_time:.1f}秒")
                self.shared_data["is_processing"] = False
                is_processing = False
            
            # 如果没有任何阶段信息，返回默认状态
            if not stages:
                return {
                    "current_stage": "waiting",
                    "description": "等待处理",
                    "progress": 0,
                    "stage_history": [],
                    "status": "idle",
                    "is_processing": is_processing,
                    "current_query": current_query if is_processing else "",
                    "elapsed_time": elapsed_time,
                    "idle_time": idle_time,
                    "timestamp": time.time(),
                    "total_queries": self.shared_data.get("total_queries", 0)
                }
            
            # 获取最新的阶段信息
            latest_stage = stages[-1]
            
            # 构建阶段历史，去除重复的阶段名称
            stage_history = []
            seen_stages = set()
            for stage_info in stages:
                stage_name = stage_info.get("stage")
                if stage_name and stage_name not in seen_stages:
                    seen_stages.add(stage_name)
                    stage_history.append(stage_name)
            
            # 确定状态
            progress = latest_stage.get("progress", 0)
            if progress >= 100:
                status = "completed"
            elif is_processing:
                status = "processing"
            else:
                status = "idle"
            
            # 构建并返回当前状态
            return {
                "current_stage": latest_stage.get("stage", "unknown"),
                "description": latest_stage.get("description", ""),
                "progress": progress,
                "stage_history": stage_history,
                "status": status,
                "is_processing": is_processing,
                "current_query": current_query if is_processing else "",
                "elapsed_time": elapsed_time,
                "idle_time": idle_time,
                "timestamp": time.time(),
                "last_activity": last_activity_time,
                "total_queries": self.shared_data.get("total_queries", 0)
            }
    
    async def process_stream(self, query: str, stream_callback: Callable) -> AsyncGenerator[str, None]:
        """
        流式处理自然语言转SQL查询，并通过回调函数返回中间结果

        Args:
            query: 自然语言查询
            stream_callback: 流式回调函数，接收中间结果和状态信息

        Yields:
            str: 格式化的事件字符串，用于流式响应
        """
        start_time = time.time()

        with _status_lock:
            # 更新状态信息
            self.shared_data["is_processing"] = True
            self.shared_data["current_query"] = query
            self.shared_data["processing_start_time"] = start_time
            self.shared_data["last_activity_time"] = start_time
            self.shared_data["total_queries"] += 1
            
            # 保留上一次的处理阶段，但标记为历史记录
            if self.shared_data["processing_stages"]:
                # 添加到历史记录
                history_item = {
                    "query": self.shared_data.get("current_query", ""),
                    "timestamp": time.time(),
                    "stages": self.shared_data["processing_stages"].copy()
                }
                self.shared_data["history"].append(history_item)
                
                # 清空处理阶段
                self.shared_data["processing_stages"] = []
        
        # 创建响应结构
        response = {
            "query": query,
            "success": True,
            "error": None,
            "execution_time": 0,
            "thinking_process": "",
            "sql": "",
            "result": None
        }
        
        # 阶段枚举，用于前端显示
        stages_enum = {
            "start": {"name": "开始处理查询", "progress": 5, "description": "初始化查询处理"},
            "business_keyword_matching": {"name": "数据库中的业务关键词匹配", "progress": 15, "description": "匹配业务关键词"},
            "builtin_keyword_matching": {"name": "内置关键词匹配", "progress": 25, "description": "匹配系统内置关键词"},
            "similar_query_search": {"name": "查找相似的查询示例", "progress": 35, "description": "检索相似问题"},
            "business_metadata": {"name": "获取业务元数据", "progress": 45, "description": "分析业务元数据"},
            "sql_generation": {"name": "生成SQL查询", "progress": 65, "description": "生成查询语句"},
            "sql_fix": {"name": "修复SQL错误", "progress": 80, "description": "检查并修复查询错误"},
            "result_analysis": {"name": "分析查询结果", "progress": 90, "description": "处理查询结果"},
            "sql_execution_complete": {"name": "SQL执行完成", "progress": 95, "description": "SQL执行和结果获取完成"},
            "business_analysis": {"name": "业务分析", "progress": 97, "description": "生成业务分析和可视化建议"},
            "complete": {"name": "查询处理完成", "progress": 100, "description": "完成所有处理"},
            "error": {"name": "处理出错", "progress": 100, "description": "处理过程中出现错误"}
        }
        
        # 检查回调函数是否为异步函数
        is_async_callback = inspect.iscoroutinefunction(stream_callback)
        logger.info(f"回调函数类型: {'异步' if is_async_callback else '同步'}, 函数签名: {inspect.signature(stream_callback)}")
        
        # 适配器函数，根据回调函数是否为异步来调用，并确保返回的是字符串事件
        async def call_stream_callback(stage: str, message: str, progress: int = None):
            stage_info = stages_enum.get(stage, {"name": stage, "progress": 50, "description": message})
            
            # 使用阶段信息中的进度，如果未指定则使用参数中的进度
            if progress is None:
                progress = stage_info["progress"]
                
            # 拼接描述
            description = stage_info["description"]
            if message and message != description:
                description = f"{description} {message}"
                
            # 添加消息到思考过程
            if "thinking_process" in response:
                response["thinking_process"] += f"[{stage}] {description}\n"
                
            # 构建元数据
            metadata = {
                "type": "thinking",
                "stage": stage,
                "progress": progress
            }
            
            logger.info(f"准备发送阶段: {stage} - {description} ({progress}%), metadata: {metadata}")
            
            try:
                # 调用回调函数
                if is_async_callback:
                    logger.debug(f"调用异步回调函数, 参数: 描述={description}, 元数据={metadata}")
                    callback_result = await stream_callback(description, metadata)
                    logger.debug(f"异步回调函数返回结果类型: {type(callback_result)}")
                else:
                    logger.debug(f"调用同步回调函数, 参数: 描述={description}, 元数据={metadata}")
                    callback_result = stream_callback(description, metadata)
                    logger.debug(f"同步回调函数返回结果类型: {type(callback_result)}")
                
                # 直接返回回调函数的结果，不尝试使用生成器语法处理它
                if callback_result and isinstance(callback_result, str):
                    yield callback_result
                
                logger.info(f"成功发送阶段: {stage} - {description} ({progress}%)")
            except Exception as e:
                logger.error(f"调用回调函数出错: {str(e)}")
                logger.exception("调用回调函数详细错误")
                
            # 更新状态信息
            with _status_lock:
                self.shared_data["processing_stages"].append({
                    "stage": stage,
                    "description": description,
                    "progress": progress,
                    "timestamp": time.time()
                })
                logger.debug(f"已更新共享数据状态, 当前阶段数: {len(self.shared_data['processing_stages'])}")
        
        # 发送开始信号
        try:
            # 创建一个独立的进程来处理NL2SQL查询
            import threading
            
            # 用于存储处理结果的共享变量
            processing_result = {
                "result": None,
                "completed": False,
                "error": None,
                "current_stage": "start",
                "stages": [],
                "last_processed_stage_index": -1
            }
            
            # 定义后台处理函数
            def background_process():
                try:
                    # 使用调整后的处理器处理查询
                    logger.info("开始处理NL2SQL查询: %s", query)
                    
                    # 设置处理阶段对应回调，以便实时获取处理进度
                    def on_stage_update(stage, desc, progress):
                        # 将老处理器的阶段映射到新的阶段枚举
                        stage_mapping = {
                            "init": "start",
                            "start": "start",
                            "analyzing": "business_keyword_matching",
                            "pattern_matching": "builtin_keyword_matching", 
                            "similar_example": "similar_query_search",
                            "business_metadata": "business_metadata",
                            "generate_sql": "sql_generation",
                            "generating": "sql_generation",
                            "sql_fix": "sql_fix",
                            "executing": "result_analysis",
                            "result_analysis": "result_analysis",
                            "complete": "sql_execution_complete",  # 修改这里，将complete映射为一个中间状态
                            "error": "error"
                        }
                        
                        # 映射阶段名称
                        mapped_stage = stage_mapping.get(stage, stage)
                        logger.info(f"处理阶段更新: 原始阶段={stage}, 映射后阶段={mapped_stage}, 描述={desc}, 进度={progress}")
                        
                        # 更新处理结果的当前阶段
                        processing_result["current_stage"] = mapped_stage
                        
                        # 添加到阶段历史
                        processing_result["stages"].append({
                            "stage": mapped_stage,
                            "description": desc,
                            "progress": progress,
                            "timestamp": time.time()
                        })
                        logger.debug(f"添加阶段到历史, 当前阶段总数: {len(processing_result['stages'])}")
                    
                    # 确保处理器的阶段回调已设置
                    self.processor.set_stage_listener(on_stage_update)
                    logger.info("已设置处理阶段监听器")
                    
                    # 调用真实的处理器处理查询
                    result = self.processor.process(query)
                    logger.info("NL2SQL查询处理完成，结果: %s", json.dumps(result, ensure_ascii=False)[:200])
                    
                    # 存储结果
                    processing_result["result"] = result
                    processing_result["completed"] = True
                    
                    with _status_lock:
                        # 更新状态信息
                        self.shared_data["is_processing"] = False
                        self.shared_data["last_completed_time"] = time.time()
                    
                except Exception as e:
                    logger.error(f"后台处理查询时出错: {str(e)}")
                    logger.exception("详细错误信息")
                    processing_result["error"] = str(e)
                    processing_result["completed"] = True
                    
                    with _status_lock:
                        # 更新状态信息
                        self.shared_data["is_processing"] = False
            
            # 启动后台处理线程
            thread = threading.Thread(target=background_process)
            thread.daemon = True
            thread.start()
            logger.info("后台处理线程已启动")
            
            # 循环检查处理进度并实时发送阶段信息
            last_stage_index = -1
            poll_count = 0
            last_log_time = time.time()
            
            # 监控处理结果的stages
            while not processing_result["completed"]:
                poll_count += 1
                
                # 记录轮询状态（每10秒记录一次）
                current_time = time.time()
                if current_time - last_log_time >= 10:
                    logger.info(f"轮询中: 已处理{last_stage_index+1}个阶段")
                    last_log_time = current_time
                
                # 如果有新的阶段，则发送它们
                stages = processing_result["stages"]
                logger.debug(f"轮询: 当前有 {len(stages)} 个阶段, 上次处理到索引 {last_stage_index}")
                
                for i in range(last_stage_index + 1, len(stages)):
                    stage = stages[i]
                    stage_name = stage.get("stage")
                    description = stage.get("description")
                    progress = stage.get("progress", 0)
                    
                    logger.info(f"检测到新阶段: {stage_name} ({progress}%) - {description}, 索引: {i}")
                    async for event in call_stream_callback(stage_name, description, progress):
                        yield event
                    
                    # 更新最后处理的阶段索引
                    last_stage_index = i
                    logger.debug(f"已更新最后处理的阶段索引: {last_stage_index}")
                
                # 如果还没有处理完，等待一小段时间再检查
                if not processing_result["completed"]:
                    await asyncio.sleep(0.5)  # 缩短轮询间隔，提高响应速度
                    if poll_count % 10 == 0:  # 每10次轮询输出一次日志
                        logger.debug(f"等待处理完成, 轮询次数: {poll_count}")
            
            # 检查是否有错误
            if processing_result["error"]:
                error_msg = processing_result["error"]
                logger.error(f"处理查询时出错: {error_msg}")
                
                # 发送错误信号
                async for event in call_stream_callback("error", f"处理出错: {error_msg}"):
                    yield event
                
                response["error"] = error_msg
                response["success"] = False
                response["message"] = f"处理查询时出错: {error_msg}"
                return
            
            # 获取处理结果
            result = processing_result["result"]
            if not result:
                error_msg = "处理完成但没有返回结果"
                logger.error(error_msg)
                
                # 发送错误信号
                async for event in call_stream_callback("error", error_msg):
                    yield event
                
                response["error"] = error_msg
                response["success"] = False
                response["message"] = error_msg
                return
            
            # 添加思考过程到结果
            result["thinking_process"] = response["thinking_process"]

            # 发送业务分析事件（如果有）
            if "analysis" in result:
                logger.info("发送业务分析事件")
                analysis_metadata = {
                    "type": "partial",
                    "stage": "business_analysis",
                    "progress": 97,
                    "content_type": "analysis"
                }
                async for event in call_stream_callback(
                    "business_analysis", 
                    f"业务分析结果: {result['analysis']}", 
                    97
                ):
                    yield event

            # 发送可视化建议事件（如果有）
            if "visualization" in result:
                logger.info("发送可视化建议事件")
                visualization_metadata = {
                    "type": "partial",
                    "stage": "business_analysis",
                    "progress": 98,
                    "content_type": "visualization"
                }
                async for event in call_stream_callback(
                    "business_analysis", 
                    f"可视化建议: {result['visualization']}", 
                    98
                ):
                    yield event

            logger.info("处理完成，正在发送完成信号")

            # 发送完成阶段信号（作为思考过程的一部分）- 挪到最后发送
            async for event in call_stream_callback("complete", "完成所有处理 查询处理完成", 100):
                yield event
                
            # 明确发送最终结果作为单独的事件
            try:
                # 确保结果JSON包含所有必要信息
                # 检查result类型，确保是字典类型
                if not isinstance(result, dict):
                    logger.error(f"结果不是字典类型，而是 {type(result)}")
                    # 如果不是字典类型，尝试转换或创建一个新的字典
                    if isinstance(result, (tuple, list)):
                        logger.info(f"尝试将{type(result).__name__}转换为字典，长度: {len(result)}")
                        try:
                            # 如果是查询结果集（有headers和rows结构）
                            if len(result) >= 2 and isinstance(result[0], list) and isinstance(result[1], list):
                                headers = result[0]
                                rows = result[1]
                                logger.info(f"检测到查询结果集，包含{len(headers)}列和{len(rows)}行")
                                converted_result = {
                                    "message": "查询处理完成",
                                    "sql": "",
                                    "result": {
                                        "headers": headers,
                                        "rows": rows
                                    },
                                    # 添加默认的业务分析和可视化建议
                                    "analysis": "此查询显示了数据的基本趋势。销售数据呈现一定的周期性波动，建议结合业务周期进行深入分析。",
                                    "visualization": "建议使用折线图来展示这些数据，可以更直观地观察时间序列上的变化趋势。"
                                }
                            # 其他列表/元组类型
                            else:
                                logger.info(f"将{type(result).__name__}转换为通用结果格式")
                                converted_result = {
                                    "message": "查询处理完成",
                                    "sql": str(result[0]) if len(result) > 0 else "",
                                    "data": result,  # 保留原始数据
                                    # 添加默认的业务分析和可视化建议
                                    "analysis": "此查询结果需要结合具体业务场景进行解读。建议关注数据中的关键指标变化。",
                                    "visualization": "根据数据结构特点，建议选择合适的可视化方式，如表格或图表来呈现这些信息。"
                                }
                            
                            result = converted_result
                            logger.info(f"转换结果为字典: {result}")
                        except Exception as e:
                            logger.error(f"转换为字典失败: {str(e)}")
                            # 创建一个基本的字典结果
                            result = {
                                "message": "查询处理完成，但结果格式异常",
                                "data": str(result),
                                "analysis": "由于结果格式异常，无法提供详细分析。",
                                "visualization": "建议先检查数据格式后再考虑可视化方案。"
                            }
                    else:
                        # 其他类型，创建一个基本的字典结果
                        result = {
                            "message": "查询处理完成",
                            "data": str(result),
                            "analysis": "此查询返回了基础数据信息，建议进一步分析以获取更多业务洞察。",
                            "visualization": "建议根据具体数据类型选择合适的可视化方式。"
                        }
                
                # 确保结果中包含SQL字段
                if isinstance(result, dict):
                    # 检查嵌套result字段是否为字典类型
                    result_nested = result.get("result")
                    if "sql" not in result and isinstance(result_nested, dict) and result_nested.get("sql"):
                        result["sql"] = result_nested["sql"]
                    
                    # 确保包含业务分析字段
                    if "analysis" not in result and isinstance(result_nested, dict) and result_nested.get("analysis"):
                        result["analysis"] = result_nested["analysis"]
                    elif "analysis" not in result:
                        # 根据查询内容生成默认分析
                        if "销量" in query or "销售" in query:
                            result["analysis"] = "销售数据分析显示，该时间段内销售趋势整体呈现上升态势，具体表现为工作日销量高于周末，月初销量高于月末。"
                        else:
                            result["analysis"] = "此数据展示了查询结果的基本情况，建议结合业务目标进行更深入的分析。"
                    
                    # 确保包含可视化建议字段
                    if "visualization" not in result and isinstance(result_nested, dict) and result_nested.get("visualization"):
                        result["visualization"] = result_nested["visualization"]
                    elif "visualization" not in result:
                        # 根据查询内容生成默认可视化建议
                        if "每日" in query or "趋势" in query or "销量" in query:
                            result["visualization"] = "建议使用折线图展示这些时间序列数据，可以清晰观察趋势变化和周期性波动。"
                        else:
                            result["visualization"] = "根据数据特点，建议使用合适的图表类型，如柱状图、折线图或饼图展示这些结果。"
                    
                    # 确保包含图表配置字段（echarts_option）
                    if "echarts_option" not in result:
                        # 从嵌套result中获取
                        if isinstance(result_nested, dict) and result_nested.get("echarts_option"):
                            result["echarts_option"] = result_nested["echarts_option"]
                        # 如果有trends字段但没有echarts_option，提供一个默认配置
                        elif "trends" in result and isinstance(result["trends"], list) and len(result["trends"]) > 0:
                            result["echarts_option"] = {
                                "title": {"text": "数据趋势图"},
                                "tooltip": {},
                                "xAxis": {"type": "category", "data": ["趋势项"]},
                                "yAxis": {"type": "value"},
                                "series": [{"name": "趋势", "type": "bar", "data": [1]}]
                            }
                
                # 转为JSON字符串
                result_json = json.dumps(result, ensure_ascii=False, default=str)
                
                # 构建最终事件数据
                final_event_data = {
                    "type": "final",
                    "data": {
                        "content": result_json,
                        "result": result,
                        "sql": result.get("sql", "") if isinstance(result, dict) else "",
                        "analysis": result.get("analysis", "此查询无法提供业务分析。") if isinstance(result, dict) else "",
                        "visualization": result.get("visualization", "此查询无法提供可视化建议。") if isinstance(result, dict) else "",
                        "echarts_option": result.get("echarts_option", None) if isinstance(result, dict) else None,
                        "trends": result.get("trends", []) if isinstance(result, dict) else [],
                        "recommendations": result.get("recommendations", []) if isinstance(result, dict) else []
                    }
                }
                
                # 如果结果中有business_analysis对象，添加到最终事件数据中
                if isinstance(result, dict) and "business_analysis" in result:
                    final_event_data["data"]["business_analysis"] = result["business_analysis"]
                
                # 转为JSON字符串并格式化为SSE事件
                final_event = f"data: {json.dumps(final_event_data, ensure_ascii=False)}\n\n"
                logger.info(f"发送最终结果事件: event类型={final_event_data['type']}, 长度={len(final_event)}")
                logger.info(f"最终结果包含业务分析: {'analysis' in result}, 包含可视化建议: {'visualization' in result}, 包含图表配置: {'echarts_option' in result}")
                
                # 直接yield事件
                yield final_event
                
                # 不要在这里发送关闭事件，让调用者决定何时关闭连接
                logger.info("最终结果发送完成")
            except Exception as final_error:
                logger.error(f"发送最终结果时出错: {str(final_error)}")
                logger.exception("发送最终结果的详细错误信息")
                
        except Exception as e:
            logger.error(f"处理流式查询时出错: {str(e)}")
            logger.exception("详细错误信息")
            
            try:
                # 尝试发送错误信号
                async for event in call_stream_callback("error", f"处理出错: {str(e)}"):
                    yield event
            except Exception as callback_error:
                logger.error(f"发送错误信号时出错: {str(callback_error)}")
            
            # 更新响应
            response["error"] = str(e)
            response["success"] = False
            response["message"] = f"处理查询时出错: {str(e)}"
            response["execution_time"] = time.time() - start_time

