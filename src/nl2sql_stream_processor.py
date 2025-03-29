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
from typing import Dict, List, Any, Optional, Tuple, Union, Callable

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
                    "stages": self.shared_data["processing_stages"]
                }
                self.shared_data["history"].append(history_item)
                
                # 限制历史记录数量
                if len(self.shared_data["history"]) > 10:
                    self.shared_data["history"] = self.shared_data["history"][-10:]
            
            # 重置处理阶段
            self.shared_data["processing_stages"] = []
            self.shared_data["last_stage_index"] = -1

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
            "success": True,
            "processing_stages": []  # 用于记录处理阶段
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
                    # 处理查询
                    logger.info("开始处理NL2SQL查询: %s", query)
                    result = self.processor.process(query)
                    logger.info("NL2SQL查询处理完成")
                    
                    # 存储结果
                    processing_result["result"] = result
                    if "processing_stages" in result:
                        logger.info(f"获取到 {len(result['processing_stages'])} 个处理阶段")
                        for stage in result["processing_stages"]:
                            logger.info(f"阶段信息: {stage['stage']} - {stage['description']} ({stage['progress']}%)")
                        processing_result["stages"] = result.get("processing_stages", [])
                    else:
                        logger.warning("结果中没有处理阶段信息！")
                    processing_result["completed"] = True
                    
                    with _status_lock:
                        # 更新状态信息
                        self.shared_data["is_processing"] = False
                        self.shared_data["last_completed_time"] = time.time()
                        
                except Exception as e:
                    logger.error(f"后台处理查询时出错: {str(e)}")
                    processing_result["error"] = str(e)
                    processing_result["completed"] = True
                    
                    with _status_lock:
                        # 更新状态信息
                        self.shared_data["is_processing"] = False
            
            # 启动后台处理线程
            thread = threading.Thread(target=background_process)
            thread.daemon = True
            thread.start()
            
            # 初始阶段信息
            await call_stream_callback("开始处理查询...", {
                "step": "start",
                "type": "start",
                "progress": 5
            })
            
            # 记录初始阶段
            with _status_lock:
                self.shared_data["processing_stages"].append({
                    "stage": "start",
                    "description": "开始处理查询...",
                    "progress": 5,
                    "timestamp": time.time()
                })
            
            # 循环检查处理进度并实时发送阶段信息
            last_stage_index = -1
            poll_count = 0
            last_log_time = time.time()
            while not processing_result["completed"]:
                poll_count += 1
                
                # 检查是否有新的阶段信息需要发送
                with _status_lock:
                    stages = self.shared_data["processing_stages"]
                
                # 记录轮询状态（每10秒记录一次）
                current_time = time.time()
                if current_time - last_log_time >= 10:  # 每10秒记录一次日志
                    logger.info(f"轮询中: 已轮询{poll_count}次, 已处理{last_stage_index+1}个阶段, 当前有{len(stages)}个阶段")
                    last_log_time = current_time
                
                # 如果有新的阶段，则发送它们
                for i in range(last_stage_index + 1, len(stages)):
                    stage = stages[i]
                    stage_name = stage.get("stage")
                    description = stage.get("description")
                    progress = stage.get("progress", 0)
                    
                    logger.info(f"实时发送阶段信息: {stage_name} ({progress}%) - {description}")
                    await call_stream_callback(description, {
                        "step": stage_name,
                        "type": stage_name,
                        "progress": progress
                    })
                    
                    # 更新最后处理的阶段索引
                    last_stage_index = i
                
                # 如果还没有处理完，等待一小段时间再检查
                if not processing_result["completed"]:
                    await asyncio.sleep(1.0)  # 1秒的轮询间隔
            
            # 检查是否有错误
            if processing_result["error"]:
                error_msg = processing_result["error"]
                logger.error(f"处理查询时出错: {error_msg}")
                
                # 发送错误信号
                await call_stream_callback(f"处理出错: {error_msg}", {
                    "step": "error",
                    "type": "error",
                    "progress": 100
                })
                
                # 记录错误状态
                with _status_lock:
                    self.shared_data["processing_stages"].append({
                        "stage": "error",
                        "description": f"处理出错: {error_msg}",
                        "progress": 100,
                        "timestamp": time.time()
                    })
                
                response["error"] = error_msg
                response["success"] = False
                response["message"] = f"处理查询时出错: {error_msg}"
                return response
            
            # 获取处理结果
            result = processing_result["result"]
            if not result:
                error_msg = "处理完成但没有返回结果"
                logger.error(error_msg)
                
                # 发送错误信号
                await call_stream_callback(error_msg, {
                    "step": "error",
                    "type": "error",
                    "progress": 100
                })
                
                # 记录错误状态
                with _status_lock:
                    self.shared_data["processing_stages"].append({
                        "stage": "error",
                        "description": error_msg,
                        "progress": 100,
                        "timestamp": time.time()
                    })
                
                response["error"] = error_msg
                response["success"] = False
                response["message"] = error_msg
                return response
            
            # 添加思考过程到结果
            result["thinking_process"] = response["thinking_process"]
            
            # 确保结果中包含step和type字段
            if "step" not in result:
                result["step"] = "complete"
            if "type" not in result:
                result["type"] = "complete"
            if "progress" not in result:
                result["progress"] = 100
            
            # 发送完成信号
            await call_stream_callback("查询处理完成", {
                "step": "complete",
                "type": "complete",
                "progress": 100
            })
            
            # 记录完成状态
            with _status_lock:
                self.shared_data["processing_stages"].append({
                    "stage": "complete",
                    "description": "查询处理完成",
                    "progress": 100,
                    "timestamp": time.time()
                })
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"流式处理查询时出错: {error_msg}")
            
            # 发送错误信号
            await call_stream_callback(f"处理出错: {error_msg}", {
                "step": "error",
                "type": "error",
                "progress": 100
            })
            
            # 记录错误状态
            with _status_lock:
                self.shared_data["processing_stages"].append({
                    "stage": "error",
                    "description": f"处理出错: {error_msg}",
                    "progress": 100,
                    "timestamp": time.time()
                })
                self.shared_data["is_processing"] = False
            
            response["error"] = error_msg
            response["success"] = False
            response["message"] = f"处理查询时出错: {error_msg}"
            
            return response

