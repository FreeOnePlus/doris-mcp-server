#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MCP 客户端实现
遵循 MCP 协议，使用 SSE 建立连接
"""

import json
import time
import uuid
import logging
import requests
import sseclient
from typing import Dict, Any, Optional, Callable, List, Union
import threading

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MCPClient:
    """MCP 协议客户端"""
    
    def __init__(self, server_url: str):
        """
        初始化 MCP 客户端
        
        Args:
            server_url: MCP 服务器 URL (例如: http://localhost:3000)
        """
        self.server_url = server_url.rstrip('/')
        self.sse_url = f"{self.server_url}/sse"
        self.session_id = None
        self.message_url = None
        self.sse_client = None
        self.sse_connection = None
        self.sse_thread = None
        self.connected = False
        self.callbacks = {}
        self.response_cache = {}
        
    def connect(self) -> bool:
        """
        连接到 MCP 服务器
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 创建 SSE 连接
            headers = {"Accept": "text/event-stream"}
            self.sse_connection = requests.get(self.sse_url, headers=headers, stream=True)
            
            if self.sse_connection.status_code != 200:
                logger.error(f"SSE 连接失败: {self.sse_connection.status_code} {self.sse_connection.reason}")
                return False
            
            # 创建 SSE 客户端
            self.sse_client = sseclient.SSEClient(self.sse_connection)
            
            # 启动 SSE 监听线程
            self.sse_thread = threading.Thread(target=self._listen_sse, daemon=True)
            self.sse_thread.start()
            
            # 等待连接初始化
            timeout = time.time() + 5  # 5秒超时
            while not self.connected and time.time() < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                logger.error("连接超时，未收到初始化事件")
                self.disconnect()
                return False
            
            logger.info(f"成功连接到 MCP 服务器: {self.server_url}, 会话ID: {self.session_id}")
            return True
            
        except Exception as e:
            logger.error(f"连接到 MCP 服务器时出错: {str(e)}")
            self.disconnect()
            return False
    
    def _listen_sse(self):
        """监听 SSE 事件"""
        try:
            for event in self.sse_client:
                if event.event == "init":
                    # 处理初始化事件
                    self._handle_init_event(event.data)
                elif event.event == "response":
                    # 处理响应事件
                    self._handle_response_event(event.data)
        except Exception as e:
            logger.error(f"SSE 监听出错: {str(e)}")
            self.connected = False
        finally:
            self.connected = False
    
    def _handle_init_event(self, data: str):
        """
        处理初始化事件
        
        Args:
            data: 事件数据
        """
        try:
            init_data = json.loads(data)
            self.session_id = init_data.get("session_id")
            self.message_url = init_data.get("message_url") or f"{self.server_url}/message"
            self.connected = True
            logger.debug(f"收到初始化事件: 会话ID={self.session_id}, 消息URL={self.message_url}")
        except Exception as e:
            logger.error(f"处理初始化事件时出错: {str(e)}")
    
    def _handle_response_event(self, data: str):
        """
        处理响应事件
        
        Args:
            data: 事件数据
        """
        try:
            response_data = json.loads(data)
            request_id = response_data.get("id")
            
            if request_id in self.callbacks:
                # 调用回调函数
                callback = self.callbacks.pop(request_id)
                callback(response_data)
            
            # 缓存响应
            self.response_cache[request_id] = response_data
            logger.debug(f"收到响应: ID={request_id}, 数据={response_data}")
        except Exception as e:
            logger.error(f"处理响应事件时出错: {str(e)}")
    
    def request(self, method: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
        """
        发送请求到 MCP 服务器
        
        Args:
            method: 方法名
            params: 方法参数
            timeout: 超时时间(秒)
        
        Returns:
            Dict[str, Any]: 响应数据
        
        Raises:
            Exception: 请求失败时抛出
        """
        if not self.connected or not self.session_id:
            raise Exception("未连接到 MCP 服务器，请先调用 connect() 方法")
        
        # 生成请求ID
        request_id = str(uuid.uuid4())
        
        # 创建请求体
        request_body = {
            "id": request_id,
            "method": method,
            "session_id": self.session_id
        }
        
        if params:
            request_body["params"] = params
        
        # 结果占位符
        result = None
        
        # 创建事件以等待响应
        response_event = threading.Event()
        
        # 定义回调
        def on_response(response):
            nonlocal result
            result = response
            response_event.set()
        
        # 注册回调
        self.callbacks[request_id] = on_response
        
        # 发送请求
        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                self.message_url, 
                headers=headers, 
                json=request_body
            )
            
            if response.status_code != 202:
                # 如果不是202 Accepted，可能是直接响应
                try:
                    result = response.json()
                    response_event.set()
                except:
                    raise Exception(f"请求失败: {response.status_code} {response.reason}")
                
        except Exception as e:
            self.callbacks.pop(request_id, None)
            raise Exception(f"发送请求时出错: {str(e)}")
        
        # 等待响应
        if not response_event.wait(timeout):
            self.callbacks.pop(request_id, None)
            raise Exception(f"请求超时: {method}")
        
        # 检查错误
        if result and "error" in result:
            raise Exception(f"请求错误: {result.get('error')}")
        
        return result.get("result", result)
    
    def disconnect(self):
        """断开 MCP 服务器连接"""
        self.connected = False
        
        if self.sse_connection:
            try:
                self.sse_connection.close()
            except:
                pass
            self.sse_connection = None
        
        self.sse_client = None
        self.session_id = None
        self.callbacks = {}
        logger.info("已断开 MCP 服务器连接")


# 使用示例
if __name__ == "__main__":
    # 创建客户端
    client = MCPClient("http://localhost:3000")
    
    try:
        # 连接到服务器
        if client.connect():
            # 测试ping
            print("\n测试 ping...")
            result = client.request("ping")
            print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            # 测试获取提示列表
            print("\n测试 prompts/list...")
            result = client.request("prompts/list")
            print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            # 测试获取资源模板列表
            print("\n测试 resources/templates/list...")
            result = client.request("resources/templates/list")
            print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            # 测试获取数据库信息
            print("\n测试 资源 doris://database/info...")
            result = client.request("resource", {"uri": "doris://database/info"})
            print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            # 测试获取数据库元数据
            print("\n测试 资源 doris://database/metadata...")
            result = client.request("resource", {"uri": "doris://database/metadata"})
            print(f"结果: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 断开连接
        client.disconnect() 