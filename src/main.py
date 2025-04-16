#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Doris MCP Server 主入口

这是Doris MCP服务器的主入口文件，用于启动服务器和初始化应用。
"""

import os
import sys
import argparse
import logging
import asyncio
import json
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import uvicorn
from fastapi import FastAPI, Request
from dotenv import load_dotenv

# 添加项目根目录到路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# 导入MCP工具和资源
from mcp.server.fastmcp import Context, FastMCP
from src.sse_server import DorisMCPSseServer
from src.tools.tool_initializer import register_mcp_tools
from src.nl2sql_service import NL2SQLService

# 加载环境变量
load_dotenv(override=True)

# 获取日志记录器
from src.utils.logger import get_logger
logger = get_logger(__name__)

# 解析命令行参数
def parse_args():
    parser = argparse.ArgumentParser(description="Apache Doris MCP Server")
    parser.add_argument('--host', type=str, default=os.getenv('SERVER_HOST', '0.0.0.0'), help='主机地址')
    parser.add_argument('--port', type=int, default=int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000'))), help='端口号')
    parser.add_argument('--debug', action='store_true', help='开启调试模式')
    parser.add_argument('--reload', action='store_true', help='开启自动重载')
    return parser.parse_args()

# 定义应用上下文类
@dataclass
class AppContext:
    """应用上下文，包含全局共享资源"""
    nl2sql_service: NL2SQLService
    config: Dict[str, Any]

# 应用生命周期管理
@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """管理应用生命周期和共享资源"""
    # 初始化配置
    config = {
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": int(os.getenv("DB_PORT", "9030")),
        "db_user": os.getenv("DB_USER", "root"),
        "db_password": os.getenv("DB_PASSWORD", ""),
        "db_database": os.getenv("DB_DATABASE", "tpch"),
        "llm_provider": os.getenv("LLM_PROVIDER", "openai"),
        "llm_model": os.getenv("LLM_MODEL", "gpt-3.5-turbo"),
        "server_name": "Doris MCP Server",
        "version": "1.0.0"
    }
    
    # 初始化NL2SQL服务
    nl2sql_service = NL2SQLService()
    logger.info("NL2SQL服务初始化完成")
    
    try:
        # 提供上下文
        yield AppContext(
            nl2sql_service=nl2sql_service,
            config=config
        )
    finally:
        # 清理资源
        logger.info("清理应用资源...")
        # 如果需要，可以在这里添加资源清理代码

# 创建MCP服务器实例
mcp = FastMCP(
    name="Doris MCP Server",
    description="Apache Doris 自然语言查询服务",
    lifespan=app_lifespan,
    dependencies=["fastapi", "uvicorn", "openai"]
)

# 创建FastAPI应用
app = FastAPI(title="Doris MCP Server")

# 启动服务器
def start_server():
    args = parse_args()
    
    # 确保使用SSE传输类型
    transport_type = os.getenv('MCP_TRANSPORT_TYPE', 'sse')
    if transport_type != 'sse':
        logger.warning(f"配置的传输类型为 {transport_type}，但只支持SSE，将使用SSE模式")
        os.environ['MCP_TRANSPORT_TYPE'] = 'sse'
    
    # 输出环境变量
    print("环境变量:")
    print(f"MCP_TRANSPORT_TYPE={os.getenv('MCP_TRANSPORT_TYPE', 'sse')}")
    print(f"MCP_PORT={args.port}")
    print(f"ALLOWED_ORIGINS={os.getenv('ALLOWED_ORIGINS', '*')}")
    print(f"LOG_LEVEL={os.getenv('LOG_LEVEL', 'info')}")
    print(f"MCP_ALLOW_CREDENTIALS={os.getenv('MCP_ALLOW_CREDENTIALS', 'false')}")
    print(f"MCP_DEBUG_ADAPTER={os.getenv('MCP_DEBUG_ADAPTER', 'true')}")
    
    # 初始化MCP服务器
    print(f"正在启动MCP服务器 (SSE模式)...")
    
    # 创建SSE服务器
    sse_server = DorisMCPSseServer(mcp, app)
    
    print(f"服务将在 http://{args.host}:{args.port} 上运行")
    print(f"健康检查: http://{args.host}:{args.port}/health")
    print(f"SSE测试: http://{args.host}:{args.port}/sse-test")
    print("使用 Ctrl+C 停止服务")
    
    # 将MCP服务器实例添加到app.state中
    app.state.mcp = mcp
    
    # 注册MCP工具函数
    register_mcp_tools(mcp)
    
    # 启动FastAPI应用
    uvicorn.run(
        app, 
        host=args.host, 
        port=args.port,
        log_level="debug" if args.debug else "info",
        reload=args.reload
    )

if __name__ == "__main__":
    try:
        # 启动服务器
        print("启动 MCP SSE 服务器，地址: http://{}:{}/".format(
            os.getenv('SERVER_HOST', '0.0.0.0'), 
            int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000')))
        ))
        print("MCP SSE 端点: http://{}:{}/mcp".format(
            os.getenv('SERVER_HOST', '0.0.0.0'), 
            int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000')))
        ))
        print("MCP 消息端点: http://{}:{}/mcp/messages".format(
            os.getenv('SERVER_HOST', '0.0.0.0'), 
            int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000')))
        ))
        start_server()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，服务器关闭")
    except Exception as e:
        logger.error(f"服务器启动出错: {e}", exc_info=True) 