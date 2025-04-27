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
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# 添加项目根目录到路径
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# 导入MCP工具和资源
from mcp.server.fastmcp import Context, FastMCP
from src.sse_server import DorisMCPSseServer
from src.streamable_server import DorisMCPStreamableServer
from src.tools.tool_initializer import register_mcp_tools
from src.utils.db_init import init_metadata_tables

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
    config: Dict[str, Any]

# 应用生命周期管理
@asynccontextmanager
async def app_lifespan(app: FastAPI) -> AsyncIterator[AppContext]:
    """管理应用生命周期和共享资源"""
    # 初始化元数据表
    logger.info("开始初始化元数据表...")
    try:
        init_metadata_tables()
        logger.info("元数据表初始化成功")
    except Exception as e:
        logger.error(f"初始化元数据表时出错: {str(e)}", exc_info=True)
    
    # 输出所有环境变量，帮助调试
    logger.info("当前环境变量值:")
    logger.info(f"FORCE_REFRESH_METADATA={os.getenv('FORCE_REFRESH_METADATA')}")
    logger.info(f"DB_HOST={os.getenv('DB_HOST')}")
    logger.info(f"DB_PORT={os.getenv('DB_PORT')}")
    logger.info(f"DB_USER={os.getenv('DB_USER')}")
    logger.info(f"DB_DATABASE={os.getenv('DB_DATABASE')}")
    
    # 刷新元数据
    force_refresh_value = os.getenv("FORCE_REFRESH_METADATA", "false")
    logger.info(f"原始FORCE_REFRESH_METADATA值: '{force_refresh_value}'")
    force_refresh = force_refresh_value.lower() == "true"
    logger.info(f"解析后的force_refresh值: {force_refresh}")
    
    # 检查是否已在main入口处刷新过元数据
    metadata_refreshed = os.getenv("METADATA_REFRESHED", "false").lower() == "true"
    
    if force_refresh and not metadata_refreshed:
        logger.info("检测到FORCE_REFRESH_METADATA=true且尚未刷新元数据，将强制刷新元数据...")
        try:
            # 创建元数据提取器
            from src.utils.metadata_extractor import MetadataExtractor
            logger.info("开始创建MetadataExtractor实例...")
            metadata_extractor = MetadataExtractor()
            logger.info("成功创建MetadataExtractor实例")
            
            # 强制刷新所有数据库的元数据
            logger.info("开始调用refresh_all_databases_metadata(force=True)...")
            result = metadata_extractor.refresh_all_databases_metadata(force=True)
            logger.info(f"元数据刷新结果: {result}")
            if result:
                logger.info("元数据强制刷新完成")
                # 设置环境变量标记已刷新
                os.environ["METADATA_REFRESHED"] = "true"
            else:
                logger.warning("元数据刷新失败，请检查日志获取详细错误信息")
        except Exception as e:
            logger.error(f"强制刷新元数据时出错: {str(e)}", exc_info=True)
    else:
        if metadata_refreshed:
            logger.info("元数据已在之前的步骤中刷新，跳过元数据刷新")
        else:
            logger.info("FORCE_REFRESH_METADATA不为true，跳过元数据刷新")
    
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
    
    # 将共享资源（包括mcp实例）存储在app.state中
    # mcp 实例在外部创建，这里可以存储其他配置
    app.state.config = config
    app.state.mcp = mcp
    
    try:
        # 提供上下文，状态已在 app.state 中设置
        yield # No need to yield the context object itself
    finally:
        # 清理资源
        logger.info("清理应用资源...")

# 创建MCP服务器实例
mcp = FastMCP(
    name="Doris MCP Server",
    description="Apache Doris 自然语言查询服务",
    lifespan=app_lifespan,
    dependencies=["fastapi", "uvicorn", "openai", "sse_starlette"]
)

# 创建FastAPI应用
app = FastAPI(
    title="Doris MCP Server (Hybrid: SSE + Streamable)",
    lifespan=lambda app_instance: app_lifespan(app_instance)
)

# 启动服务器
def start_server():
    args = parse_args()
    
    # 配置 CORS
    origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
    allow_credentials = os.getenv("MCP_ALLOW_CREDENTIALS", "false").lower() == "true"
    logger.info(f"CORS Settings: allow_origins={origins}, allow_credentials={allow_credentials}")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Mcp-Session-Id"],
    )
    
    # 输出环境变量
    print("--- Configuration ---")
    print(f"Server Host: {args.host}")
    print(f"Server Port: {args.port}")
    print(f"Allowed Origins: {origins}")
    print(f"Allow Credentials: {allow_credentials}")
    print(f"Log Level: {os.getenv('LOG_LEVEL', 'info')}")
    print(f"Debug Mode: {args.debug}")
    print(f"Reload Mode: {args.reload}")
    print(f"DB Host: {os.getenv('DB_HOST')}")
    print(f"DB Port: {os.getenv('DB_PORT')}")
    print(f"DB User: {os.getenv('DB_USER')}")
    print(f"DB Database: {os.getenv('DB_DATABASE')}")
    print(f"Force Refresh Metadata: {os.getenv('FORCE_REFRESH_METADATA', 'false')}")
    print("---------------------")
    
    # 初始化MCP服务器 (Hybrid Mode)
    print(f"Starting MCP Server in Hybrid Mode (SSE + Streamable HTTP)...")
    
    # 创建并初始化两个服务器实现
    # 它们会将各自的路由注册到同一个 FastAPI 'app' 实例上
    print("Initializing Legacy SSE Server...")
    sse_server = DorisMCPSseServer(mcp, app) 
    print("Initializing Streamable HTTP Server...")
    streamable_server = DorisMCPStreamableServer(mcp, app)
    
    # 打印可用端点
    base_url = f"http://{args.host}:{args.port}"
    print(f"Service running at: {base_url}")
    print(f"  Health Check: GET {base_url}/health")
    print(f"  Status Check: GET {base_url}/status")
    print(f"  Legacy SSE Init: GET {base_url}/mcp-sse-init")
    print(f"  Legacy SSE Messages: POST {base_url}/mcp/messages")
    print(f"  Streamable HTTP: GET/POST/DELETE/OPTIONS {base_url}/mcp")
    print("---------------------")
    print("Use Ctrl+C to stop the service")
    
    # 将MCP服务器实例添加到app.state中 (如果lifespan中没有设置，这里是必须的)
    if not hasattr(app.state, 'mcp'):
         app.state.mcp = mcp
         logger.warning("MCP instance set in start_server, consider setting it earlier in lifespan.")

    # 注册MCP工具函数
    logger.info("Registering MCP tools...")
    asyncio.run(register_mcp_tools(mcp))
    
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
        # 不再需要在主程序块中执行初始化，这些操作已移至 app_lifespan
        # logger.info("Executing pre-start metadata initialization...")
        # init_metadata_tables()
        
        # # 检查各种元数据刷新控制变量
        # force_refresh_value = os.getenv("FORCE_REFRESH_METADATA", "false").lower()
        # startup_refresh_value = os.getenv("STARTUP_REFRESH_METADATA", "false").lower()
        # logger.info(f"FORCE_REFRESH_METADATA={force_refresh_value}")
        # logger.info(f"STARTUP_REFRESH_METADATA={startup_refresh_value}")
        
        # # 确定是否需要刷新元数据
        # force_refresh = force_refresh_value == "true"
        # startup_refresh = startup_refresh_value == "true"
        # need_refresh = force_refresh or startup_refresh
        
        # if need_refresh:
        #     logger.info(f"检测到需要刷新元数据: FORCE_REFRESH_METADATA={force_refresh}, STARTUP_REFRESH_METADATA={startup_refresh}")
        #     from src.utils.metadata_extractor import MetadataExtractor
        #     from src.utils.db import execute_query
            
        #     try:
        #         # 创建元数据提取器
        #         metadata_extractor = MetadataExtractor()
        #         # 执行刷新，根据force_refresh决定是全量还是增量
        #         logger.info(f"正在执行{'强制全量' if force_refresh else '增量'}刷新元数据...")
        #         result = metadata_extractor.refresh_all_databases_metadata(force=force_refresh)
                
        #         if result:
        #             logger.info("元数据刷新成功")
        #             # 设置环境变量，标记元数据已经刷新过
        #             os.environ["METADATA_REFRESHED"] = "true"
        #         else:
        #             logger.warning("警告：元数据刷新失败，请检查日志获取详细错误信息")
        #     except Exception as e:
        #         logger.error(f"刷新元数据时出错: {str(e)}")
        # else:
        #     logger.info("不需要在启动时刷新元数据，跳过刷新")
        #     # 检查是否已存在元数据
        #     try:
        #         from src.utils.db import execute_query
        #         from src.prompts.metadata_schema import METADATA_DB_NAME
                
        #         db_name = os.getenv("DB_DATABASE", "")
        #         query = f"""
        #         SELECT COUNT(*) as count
        #         FROM {METADATA_DB_NAME}.business_metadata 
        #         WHERE db_name = '{db_name}' 
        #         AND table_name = '' 
        #         AND metadata_type = 'business_summary'
        #         """
                
        #         result = execute_query(query)
        #         if result and result[0]['count'] > 0:
        #             logger.info(f"数据库 {db_name} 已存在元数据")
        #             # 设置环境变量，标记元数据已经存在
        #             os.environ["METADATA_REFRESHED"] = "true"
        #         else:
        #             logger.info(f"数据库 {db_name} 不存在元数据，将在有需要时刷新")
        #     except Exception as e:
        #         logger.error(f"检查元数据时出错: {str(e)}")
        
        # 启动服务器
        start_server()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，服务器关闭")
    except Exception as e:
        logger.critical(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1) 