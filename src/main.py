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
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
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
    
    try:
        # 提供上下文
        yield AppContext(
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
    logger.info("正在注册MCP工具...")
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
        # 直接执行元数据初始化和刷新，确保在服务器启动前完成
        logger.info("执行元数据初始化...")
        init_metadata_tables()
        
        # 检查各种元数据刷新控制变量
        force_refresh_value = os.getenv("FORCE_REFRESH_METADATA", "false").lower()
        startup_refresh_value = os.getenv("STARTUP_REFRESH_METADATA", "false").lower()
        logger.info(f"FORCE_REFRESH_METADATA={force_refresh_value}")
        logger.info(f"STARTUP_REFRESH_METADATA={startup_refresh_value}")
        
        # 确定是否需要刷新元数据
        force_refresh = force_refresh_value == "true"
        startup_refresh = startup_refresh_value == "true"
        need_refresh = force_refresh or startup_refresh
        
        if need_refresh:
            logger.info(f"检测到需要刷新元数据: FORCE_REFRESH_METADATA={force_refresh}, STARTUP_REFRESH_METADATA={startup_refresh}")
            from src.utils.metadata_extractor import MetadataExtractor
            from src.utils.db import execute_query
            
            try:
                # 创建元数据提取器
                metadata_extractor = MetadataExtractor()
                # 执行刷新，根据force_refresh决定是全量还是增量
                logger.info(f"正在执行{'强制全量' if force_refresh else '增量'}刷新元数据...")
                result = metadata_extractor.refresh_all_databases_metadata(force=force_refresh)
                
                if result:
                    logger.info("元数据刷新成功")
                    # 设置环境变量，标记元数据已经刷新过
                    os.environ["METADATA_REFRESHED"] = "true"
                else:
                    logger.warning("警告：元数据刷新失败，请检查日志获取详细错误信息")
            except Exception as e:
                logger.error(f"刷新元数据时出错: {str(e)}")
        else:
            logger.info("不需要在启动时刷新元数据，跳过刷新")
            # 检查是否已存在元数据
            try:
                from src.utils.db import execute_query
                from src.prompts.metadata_schema import METADATA_DB_NAME
                
                db_name = os.getenv("DB_DATABASE", "")
                query = f"""
                SELECT COUNT(*) as count
                FROM {METADATA_DB_NAME}.business_metadata 
                WHERE db_name = '{db_name}' 
                AND table_name = '' 
                AND metadata_type = 'business_summary'
                """
                
                result = execute_query(query)
                if result and result[0]['count'] > 0:
                    logger.info(f"数据库 {db_name} 已存在元数据")
                    # 设置环境变量，标记元数据已经存在
                    os.environ["METADATA_REFRESHED"] = "true"
                else:
                    logger.info(f"数据库 {db_name} 不存在元数据，将在有需要时刷新")
            except Exception as e:
                logger.error(f"检查元数据时出错: {str(e)}")
        
        # 启动服务器
        logger.info("启动 MCP SSE 服务器，地址: http://{}:{}/".format(
            os.getenv('SERVER_HOST', '0.0.0.0'), 
            int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000')))
        ))
        logger.info("MCP SSE 端点: http://{}:{}/mcp".format(
            os.getenv('SERVER_HOST', '0.0.0.0'), 
            int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000')))
        ))
        logger.info("MCP 消息端点: http://{}:{}/mcp/messages".format(
            os.getenv('SERVER_HOST', '0.0.0.0'), 
            int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000')))
        ))
        start_server()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，服务器关闭")
    except Exception as e:
        logger.error(f"服务器启动出错: {e}", exc_info=True) 