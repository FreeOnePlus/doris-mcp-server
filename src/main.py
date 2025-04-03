#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Doris MCP NL2SQL服务

主入口文件
"""

import os
import sys
import json
from dotenv import load_dotenv
import datetime
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route
from fastapi import Response, Request, FastAPI
import logging
import uvicorn

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入统一日志配置
from src.utils.logger import get_logger, audit_logger

# 导入工具注册中心和初始化模块
from src.tools.tool_registry import get_registry
from src.tools.tool_initializer import init_tools, register_mcp_tools

# 导入SSE服务器实现
from src.sse_server import DorisMCPSseServer

# 获取日志器
logger = get_logger(__name__)

# 替换json.dumps，确保中文不被转义为Unicode序列
_original_dumps = json.dumps
def _custom_dumps(*args, **kwargs):
    kwargs['ensure_ascii'] = False
    return _original_dumps(*args, **kwargs)
json.dumps = _custom_dumps

# 从mcp.server.fastmcp导入FastMCP
from mcp.server.fastmcp import FastMCP

# 导入服务
from src.nl2sql_service import NL2SQLService

# 导入SQL优化器
from src.utils.sql_optimizer import SQLOptimizer

# 加载环境变量
load_dotenv()

# 读取环境变量决定是否自动刷新元数据
auto_refresh_metadata = os.getenv("AUTO_REFRESH_METADATA", "false").lower() == "true"

# 初始化MCP服务器
mcp = FastMCP(
    name="Doris MCP Server",
    instructions="这是一个用于 Apache Doris 数据库的 MCP 服务器，提供查询和分析功能。"
)

# 将工具注册到FastMCP
logger.info("开始注册工具到FastMCP...")
register_mcp_tools(mcp)
# 注意：list_tools()是异步函数，不能在同步代码中直接调用
# 后续在异步上下文中调用时会获取工具列表
logger.info("工具注册完成")

# 创建FastAPI应用
app = FastAPI(title="Doris MCP Server")

# 将MCP实例添加到应用状态
app.state.mcp = mcp

# 初始化SSE服务器
sse_server = DorisMCPSseServer(mcp, app)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源，更灵活的配置
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头部
    expose_headers=["*"],  # 暴露所有响应头
    max_age=3600,  # 预检请求缓存时间
)

# 添加MCP适配器中间件
from src.mcp_adapter import MCPAdapterMiddleware
app.add_middleware(MCPAdapterMiddleware)

# 初始化NL2SQL服务
service = NL2SQLService()

# 初始化SQL优化器
sql_optimizer = SQLOptimizer()

# 初始化流式处理器实例，用于处理状态跟踪
from src.nl2sql_stream_processor import StreamNL2SQLProcessor
stream_processor = StreamNL2SQLProcessor()

@mcp.resource("doris://database/info")
def doris_database_info():
    """获取Doris数据库信息和表结构"""
    try:
        # 只获取基本信息，不执行详细的元数据分析
        status = service.get_database_status()
        return status  # 资源类型接口不需要添加result包装
    except Exception as e:
        import traceback
        print(f"获取数据库信息时出错: {str(e)}")
        print(traceback.format_exc())
        return {
            "error": f"获取数据库信息时出错: {str(e)}"
        }

def add_sse_endpoints(app):
    """添加自定义SSE端点和测试端点"""
    async def sse_test(request):
        """SSE测试端点，返回一个简单的JSON表明SSE服务已正确配置"""
        return JSONResponse({
            "status": "ok", 
            "message": "SSE服务器正常运行", 
            "timestamp": datetime.datetime.now().isoformat()
        })
    
    async def health_check(request):
        """健康检查端点，使前端可以直接检查服务器状态"""
        return JSONResponse({
            "status": "healthy",
            "timestamp": datetime.datetime.now().isoformat()
        })
    
    async def test_session(request):
        """测试会话端点，测试会话ID处理"""
        session_id = request.path_params.get("session_id", "unknown")
        return JSONResponse({
            "status": "ok",
            "session_id": session_id,
            "message": f"会话 {session_id} 有效",
            "timestamp": datetime.datetime.now().isoformat()
        })
    
    # 使用Starlette的路由方式添加路由
    routes = [
        Route("/sse-test", sse_test),
        Route("/health", health_check),
        Route("/test-session/{session_id}", test_session)
    ]
    
    # 将路由添加到应用
    for route in routes:
        app.routes.append(route)
    
    return app

async def main():
    """主入口函数"""
    try:
        # 启动服务器
        config = uvicorn.Config(
            app=app,
            host="127.0.0.1",
            port=3000,
            log_level="info"
        )
        server = uvicorn.Server(config)
        
        # 打印服务器信息
        print(f"启动 MCP SSE 服务器，地址: http://127.0.0.1:3000/")
        print(f"MCP SSE 端点: http://127.0.0.1:3000/mcp")
        print(f"MCP 消息端点: http://127.0.0.1:3000/mcp/messages")
        
        await server.serve()
        
    except Exception as e:
        logger.error(f"服务器启动失败: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main()) 

# 注意：所有工具已通过register_mcp_tools函数在前面注册过，这里不再重复注册
# 下面的重复工具定义已被移除

