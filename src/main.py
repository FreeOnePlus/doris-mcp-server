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
from fastapi import Response, Request

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入统一日志配置
from src.utils.logger import get_logger, audit_logger

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
mcp = FastMCP("Doris NL2SQL")

# 初始化NL2SQL服务 (使用新的初始化参数格式)
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

@mcp.tool()
def nl2sql_query(query: str):
    """
    将自然语言查询转换为SQL，并执行查询返回结果。
    
    参数:
        query: 自然语言查询
    
    返回:
        查询结果，包含SQL、执行结果等
    """
    try:
        # 记录开始处理请求
        start_time = datetime.datetime.now()
        request_id = hash(f"{query}_{start_time.isoformat()}")
        
        # 记录请求到审计日志
        audit_data = {
            "timestamp": start_time.isoformat(),
            "request_id": str(request_id),
            "action": "nl2sql_query",
            "query": query,
            "status": "processing"
        }
        audit_logger.audit(json.dumps(audit_data))
        
        # 调用服务处理查询
        result = service.process_query(query)
        
        # 记录处理结果到审计日志
        end_time = datetime.datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        audit_data.update({
            "end_timestamp": end_time.isoformat(),
            "duration_ms": duration_ms,
            "status": "completed" if result.get("success", False) else "failed",
            "sql": result.get("sql", ""),
            "error": result.get("error", None)
        })
        audit_logger.audit(json.dumps(audit_data))
        
        # 返回结果
        return result
    except Exception as e:
        # 记录错误到审计日志和错误日志
        error_msg = str(e)
        logger.error(f"处理查询时出错: {error_msg}")
        
        # 错误审计
        if 'audit_data' in locals():
            audit_data.update({
                "status": "error",
                "error": error_msg
            })
            audit_logger.audit(json.dumps(audit_data))
        
        return {
            "success": False,
            "message": f"处理查询时出错: {error_msg}",
            "query": query
        }

@mcp.tool()
async def nl2sql_query_stream(query: str):
    """
    将自然语言查询转换为SQL，并使用流式响应返回结果。
    提供实时的思考过程和进度更新。
    
    参数:
        query: 自然语言查询
    
    返回:
        查询结果，包含SQL、执行结果等
    """
    try:
        # 记录开始处理请求
        start_time = datetime.datetime.now()
        request_id = hash(f"{query}_{start_time.isoformat()}")
        
        # 记录请求到审计日志
        audit_data = {
            "timestamp": start_time.isoformat(),
            "request_id": str(request_id),
            "action": "nl2sql_query_stream",
            "query": query,
            "status": "processing"
        }
        audit_logger.audit(json.dumps(audit_data))
        
        # 使用全局流式处理器实例
        global stream_processor
        
        # 创建一个哑回调函数，因为真正的回调会由adapter提供
        async def dummy_callback(content: str, metadata: dict):
            pass
        
        # 使用流式处理查询
        result = await stream_processor.process_stream(query, dummy_callback)
        
        # 记录处理结果到审计日志
        end_time = datetime.datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        audit_data.update({
            "end_timestamp": end_time.isoformat(),
            "duration_ms": duration_ms,
            "status": "completed" if not result.get("error") else "failed",
            "sql": result.get("sql", ""),
            "error": result.get("error", None)
        })
        audit_logger.audit(json.dumps(audit_data))
        
        # 返回结果
        return result
    except Exception as e:
        # 记录错误到审计日志和错误日志
        error_msg = str(e)
        logger.error(f"处理流式查询时出错: {error_msg}")
        
        # 错误审计
        if 'audit_data' in locals():
            audit_data.update({
                "status": "error",
                "error": error_msg
            })
            audit_logger.audit(json.dumps(audit_data))
        
        return {
            "success": False,
            "message": f"处理流式查询时出错: {error_msg}",
            "query": query
        }

@mcp.tool()
def list_database_tables():
    """
    列出数据库中的所有表
    
    返回:
        表列表
    """
    try:
        result = service.list_tables()
        return result
    except Exception as e:
        return {
            "success": False,
            "message": f"列出表时出错: {str(e)}"
        }

@mcp.tool()
def explain_table(table_name: str):
    """
    获取表结构的详细信息
    
    参数:
        table_name: 表名
    
    返回:
        表结构详细信息
    """
    try:
        result = service.explain_table(table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "message": f"解释表时出错: {str(e)}"
        }

@mcp.tool()
def get_business_overview():
    """
    获取数据库业务领域概览
    
    返回:
        业务领域概览信息
    """
    try:
        result = service.get_business_overview()
        return result
    except Exception as e:
        return {
            "success": False,
            "message": f"获取业务概览时出错: {str(e)}"
        }

@mcp.tool()
def refresh_metadata(force: bool = False):
    """
    刷新并保存元数据
    
    参数:
        force: 是否强制执行全量刷新，默认为False(增量刷新)
    
    返回:
        操作结果
    """
    try:
        service._refresh_metadata(force=force)
        refresh_type = "全量" if force else "增量"
        return {
            "success": True,
            "message": f"{refresh_type}元数据刷新完成"
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"刷新元数据时出错: {str(e)}"
        }

@mcp.tool()
def sql_optimize(sql: str, requirements: str = ""):
    """
    对SQL语句进行优化分析，提供性能改进建议和业务含义解读
    
    参数:
        sql: 需要优化的SQL语句
        requirements: 用户的优化需求说明
    
    返回:
        优化分析结果，包括业务分析、性能分析、瓶颈识别和优化建议
    """
    try:
        # 记录开始处理请求
        start_time = datetime.datetime.now()
        request_id = hash(f"{sql}_{requirements}_{start_time.isoformat()}")
        
        # 记录请求到审计日志
        audit_data = {
            "timestamp": start_time.isoformat(),
            "request_id": str(request_id),
            "action": "sql_optimize",
            "sql": sql,
            "requirements": requirements,
            "status": "processing"
        }
        audit_logger.audit(json.dumps(audit_data))
        
        # 调用SQL优化器处理
        result = sql_optimizer.process(sql, requirements)
        
        # 记录处理结果到审计日志
        end_time = datetime.datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        audit_data.update({
            "end_timestamp": end_time.isoformat(),
            "duration_ms": duration_ms,
            "status": result.get("status", "unknown"),
            "original_sql": sql,
        })
        audit_logger.audit(json.dumps(audit_data))
        
        # 返回结果
        return result
    except Exception as e:
        # 记录错误到审计日志和错误日志
        error_msg = str(e)
        logger.error(f"处理SQL优化请求时出错: {error_msg}")
        
        # 错误审计
        if 'audit_data' in locals():
            audit_data.update({
                "status": "error",
                "error": error_msg
            })
            audit_logger.audit(json.dumps(audit_data))
        
        return {
            "success": False,
            "message": f"处理SQL优化请求时出错: {error_msg}",
            "sql": sql,
            "requirements": requirements
        }

@mcp.tool()
def fix_sql(sql: str, error_message: str, requirements: str = ""):
    """
    修复SQL语句中的错误
    
    参数:
        sql: 含有错误的SQL语句
        error_message: 错误信息
        requirements: 用户的需求说明
    
    返回:
        修复结果，包括错误分析、修复后的SQL和业务逻辑说明
    """
    try:
        # 提取表信息
        table_info = sql_optimizer.extract_table_info(sql)
        
        # 调用SQL修复功能
        result = sql_optimizer.fix_sql(sql, error_message, requirements, table_info)
        
        return {
            "success": True,
            "original_sql": sql,
            "fix_result": result,
            "table_info": table_info
        }
    except Exception as e:
        error_msg = str(e)
        logger.error(f"修复SQL时出错: {error_msg}")
        
        return {
            "success": False,
            "message": f"修复SQL时出错: {error_msg}",
            "sql": sql,
            "error_message": error_message
        }

@mcp.tool()
def get_nl2sql_status():
    """
    获取当前NL2SQL处理状态
    
    返回:
        当前处理阶段、进度和阶段历史
    """
    try:
        # 使用全局流式处理器获取当前状态
        status = stream_processor.get_current_processing_status()
        return {
            "success": True,
            "current_status": status,
            "timestamp": datetime.datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"获取处理状态时出错: {str(e)}")
        return {
            "success": False,
            "message": f"获取处理状态时出错: {str(e)}",
            "timestamp": datetime.datetime.now().isoformat()
        }

@mcp.tool()
def list_llm_providers():
    """
    列出可用的LLM提供商
    
    返回:
        可用的LLM提供商列表
    """
    try:
        providers = service.get_available_llm_providers()
        return {
            "success": True,
            "providers": providers,
            "current_provider": service.llm_provider
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"列出LLM提供商时出错: {str(e)}"
        }

@mcp.tool()
def set_llm_provider(provider_name: str):
    """
    设置LLM提供商
    
    参数:
        provider_name: 提供商名称
    
    返回:
        操作结果
    """
    try:
        success = service.set_llm_provider(provider_name)
        if success:
            return {
                "success": True,
                "message": f"已切换到LLM提供商: {provider_name}"
            }
        else:
            return {
                "success": False,
                "message": f"切换LLM提供商失败，{provider_name} 可能不可用"
            }
    except Exception as e:
        return {
            "success": False,
            "message": f"设置LLM提供商时出错: {str(e)}"
        }

@mcp.tool()
def health():
    """
    健康检查工具
    
    返回服务的健康状态
    """
    return {
        "status": "healthy",
        "timestamp": datetime.datetime.now().isoformat(),
        "version": "1.0.0"
    }

@mcp.tool()
def status():
    """
    获取服务器状态
    """
    try:
        import psutil
        import platform
        
        process = psutil.Process(os.getpid())
        
        # 获取LLM提供商状态
        llm_providers = []
        try:
            from src.utils.llm_client import get_llm_providers
            providers = get_llm_providers()
            llm_providers = list(providers.keys()) if providers else []
        except Exception as e:
            logger.warning(f"获取LLM提供商信息失败: {str(e)}")
            # 提供一个默认列表，避免前端错误
            llm_providers = ["openai", "local"]
        
            return {
            "service": {
                "status": "running",
                "uptime": datetime.datetime.now().timestamp() - process.create_time(),
                "started_at": datetime.datetime.fromtimestamp(process.create_time()).isoformat(),
                "timestamp": datetime.datetime.now().isoformat(),
                "version": "1.0.0"
            },
            "system": {
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "cpu_count": psutil.cpu_count(),
                "memory_usage": {
                    "percent": psutil.virtual_memory().percent,
                    "used_mb": round(psutil.virtual_memory().used / (1024 * 1024), 2),
                    "total_mb": round(psutil.virtual_memory().total / (1024 * 1024), 2)
                },
                "process_memory_mb": round(process.memory_info().rss / (1024 * 1024), 2)
            },
            "config": {
                "host": os.getenv("SERVER_HOST", "0.0.0.0"),
                "port": int(os.getenv("SERVER_PORT", 8080)),
                "mcp_port": int(os.getenv("MCP_PORT", 3000)),
                "log_level": os.getenv("LOG_LEVEL", "INFO"),
                "llm_provider": os.getenv("LLM_PROVIDER", "openai")
            },
            "llm": {
                "providers": llm_providers,
                "default_provider": os.getenv("LLM_PROVIDER", "openai"),
                "default_model": os.getenv(f"{os.getenv('LLM_PROVIDER', 'openai').upper()}_MODEL", "unknown")
            }
            }
    except Exception as e:
        logger.error(f"获取服务器状态失败: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }

@mcp.tool()
def list_prompts():
    """列出可用的提示模板"""
    try:
        prompts = [
            # 基础NL2SQL提示
            {
                "id": "nl2sql",
                "name": "自然语言转SQL查询",
                "description": "将自然语言问题转换为SQL查询语句",
                "parameters": [
                    {"name": "question", "description": "自然语言问题", "required": True}
                ]
            },
            {
                "id": "data_analysis",
                "name": "数据分析方案",
                "description": "设计数据分析方案和SQL查询",
                "parameters": [
                    {"name": "analysis_task", "description": "分析任务描述", "required": True}
                ]
            },
            {
                "id": "table_exploration",
                "name": "表结构与数据探索",
                "description": "探索数据表结构和内容",
                "parameters": [
                    {"name": "table_name", "description": "要探索的表名", "required": True},
                    {"name": "database", "description": "数据库名称", "required": False}
                ]
            },
            
            # 高级分析提示
            {
                "id": "trend_analysis",
                "name": "趋势分析",
                "description": "分析数据的时间趋势并生成SQL查询",
                "parameters": [
                    {"name": "metric", "description": "要分析的指标", "required": True},
                    {"name": "time_period", "description": "时间范围，如'近7天'", "required": True},
                    {"name": "group_by", "description": "分组维度", "required": False}
                ]
            },
            {
                "id": "comparison_analysis",
                "name": "对比分析",
                "description": "比较不同条件下的数据并生成SQL查询",
                "parameters": [
                    {"name": "metric", "description": "要比较的指标", "required": True},
                    {"name": "dimension", "description": "比较维度", "required": True},
                    {"name": "conditions", "description": "比较条件", "required": True}
                ]
            },
            {
                "id": "anomaly_detection",
                "name": "异常检测",
                "description": "检测数据中的异常值并生成SQL查询",
                "parameters": [
                    {"name": "metric", "description": "要检测的指标", "required": True},
                    {"name": "threshold", "description": "异常阈值", "required": False}
                ]
            },
            
            # 数据可视化提示
            {
                "id": "chart_suggestion",
                "name": "图表建议",
                "description": "根据数据特征推荐合适的可视化图表",
                "parameters": [
                    {"name": "data_description", "description": "数据描述", "required": True}
                ]
            },
            {
                "id": "dashboard_design",
                "name": "仪表盘设计",
                "description": "设计数据仪表盘并生成相应的SQL查询",
                "parameters": [
                    {"name": "business_goal", "description": "业务目标", "required": True},
                    {"name": "metrics", "description": "要包含的关键指标", "required": True}
                ]
            },
            
            # 数据质量和管理提示
            {
                "id": "data_quality_check",
                "name": "数据质量检查",
                "description": "生成检查数据质量的SQL查询",
                "parameters": [
                    {"name": "table_name", "description": "要检查的表名", "required": True},
                    {"name": "check_type", "description": "检查类型，如'完整性'、'一致性'等", "required": False}
                ]
            },
            {
                "id": "data_profiling",
                "name": "数据画像",
                "description": "生成数据分布和统计特征的SQL查询",
                "parameters": [
                    {"name": "table_name", "description": "要分析的表名", "required": True},
                    {"name": "columns", "description": "要分析的列，逗号分隔", "required": False}
                ]
            },
            {
                "id": "index_recommendation",
                "name": "索引建议",
                "description": "分析查询模式并提供索引优化建议",
                "parameters": [
                    {"name": "table_name", "description": "表名", "required": True},
                    {"name": "query_pattern", "description": "查询模式描述", "required": True}
                ]
            },
            
            # 业务领域提示
            {
                "id": "business_kpi",
                "name": "业务KPI计算",
                "description": "根据业务KPI描述生成计算SQL",
                "parameters": [
                    {"name": "kpi_name", "description": "KPI名称", "required": True},
                    {"name": "kpi_definition", "description": "KPI定义", "required": True}
                ]
            },
            {
                "id": "business_report",
                "name": "业务报表生成",
                "description": "根据业务需求生成报表SQL",
                "parameters": [
                    {"name": "report_type", "description": "报表类型", "required": True},
                    {"name": "time_period", "description": "时间周期", "required": True},
                    {"name": "dimensions", "description": "报表维度", "required": False}
                ]
            }
        ]
        
        return {
            "prompts": prompts
        }
    except Exception as e:
        import traceback
        print(f"获取提示模板列表时出错: {str(e)}")
        print(traceback.format_exc())
        return {
            "error": {
                "message": f"获取提示模板列表时出错: {str(e)}"
            }
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

def main():
    """
    服务主函数
    """
    # 设置环境变量，禁用控制台日志输出
    os.environ["CONSOLE_LOGGING"] = "false"
    
    # 记录服务启动
    logger.info("启动Doris MCP NL2SQL服务 (SSE模式)")
    logger.info(f"当前工作目录: {os.getcwd()}")
    logger.info(f"Python版本: {sys.version}")
    logger.info(f"日志配置: LOG_DIR={os.getenv('LOG_DIR', 'logs')}, LOG_LEVEL={os.getenv('LOG_LEVEL', 'INFO')}")
    
    # 从环境变量读取配置
    host = os.getenv("SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("SERVER_PORT", "8080"))
    mcp_port = int(os.getenv("MCP_PORT", "3000"))
    
    # 获取允许的客户端源，默认为前端开发服务器
    allowed_origins = [origin.strip() for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:3100,http://localhost:5173").split(",")]
    
    # 添加'*'以允许所有源（仅用于测试）
    if '*' in allowed_origins:
        allowed_origins = ["*"]
        logger.info("允许所有跨域源（测试模式）")
    else:
        logger.info(f"允许的跨域源: {allowed_origins}")
    
    # 设置CORS相关的环境变量
    os.environ["MCP_ALLOWED_ORIGINS"] = ",".join(allowed_origins)
    os.environ["MCP_ALLOW_CREDENTIALS"] = "false"  # 禁用凭证要求，避免跨域问题
    os.environ["MCP_ALLOW_METHODS"] = "GET,POST,OPTIONS"
    os.environ["MCP_ALLOW_HEADERS"] = "*"
    
    # 确保FastMCP使用SSE模式
    # 创建SSE应用
    app = mcp.sse_app()
    
    # 添加自定义端点
    app = add_sse_endpoints(app)
    
    # 导入并添加MCP协议适配中间件
    from src.mcp_adapter import MCPAdapterMiddleware
    app.add_middleware(MCPAdapterMiddleware)
    logger.info("已添加MCP协议适配中间件，支持旧版客户端格式")
    
    # 添加CORS中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=False,  # 禁用凭证要求，避免CORS问题
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["*"],
        max_age=86400,  # 预检请求缓存时间（24小时）
    )
    
    # 日志显示启动信息
    logger.info(f"MCP SSE服务器准备启动于 {host}:{mcp_port}")
    logger.info(f"CORS配置: allow_origins={allowed_origins}, allow_credentials=False")
    logger.info("测试端点: /health, /sse-test")
    
    # 使用uvicorn启动服务
    import uvicorn
    uvicorn.run(
        app,
        host=host,
        port=mcp_port,
        log_level=os.getenv("LOG_LEVEL", "info").lower()
    )

if __name__ == "__main__":
    main() 
