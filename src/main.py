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

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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

# 加载环境变量
load_dotenv()

# 读取环境变量决定是否自动刷新元数据
auto_refresh_metadata = os.getenv("AUTO_REFRESH_METADATA", "false").lower() == "true"

# 初始化MCP服务器
mcp = FastMCP("Doris NL2SQL")

# 初始化NL2SQL服务
service = NL2SQLService(auto_refresh_metadata=auto_refresh_metadata)

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
        # 调用服务处理查询
        result = service.process_query(query)
        return result
    except Exception as e:
        return {
            "success": False,
            "message": f"处理查询时出错: {str(e)}",
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

def main():
    """主程序入口"""
    # 配置roots目录能力 - 允许客户端访问的根目录
    root_dirs = [
        {
            "name": "项目根目录",
            "path": os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
            "description": "Doris NL2SQL MCP服务项目根目录"
        },
        {
            "name": "数据目录",
            "path": os.path.abspath(os.path.join(os.path.dirname(__file__), "../data")),
            "description": "存储数据文件的目录"
        },
        {
            "name": "文档目录",
            "path": os.path.abspath(os.path.join(os.path.dirname(__file__), "../docs")),
            "description": "项目文档目录"
        }
    ]
    
    # 设置根目录
    for root_dir in root_dirs:
        try:
            path = root_dir["path"]
            # 确保目录存在
            os.makedirs(path, exist_ok=True)
            
            mcp.add_root(
                name=root_dir["name"],
                path=path,
                description=root_dir["description"]
            )
            print(f"已添加根目录: {root_dir['name']} -> {path}")
        except Exception as e:
            print(f"添加根目录 {root_dir['name']} 失败: {str(e)}")
    
    # 添加roots/list方法，用于列出可访问的根目录
    @mcp.method("roots/list")
    def list_roots():
        """列出可访问的根目录"""
        try:
            roots = []
            for root in mcp.get_roots():
                roots.append({
                    "name": root.name,
                    "path": root.path,
                    "description": root.description
                })
            
            return {
                "roots": roots
            }
        except Exception as e:
            import traceback
            print(f"获取根目录列表时出错: {str(e)}")
            print(traceback.format_exc())
            return {
                "error": {
                    "message": f"获取根目录列表时出错: {str(e)}"
                }
            }
    
    # 注册ping处理函数 - 用于检查服务器连接状态
    @mcp.method("ping")
    def ping_handler():
        """处理ping请求，返回服务器状态信息"""
        try:
            current_time = service.get_server_time() or str(datetime.datetime.now())
            return {
                "server": "Doris NL2SQL MCP Server",
                "version": "1.0.0",
                "status": "active",
                "timestamp": current_time
            }
        except Exception as e:
            import traceback
            print(f"处理ping请求时出错: {str(e)}")
            print(traceback.format_exc())
            return {
                "error": {
                    "message": f"处理ping请求时出错: {str(e)}"
                }
            }
    
    # 注册资源模板列表处理函数 - 用于提供可用的资源模板
    @mcp.method("resources/templates/list")
    def list_resource_templates():
        """列出可用的资源模板"""
        try:
            resourceTemplates = [
                # 数据库基础信息资源
                {
                    "uri": "doris://database/info",
                    "name": "数据库基本信息",
                    "description": "获取Doris数据库的基本信息，包括版本、连接状态等"
                },
                {
                    "uri": "doris://database/tables",
                    "name": "数据库表列表",
                    "description": "获取数据库中所有表的列表"
                },
                {
                    "uri": "doris://database/metadata",
                    "name": "数据库详细元数据",
                    "description": "获取数据库的详细元数据信息，包括表结构和关系"
                },
                {
                    "uri": "doris://database/stats",
                    "name": "数据库统计信息",
                    "description": "获取数据库的统计信息，如大小、表数量等"
                },
                
                # 表和视图资源
                {
                    "uri": "schema://{database}/{table}",
                    "name": "表结构信息",
                    "description": "获取指定表的结构信息",
                    "parameters": [
                        {"name": "database", "description": "数据库名称"},
                        {"name": "table", "description": "表名称"}
                    ]
                },
                {
                    "uri": "metadata://{database}/{table}",
                    "name": "表元数据",
                    "description": "获取指定表的元数据和业务含义",
                    "parameters": [
                        {"name": "database", "description": "数据库名称"},
                        {"name": "table", "description": "表名称"}
                    ]
                },
                {
                    "uri": "sample://{database}/{table}",
                    "name": "表数据样本",
                    "description": "获取指定表的数据样本（前10行）",
                    "parameters": [
                        {"name": "database", "description": "数据库名称"},
                        {"name": "table", "description": "表名称"}
                    ]
                },
                {
                    "uri": "stats://{database}/{table}",
                    "name": "表统计信息",
                    "description": "获取指定表的统计信息，包括行数、大小等",
                    "parameters": [
                        {"name": "database", "description": "数据库名称"},
                        {"name": "table", "description": "表名称"}
                    ]
                },
                {
                    "uri": "view://{database}/{view}",
                    "name": "视图定义",
                    "description": "获取指定视图的SQL定义",
                    "parameters": [
                        {"name": "database", "description": "数据库名称"},
                        {"name": "view", "description": "视图名称"}
                    ]
                },
                
                # 系统监控资源
                {
                    "uri": "system://status",
                    "name": "系统状态",
                    "description": "获取Doris NL2SQL服务的系统状态信息"
                },
                {
                    "uri": "system://performance",
                    "name": "系统性能",
                    "description": "获取Doris数据库的性能指标"
                },
                {
                    "uri": "system://logs",
                    "name": "系统日志",
                    "description": "获取最近的系统日志条目"
                },
                {
                    "uri": "system://audit",
                    "name": "审计日志",
                    "description": "获取Doris数据库的审计日志"
                },
                
                # 文档资源
                {
                    "uri": "docs://{topic}",
                    "name": "文档",
                    "description": "获取指定主题的文档",
                    "parameters": [
                        {"name": "topic", "description": "文档主题"}
                    ]
                },
                {
                    "uri": "docs://guide/getting-started",
                    "name": "入门指南",
                    "description": "获取Doris NL2SQL服务的入门指南"
                },
                {
                    "uri": "docs://guide/nl2sql",
                    "name": "NL2SQL使用指南",
                    "description": "获取自然语言转SQL功能的使用指南"
                },
                {
                    "uri": "docs://api/reference",
                    "name": "API参考",
                    "description": "获取API接口的详细参考文档"
                },
                
                # 其他高级资源
                {
                    "uri": "query://history",
                    "name": "查询历史",
                    "description": "获取最近执行的SQL查询历史"
                },
                {
                    "uri": "query://suggest",
                    "name": "查询建议",
                    "description": "获取基于当前数据库状态的查询建议"
                },
                {
                    "uri": "model://info",
                    "name": "模型信息",
                    "description": "获取当前使用的LLM模型信息"
                }
            ]
            
            return {
                "resourceTemplates": resourceTemplates
            }
        except Exception as e:
            import traceback
            print(f"获取资源模板列表时出错: {str(e)}")
            print(traceback.format_exc())
            return {
                "error": {
                    "message": f"获取资源模板列表时出错: {str(e)}"
                }
            }
    
    # 注册提示模板列表处理函数 - 用于提供可用的提示模板
    @mcp.method("prompts/list")
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
    
    # 注册所有提示模板
    from src.prompts.prompts import nl2sql, data_analysis, table_exploration
    
    # 定义提示模板函数
    @mcp.prompt("nl2sql", "将自然语言转换为SQL查询")
    def nl2sql_prompt(question: str):
        return [
            {
                "role": "user",
                "content": nl2sql(question)
            }
        ]
    
    @mcp.prompt("data_analysis", "设计数据分析方案")
    def data_analysis_prompt(analysis_task: str):
        return [
            {
                "role": "user",
                "content": data_analysis(analysis_task)
            }
        ]
    
    @mcp.prompt("table_exploration", "探索数据表结构和内容")
    def table_exploration_prompt(table_name: str, database: str = None):
        return [
            {
                "role": "user",
                "content": table_exploration(table_name, database)
            }
        ]
    
    # 注册schema资源
    @mcp.resource("schema://{database}/{table}")
    def schema_resource(database: str, table: str):
        try:
            from src.resources.schema_resources import get_schema
            return {"content": get_schema(database, table)}
        except Exception as e:
            return {"error": f"获取表结构时出错: {str(e)}"}
    
    # 注册metadata资源
    @mcp.resource("metadata://{database}/{table}")
    def metadata_resource(database: str, table: str):
        try:
            from src.resources.schema_resources import get_metadata
            return {"content": get_metadata(database, table)}
        except Exception as e:
            return {"error": f"获取表元数据时出错: {str(e)}"}
    
    # 注册docs资源
    @mcp.resource("docs://{topic}")
    def docs_resource(topic: str):
        try:
            from src.resources.docs_resources import get_docs
            return {"content": get_docs(topic)}
        except Exception as e:
            return {"error": f"获取文档时出错: {str(e)}"}
    
    # 注册系统状态资源
    @mcp.resource("system://status")
    def system_status_resource():
        try:
            from src.resources.system_resources import get_system_status
            return {"content": get_system_status()}
        except Exception as e:
            return {"error": f"获取系统状态时出错: {str(e)}"}
    
    # 注册系统性能资源
    @mcp.resource("system://performance")
    def system_performance_resource():
        try:
            from src.resources.system_resources import get_system_performance
            return {"content": get_system_performance()}
        except Exception as e:
            return {"error": f"获取系统性能时出错: {str(e)}"}
    
    # 注册系统日志资源
    @mcp.resource("system://logs")
    def system_logs_resource():
        try:
            from src.resources.system_resources import get_system_logs
            return {"content": get_system_logs()}
        except Exception as e:
            return {"error": f"获取系统日志时出错: {str(e)}"}
    
    # 注册审计日志资源
    @mcp.resource("system://audit")
    def system_audit_resource():
        try:
            from src.resources.system_resources import get_system_audit
            return {"content": get_system_audit()}
        except Exception as e:
            return {"error": f"获取审计日志时出错: {str(e)}"}
            
    # 注册额外的文档资源示例
    @mcp.resource("docs://guide/getting-started")
    def guide_getting_started_resource():
        try:
            from src.resources.docs_resources import get_docs
            return {"content": get_docs("guide/getting-started")}
        except Exception as e:
            return {"error": f"获取入门指南文档时出错: {str(e)}"}
    
    @mcp.resource("docs://guide/nl2sql")
    def guide_nl2sql_resource():
        try:
            from src.resources.docs_resources import get_docs
            return {"content": get_docs("guide/nl2sql")}
        except Exception as e:
            return {"error": f"获取NL2SQL使用指南文档时出错: {str(e)}"}
    
    @mcp.resource("docs://api/reference")
    def api_reference_resource():
        try:
            from src.resources.docs_resources import get_docs
            return {"content": get_docs("api/reference")}
        except Exception as e:
            return {"error": f"获取API参考文档时出错: {str(e)}"}
    
    # 注册数据库详细元数据资源
    @mcp.resource("doris://database/metadata")
    def doris_database_metadata():
        """获取Doris数据库的详细元数据信息"""
        try:
            # 获取表列表
            tables_result = service.list_tables()
            
            if not tables_result['success']:
                return {
                    "error": tables_result['message']
                }
            
            # 获取表详细信息
            tables_info = []
            for table in tables_result['tables']:
                table_name = table['name']
                table_result = service.explain_table(table_name)
                
                if table_result['success']:
                    tables_info.append({
                        "name": table_name,
                        "comment": table_result['table_comment'],
                        "business_description": table_result['business_description'],
                        "columns": [
                            {
                                "name": col.get('name', ''),
                                "type": col.get('type', ''),
                                "comment": col.get('comment', '')
                            }
                            for col in table_result['columns']
                        ],
                        "relationships": table_result['relationships']
                    })
            
            # 获取业务概览
            business_result = service.get_business_overview()
            
            # 组装数据库元数据信息
            metadata = {
                "database_name": tables_result['database'],
                "table_count": tables_result['count'],
                "tables": tables_info,
                "business_domain": business_result.get('business_domain', '') if business_result['success'] else '',
                "core_entities": business_result.get('core_entities', []) if business_result['success'] else [],
                "business_processes": business_result.get('business_processes', []) if business_result['success'] else []
            }
            
            return metadata
        except Exception as e:
            import traceback
            print(f"获取数据库元数据时出错: {str(e)}")
            print(traceback.format_exc())
            return {
                "error": f"获取数据库元数据时出错: {str(e)}"
            }
    
    # 注册数据库表列表资源
    @mcp.resource("doris://database/tables")
    def doris_database_tables():
        """获取Doris数据库的表列表"""
        try:
            tables_result = service.list_tables()
            
            if not tables_result['success']:
                return {
                    "error": tables_result['message']
                }
            
            return {
                "database_name": tables_result['database'],
                "tables": tables_result['tables'],
                "count": tables_result['count']
            }
        except Exception as e:
            import traceback
            print(f"获取数据库表列表时出错: {str(e)}")
            print(traceback.format_exc())
            return {
                "error": f"获取数据库表列表时出错: {str(e)}"
            }
    
    # 注册数据库统计信息资源
    @mcp.resource("doris://database/stats")
    def doris_database_stats():
        """获取Doris数据库的统计信息"""
        try:
            # 获取数据库指标
            from src.utils.db import execute_query
            
            # 获取数据库大小
            size_query = """
            SELECT 
                SUM(data_length + index_length) AS total_size 
            FROM 
                information_schema.tables 
            WHERE 
                table_schema = %s
            """
            size_result = execute_query(size_query.replace('%s', f"'{service.db_name}'"))
            
            # 获取最近修改的表
            recent_tables_query = """
            SELECT 
                table_name, 
                update_time 
            FROM 
                information_schema.tables 
            WHERE 
                table_schema = %s 
            ORDER BY 
                update_time DESC 
            LIMIT 5
            """
            recent_tables = execute_query(recent_tables_query.replace('%s', f"'{service.db_name}'"))
            
            # 返回统计信息
            return {
                "database_name": service.db_name,
                "table_count": service.get_cached_table_count(),
                "database_size": size_result[0]['total_size'] if size_result and size_result[0]['total_size'] else 0,
                "database_version": service.get_doris_version(),
                "server_time": service.get_server_time(),
                "recent_tables": recent_tables or []
            }
        except Exception as e:
            import traceback
            print(f"获取数据库统计信息时出错: {str(e)}")
            print(traceback.format_exc())
            return {
                "error": f"获取数据库统计信息时出错: {str(e)}"
            }
    
    # 获取命令行参数
    import argparse
    parser = argparse.ArgumentParser(description="Doris NL2SQL MCP服务")
    parser.add_argument("--host", default="0.0.0.0", help="服务器主机")
    parser.add_argument("--port", type=int, default=3000, help="服务器端口")
    args = parser.parse_args()
    
    print(f"启动Doris NL2SQL MCP服务，地址：http://{args.host}:{args.port}")
    print("已修改JSON编码器，中文字符不会被转义为Unicode编码")
    
    # 启动MCP服务器
    mcp.run(transport='sse')
    
    # 注意：MCP客户端需要通过SSE协议连接到此服务器
    # 客户端应使用正确的MCP客户端库（如官方的MCP-Client）连接，而不是直接使用HTTP API
    # 连接流程：
    # 1. 客户端首先连接到SSE端点（通常是/sse）
    # 2. 服务器返回一个会话ID和消息端点URL
    # 3. 客户端通过POST请求该消息端点来发送请求（如prompts/list, resources/read等）

if __name__ == "__main__":
    main() 
