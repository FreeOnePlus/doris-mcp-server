"""
Apache Doris SQL优化分析器

提供SQL智能优化分析功能，包括：
1. SQL语法和性能分析
2. 执行计划分析
3. 优化建议生成
4. 改进的SQL推荐
"""

import os
import re
import json
import logging
import time
import uuid
from typing import Dict, List, Any, Optional, Tuple, Union
from dotenv import load_dotenv
import requests

# 获取项目根目录
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

# 添加项目根目录到路径
import sys
sys.path.insert(0, PROJECT_ROOT)

# 导入相关模块
from src.utils.db import execute_query, get_db_connection, get_db_name
from src.utils.llm_client import get_llm_client, Message

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv(override=True)

class SQLOptimizer:
    """
    SQL智能优化分析器
    
    分析和优化DorisSQL语句，提供性能改进建议和业务含义解读
    """
    
    def __init__(self):
        """初始化SQL优化器"""
        # 初始化元数据提取器
        try:
            from src.utils.metadata_extractor import MetadataExtractor
            self.metadata_extractor = MetadataExtractor()
        except Exception as e:
            logger.error(f"创建元数据提取器出错: {str(e)}")
            self.metadata_extractor = None
            
        # 初始化优化关键词列表
        self._init_optimization_keywords()
        
        # LLM相关配置
        self.model_config = {
            "temperature": float(os.getenv("LLM_TEMPERATURE", "0.7")),
            "top_p": float(os.getenv("LLM_TOP_P", "0.95")),
            "max_tokens": int(os.getenv("LLM_MAX_TOKENS", "2048"))
        }
    
    def _init_optimization_keywords(self):
        """初始化SQL优化相关的关键词"""
        self.optimization_keywords = [
            "优化", "加速", "提速", "性能", "效率", "慢", "快", "耗时", "explain", 
            "profile", "分析", "改进", "提升", "调优", "瓶颈", "索引", "partition", 
            "分区", "分桶", "排序", "聚合", "join", "连接", "子查询", "物化视图"
        ]
    
    def is_optimization_task(self, query: str) -> Tuple[bool, float]:
        """
        判断是否为SQL优化任务
        
        Args:
            query: 用户查询或要求
            
        Returns:
            Tuple[bool, float]: (是否为优化任务, 置信度)
        """
        # 1. 本地关键词快速判断
        for keyword in self.optimization_keywords:
            if keyword in query:
                return True, 0.8
        
        # 2. 调用LLM进行二次判断
        try:
            llm_client = get_llm_client(stage="sql_optimization")
            
            system_prompt = """你是SQL专家，负责判断用户的请求是否与SQL优化、SQL分析、性能提升相关。
如果用户请求涉及SQL优化、性能分析、执行计划分析、索引建议等，判断为是；
如果用户只是想执行SQL或查询数据，而不关心优化，判断为否。

请返回JSON格式：{"is_optimization": true/false, "confidence": 0.0-1.0}"""
            
            user_prompt = f"用户请求: {query}"
            
            messages = [
                Message("system", system_prompt),
                Message("user", user_prompt)
            ]
            
            response = llm_client.chat(messages)
            
            try:
                result = json.loads(response.content)
                return result.get("is_optimization", False), result.get("confidence", 0.5)
            except json.JSONDecodeError:
                # 尝试使用正则表达式提取
                is_opt_match = re.search(r'"is_optimization":\s*(true|false)', response.content)
                conf_match = re.search(r'"confidence":\s*([0-9.]+)', response.content)
                
                is_optimization = is_opt_match and is_opt_match.group(1) == "true"
                confidence = float(conf_match.group(1)) if conf_match else 0.5
                
                return is_optimization, confidence
                
        except Exception as e:
            logger.error(f"LLM判断SQL优化任务时出错: {str(e)}")
            # 默认返回False，低置信度
            return False, 0.3
    
    def extract_table_info(self, sql: str) -> Dict[str, Any]:
        """
        从SQL中提取表信息
        
        Args:
            sql: SQL语句
            
        Returns:
            Dict: 表信息字典
        """
        try:
            # 使用正则表达式提取表名
            from_pattern = r'\bFROM\s+`?([a-zA-Z0-9_\.]+)`?'
            join_pattern = r'\bJOIN\s+`?([a-zA-Z0-9_\.]+)`?'
            
            tables = set()
            
            # 提取FROM子句中的表
            from_matches = re.finditer(from_pattern, sql, re.IGNORECASE)
            for match in from_matches:
                tables.add(match.group(1))
            
            # 提取JOIN子句中的表
            join_matches = re.finditer(join_pattern, sql, re.IGNORECASE)
            for match in join_matches:
                tables.add(match.group(1))
            
            # 获取每个表的详细信息
            table_info = {}
            for table in tables:
                # 处理可能带有数据库前缀的表名
                if '.' in table:
                    db_name, table_name = table.split('.', 1)
                else:
                    db_name = os.getenv("DB_DATABASE", "")
                    table_name = table
                
                # 获取表结构
                schema = self.metadata_extractor.get_table_schema(table_name, db_name)
                table_info[table] = schema
            
            return {
                "tables": list(tables),
                "table_details": table_info
            }
            
        except Exception as e:
            logger.error(f"提取表信息时出错: {str(e)}")
            return {"tables": [], "table_details": {}}
    
    def execute_with_profile(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL并收集profile信息
        
        Args:
            sql: SQL语句
            
        Returns:
            Dict: 执行结果和profile信息
        """
        try:
            # 生成唯一trace_id
            trace_id = str(uuid.uuid4()).replace('-', '')
            
            # 获取数据库连接信息
            db_host = os.getenv("DB_HOST", "localhost")
            db_web_port = os.getenv("DB_WEB_PORT", "8030")
            base_url = f"http://{db_host}:{db_web_port}"
            
            # 准备SQL语句，设置trace_id和开启profile
            profile_setup_sql = f"SET session_context='trace_id:{trace_id}'; SET enable_profile=true;"
            
            # 完整的SQL执行（包括准备和实际查询）
            full_sql = f"{profile_setup_sql}\n{sql}"
            
            # 收集explain信息
            explain_sql = f"EXPLAIN {sql}"
            explain_results = execute_query(explain_sql)
            
            # 执行查询
            logger.info(f"执行SQL（trace_id: {trace_id}）: {sql[:100]}...")
            results = execute_query(full_sql)
            
            # 等待一段时间，确保profile信息已经生成
            time.sleep(1)
            
            # 通过trace_id获取query_id
            query_id = None
            try:
                query_id_url = f"{base_url}/rest/v2/manager/query/trace_id/{trace_id}"
                logger.info(f"获取query_id，请求URL: {query_id_url}")
                
                query_id_response = requests.get(query_id_url)
                if query_id_response.status_code == 200:
                    query_id_data = query_id_response.json()
                    if query_id_data.get("code") == 0 and query_id_data.get("data"):
                        query_id = query_id_data["data"]
                        logger.info(f"成功获取query_id: {query_id}")
                    else:
                        logger.warning(f"获取query_id响应异常: {query_id_data}")
                else:
                    logger.warning(f"获取query_id失败，状态码: {query_id_response.status_code}")
            except Exception as e:
                logger.error(f"获取query_id时出错: {str(e)}")
            
            # 通过query_id获取profile信息
            profile_data = ""
            if query_id:
                try:
                    profile_url = f"{base_url}/rest/v2/manager/query/profile/text/{query_id}"
                    logger.info(f"获取profile信息，请求URL: {profile_url}")
                    
                    profile_response = requests.get(profile_url)
                    if profile_response.status_code == 200:
                        profile_json = profile_response.json()
                        if profile_json.get("code") == 0 and profile_json.get("data"):
                            profile_data = profile_json["data"].get("profile", "")
                            logger.info(f"成功获取profile信息，大小: {len(profile_data)} 字节")
                        else:
                            logger.warning(f"获取profile响应异常: {profile_json}")
                    else:
                        logger.warning(f"获取profile失败，状态码: {profile_response.status_code}")
                except Exception as e:
                    logger.error(f"获取profile时出错: {str(e)}")
            
            return {
                "status": "success",
                "results": results,
                "explain": explain_results,
                "profile": profile_data,
                "trace_id": trace_id,
                "query_id": query_id
            }
            
        except Exception as e:
            logger.error(f"执行SQL并收集profile时出错: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "results": [],
                "explain": [],
                "profile": ""
            }
    
    def fix_sql(self, sql: str, error_message: str, requirements: str = "", table_info: Dict = None, profile: str = "") -> Dict[str, Any]:
        """
        修复错误的SQL
        
        Args:
            sql: 原始SQL
            error_message: 错误信息
            requirements: 用户需求
            table_info: 表信息
            profile: 查询Profile信息（如果有）
            
        Returns:
            Dict: 修复结果
        """
        try:
            llm_client = get_llm_client(stage="sql_fix")
            
            # 准备表信息文本
            tables_info_text = ""
            if table_info and table_info.get("table_details"):
                for table_name, details in table_info.get("table_details", {}).items():
                    tables_info_text += f"表名: {table_name}\n"
                    if details and "columns" in details:
                        tables_info_text += "列信息:\n"
                        for col in details["columns"]:
                            col_name = col.get("name", "")
                            col_type = col.get("type", "")
                            col_comment = col.get("comment", "")
                            tables_info_text += f"  - {col_name} ({col_type}): {col_comment}\n"
                    tables_info_text += "\n"
            
            system_prompt = """你是Apache Doris SQL专家，负责修复SQL错误。
请分析错误原因，并提供以下内容：
1. 错误原因分析
2. 修复后的SQL
3. SQL的业务含义和逻辑说明

返回格式：
```json
{
  "error_analysis": "错误原因分析...",
  "fixed_sql": "修复后的SQL...",
  "business_meaning": "业务含义说明...",
  "sql_logic": "SQL逻辑说明..."
}
```"""
            
            # 准备Profile片段（如果有）
            profile_text = ""
            if profile:
                profile_text = f"\n查询Profile信息（可能包含错误信息）:\n{profile[:2000]}"
                if len(profile) > 2000:
                    profile_text += "\n... (已省略更多内容)"
            
            user_prompt = f"""需要修复的SQL:
```sql
{sql}
```

错误信息:
{error_message}{profile_text}

用户需求:
{requirements}

相关表的结构信息:
{tables_info_text}

请提供错误分析、修复后的SQL以及业务含义和逻辑说明。"""

            messages = [
                Message("system", system_prompt),
                Message("user", user_prompt)
            ]
            
            response = llm_client.chat(messages)
            
            # 解析LLM响应
            fix_result = self._parse_json_response(response.content)
            
            return fix_result
            
        except Exception as e:
            logger.error(f"修复SQL时出错: {str(e)}")
            return {
                "error": f"修复SQL时出错: {str(e)}",
                "error_analysis": "无法分析错误",
                "fixed_sql": sql
            }
    
    def optimize_sql(self, sql: str, explain_results: List[Dict], requirements: str = "", table_info: Dict = None, profile: str = "") -> Dict[str, Any]:
        """
        优化SQL
        
        Args:
            sql: 原始SQL
            explain_results: EXPLAIN结果
            requirements: 用户需求
            table_info: 表信息
            profile: 查询Profile信息
            
        Returns:
            Dict: 优化结果
        """
        try:
            llm_client = get_llm_client(stage="sql_optimization")
            
            # 准备表信息文本
            tables_info_text = ""
            if table_info and table_info.get("table_details"):
                for table_name, details in table_info.get("table_details", {}).items():
                    tables_info_text += f"表名: {table_name}\n"
                    if details and "columns" in details:
                        tables_info_text += "列信息:\n"
                        for col in details["columns"]:
                            col_name = col.get("name", "")
                            col_type = col.get("type", "")
                            col_comment = col.get("comment", "")
                            tables_info_text += f"  - {col_name} ({col_type}): {col_comment}\n"
                    tables_info_text += "\n"
            
            system_prompt = """你是Apache Doris SQL优化专家，负责分析和优化SQL性能。
请根据提供的SQL、执行计划、Profile和表结构，提供以下内容：
1. SQL的业务含义分析
2. 执行计划分析和性能瓶颈识别
3. 具体的优化建议，包括索引优化、查询重写、分区优化等
4. 1-2个优化后的SQL及其性能提升点

返回格式：
```json
{
  "business_analysis": "SQL业务含义分析...",
  "performance_analysis": "执行计划和性能分析...",
  "bottlenecks": ["瓶颈1", "瓶颈2", ...],
  "optimization_suggestions": ["建议1", "建议2", ...],
  "optimized_queries": [
    {
      "sql": "优化后的SQL1...",
      "explanation": "优化点说明...",
      "expected_improvement": "预期性能提升..."
    },
    {
      "sql": "优化后的SQL2...",
      "explanation": "优化点说明...",
      "expected_improvement": "预期性能提升..."
    }
  ]
}
```"""
            
            # 准备EXPLAIN结果文本
            explain_text = json.dumps(explain_results, ensure_ascii=False, indent=2)
            
            # 准备Profile片段（可能很长，只取前5000个字符）
            profile_text = ""
            if profile:
                profile_text = f"查询Profile信息（前5000字符）:\n{profile[:5000]}"
                if len(profile) > 5000:
                    profile_text += "\n... (已省略更多内容)"
            
            user_prompt = f"""需要优化的SQL:
```sql
{sql}
```

EXPLAIN结果:
```
{explain_text}
```

{profile_text}

用户需求:
{requirements}

相关表的结构信息:
{tables_info_text}

请提供SQL业务含义分析、性能分析、优化建议和优化后的SQL。"""

            messages = [
                Message("system", system_prompt),
                Message("user", user_prompt)
            ]
            
            response = llm_client.chat(messages)
            
            # 解析LLM响应
            optimization_result = self._parse_json_response(response.content)
            
            return optimization_result
            
        except Exception as e:
            logger.error(f"优化SQL时出错: {str(e)}")
            return {
                "error": f"优化SQL时出错: {str(e)}",
                "business_analysis": "无法分析SQL的业务含义",
                "performance_analysis": "无法分析性能问题",
                "bottlenecks": [],
                "optimization_suggestions": []
            }
    
    def _parse_json_response(self, content: str) -> Dict:
        """
        解析LLM返回的JSON内容
        
        Args:
            content: LLM响应内容
            
        Returns:
            Dict: 解析后的JSON
        """
        try:
            # 尝试提取```json代码块
            json_block_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
            json_block_matches = re.findall(json_block_pattern, content)
            
            for json_block in json_block_matches:
                try:
                    # 清理可能导致解析失败的控制字符
                    cleaned_block = json_block.strip()
                    return json.loads(cleaned_block)
                except json.JSONDecodeError:
                    continue
            
            # 如果无法从代码块中提取，尝试直接解析整个内容
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass
            
            # 尝试查找和提取JSON块
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_part = content[json_start:json_end]
                try:
                    return json.loads(json_part)
                except json.JSONDecodeError:
                    pass
            
            # 所有方法都失败，返回原始内容
            return {"content": content}
            
        except Exception as e:
            logger.error(f"解析JSON响应时出错: {str(e)}")
            return {"error": f"解析响应失败: {str(e)}", "content": content}
    
    def _optimize_with_llm_only(self, request_id: str, sql: str, requirements: str, table_info: Dict, error_message: str) -> Dict[str, Any]:
        """
        仅使用LLM能力优化SQL，不依赖实际执行结果
        
        Args:
            request_id: 请求ID
            sql: SQL语句
            requirements: 用户需求
            table_info: 表信息
            error_message: 错误信息
            
        Returns:
            Dict: 优化结果
        """
        logger.info(f"使用纯LLM模式优化SQL: {sql[:100]}...")
        
        try:
            llm_client = get_llm_client(stage="sql_optimization")
            
            # 准备表信息文本
            tables_info_text = ""
            if table_info and table_info.get("table_details"):
                for table_name, details in table_info.get("table_details", {}).items():
                    tables_info_text += f"表名: {table_name}\n"
                    if details and "columns" in details:
                        tables_info_text += "列信息:\n"
                        for col in details["columns"]:
                            col_name = col.get("name", "")
                            col_type = col.get("type", "")
                            col_comment = col.get("comment", "")
                            tables_info_text += f"  - {col_name} ({col_type}): {col_comment}\n"
                    tables_info_text += "\n"
            
            system_prompt = """你是Apache Doris SQL优化专家，负责分析和优化SQL性能。
此次优化是在不能执行SQL的情况下进行的，仅凭你的知识和理解来分析SQL，请提供以下内容：
1. SQL的业务含义分析
2. 基于SQL语法和结构的性能瓶颈识别
3. 具体的优化建议，包括索引优化、查询重写、分区优化等
4. 1-2个优化后的SQL及其性能提升点

返回格式：
```json
{
  "business_analysis": "SQL业务含义分析...",
  "performance_analysis": "执行计划和性能分析...",
  "bottlenecks": ["瓶颈1", "瓶颈2", ...],
  "optimization_suggestions": ["建议1", "建议2", ...],
  "optimized_queries": [
    {
      "sql": "优化后的SQL1...",
      "explanation": "优化点说明...",
      "expected_improvement": "预期性能提升..."
    },
    {
      "sql": "优化后的SQL2...",
      "explanation": "优化点说明...",
      "expected_improvement": "预期性能提升..."
    }
  ]
}
```"""
            
            user_prompt = f"""需要优化的SQL:
```sql
{sql}
```

执行SQL时遇到的错误:
{error_message}

用户需求:
{requirements}

相关表的结构信息(可能不完整):
{tables_info_text}

请注意：无法在当前数据库中执行此SQL，所以没有真实的执行计划和Profile数据作为参考。
请基于你的SQL知识和Doris优化经验，尽可能给出合理的优化建议。"""

            messages = [
                Message("system", system_prompt),
                Message("user", user_prompt)
            ]
            
            response = llm_client.chat(messages)
            
            # 解析LLM响应
            optimization_result = self._parse_json_response(response.content)
            
            # 添加免责声明
            optimization_result["disclaimer"] = "注意：此优化建议仅基于LLM的能力生成，无法执行原SQL获取实际运行参数作为参考。建议仅供参考，请谨慎甄别。"
            
            return {
                "request_id": request_id,
                "status": "llm_only",
                "message": "SQL无法在当前数据库执行，以下优化建议仅基于LLM能力生成，没有实际运行参数作为参考，请谨慎甄别。",
                "original_sql": sql,
                "error": error_message,
                "optimization_result": optimization_result,
                "table_info": table_info,
                "is_llm_only": True
            }
            
        except Exception as e:
            logger.error(f"纯LLM优化SQL时出错: {str(e)}")
            return {
                "request_id": request_id,
                "status": "error",
                "message": "SQL无法执行，且LLM优化也失败了。",
                "original_sql": sql,
                "error": f"执行错误: {error_message}; LLM优化错误: {str(e)}",
                "is_llm_only": True
            }
            
    def process(self, sql: str, requirements: str = "") -> Dict[str, Any]:
        """
        处理SQL优化请求
        
        Args:
            sql: SQL语句
            requirements: 用户需求
            
        Returns:
            Dict: 处理结果
        """
        # 生成唯一请求ID
        request_id = str(uuid.uuid4())
        
        # 记录开始处理
        logger.info(f"开始处理SQL优化请求 {request_id}: {sql[:100]}...")
        
        # 1. 判断是否为SQL优化任务
        is_optimization, confidence = self.is_optimization_task(requirements)
        
        if not is_optimization and confidence > 0.7:
            return {
                "request_id": request_id,
                "status": "error",
                "message": "这不是SQL优化相关的任务，请提供与SQL优化、性能分析相关的需求。",
                "confidence": confidence
            }
        
        # 2. 提取表信息
        table_info = self.extract_table_info(sql)
        
        # 3. 尝试执行SQL并收集profile信息
        try:
            execution_result = self.execute_with_profile(sql)
            
            # 检查执行是否成功
            if execution_result["status"] == "error":
                # 执行失败，提取错误信息
                error_message = execution_result.get("error", "未知错误")
                
                # 检查是否是表不存在或权限错误等无法执行的情况
                cannot_execute = any(keyword in error_message.lower() for keyword in [
                    "table not found", "database not found", "表不存在", "数据库不存在",
                    "permission denied", "access denied", "权限拒绝", "无权访问",
                    "syntax error", "语法错误", "unknown column", "未知列"
                ])
                
                if cannot_execute:
                    logger.warning(f"SQL无法在当前数据库执行: {error_message}，将使用纯LLM方式优化")
                    # 使用纯LLM方式进行优化
                    return self._optimize_with_llm_only(request_id, sql, requirements, table_info, error_message)
                else:
                    # 尝试修复SQL
                    fix_result = self.fix_sql(
                        sql, 
                        error_message, 
                        requirements, 
                        table_info, 
                        execution_result.get("profile", "")
                    )
                    
                    return {
                        "request_id": request_id,
                        "status": "fixed",
                        "original_sql": sql,
                        "error": error_message,
                        "fix_result": fix_result,
                        "table_info": table_info
                    }
            else:
                # 执行成功，生成优化建议
                optimization_result = self.optimize_sql(
                    sql, 
                    execution_result.get("explain", []), 
                    requirements, 
                    table_info, 
                    execution_result.get("profile", "")
                )
                
                return {
                    "request_id": request_id,
                    "status": "success",
                    "original_sql": sql,
                    "execution_result": {
                        "row_count": len(execution_result.get("results", [])) if execution_result.get("results") else 0,
                        "explain": execution_result.get("explain", []),
                        "profile": execution_result.get("profile", "")
                    },
                    "optimization_result": optimization_result,
                    "table_info": table_info
                }
        except Exception as e:
            # SQL执行或优化过程中发生异常
            logger.error(f"SQL执行或优化过程中发生异常: {str(e)}")
            # 使用纯LLM方式进行优化
            return self._optimize_with_llm_only(request_id, sql, requirements, table_info, str(e)) 