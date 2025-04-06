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
            # 添加profile收集语句
            profiled_sql = "set enable_profile=true;\n" + sql
            
            # 执行SQL
            results = execute_query(profiled_sql)
            
            # 收集explain信息
            explain_sql = f"EXPLAIN {sql}"
            explain_results = execute_query(explain_sql)
            
            return {
                "status": "success",
                "results": results,
                "explain": explain_results,
                # profile信息需要通过接口获取，这里预留
                "profile": "预留profile信息获取接口"
            }
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"执行SQL时出错: {error_message}")
            
            return {
                "status": "error",
                "error": error_message
            }
    
    def fix_sql(self, sql: str, error_message: str, requirements: str = "", table_info: Dict = None) -> Dict[str, Any]:
        """
        修复错误的SQL
        
        Args:
            sql: 原始SQL
            error_message: 错误信息
            requirements: 用户需求
            table_info: 表信息
            
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
            
            user_prompt = f"""需要修复的SQL:
```sql
{sql}
```

错误信息:
{error_message}

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
    
    def optimize_sql(self, sql: str, explain_results: List[Dict], requirements: str = "", table_info: Dict = None) -> Dict[str, Any]:
        """
        优化SQL
        
        Args:
            sql: 原始SQL
            explain_results: EXPLAIN结果
            requirements: 用户需求
            table_info: 表信息
            
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
请根据提供的SQL、执行计划和表结构，提供以下内容：
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
            
            user_prompt = f"""需要优化的SQL:
```sql
{sql}
```

EXPLAIN结果:
```
{json.dumps(explain_results, ensure_ascii=False, indent=2)}
```

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
        
        # 3. 执行SQL并收集profile信息
        execution_result = self.execute_with_profile(sql)
        
        # 4. 处理执行结果
        if execution_result["status"] == "error":
            # 执行失败，尝试修复SQL
            fix_result = self.fix_sql(
                sql, 
                execution_result["error"], 
                requirements, 
                table_info
            )
            
            return {
                "request_id": request_id,
                "status": "fixed",
                "original_sql": sql,
                "error": execution_result["error"],
                "fix_result": fix_result,
                "table_info": table_info
            }
        else:
            # 执行成功，生成优化建议
            optimization_result = self.optimize_sql(
                sql, 
                execution_result["explain"], 
                requirements, 
                table_info
            )
            
            return {
                "request_id": request_id,
                "status": "success",
                "original_sql": sql,
                "execution_result": {
                    "row_count": len(execution_result["results"]) if execution_result["results"] else 0,
                    "explain": execution_result["explain"],
                    "profile": execution_result["profile"]
                },
                "optimization_result": optimization_result,
                "table_info": table_info
            } 