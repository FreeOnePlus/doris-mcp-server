import json
import pandas as pd
from typing import Dict, List, Any, Optional

from ..utils.db import execute_query, execute_query_df, is_read_only_query
from ..utils.sql_samples import sql_sample_manager

def run_query(sql: str) -> str:
    """
    执行只读SQL查询
    
    Args:
        sql: SQL查询语句
        
    Returns:
        str: 查询结果
    """
    # 安全检查：确保是只读查询
    if not is_read_only_query(sql):
        return "错误: 仅支持查询操作，不允许修改数据库"
    
    try:
        # 使用pandas以便格式化输出
        df = execute_query_df(sql)
        
        # 检查结果大小
        if len(df) > 100:
            # 对于大结果集，只显示前100行
            result = f"查询结果包含 {len(df)} 行。为限制输出，仅显示前100行:\n\n"
            result += df.head(100).to_string(index=False)
        else:
            # 对于小结果集，显示全部
            result = df.to_string(index=False)
        
        return result
    except Exception as e:
        return f"执行查询时出错: {str(e)}"

def analyze_query(natural_language: str) -> str:
    """
    分析自然语言并生成SQL建议
    
    Args:
        natural_language: 自然语言查询
        
    Returns:
        str: 生成的SQL建议
    """
    # 查找相似的SQL样本
    similar_samples = sql_sample_manager.find_similar_samples(natural_language, top_k=2)
    
    # 构建分析结果
    result = f"### 基于您的查询: \"{natural_language}\"\n\n"
    
    # 添加相似样本
    if similar_samples:
        result += "找到以下相似的SQL示例:\n\n"
        
        for i, sample in enumerate(similar_samples):
            similarity = sample.get("similarity", 0) * 100
            result += f"#### 示例 {i+1} (相似度: {similarity:.1f}%)\n"
            result += f"描述: {sample['description']}\n"
            result += f"原始问题: {sample['nl_query']}\n"
            result += f"SQL: \n```sql\n{sample['sql']}\n```\n"
            if sample.get("explanation"):
                result += f"解释: {sample['explanation']}\n\n"
    
    # 添加建议的SQL
    result += "### 建议的SQL查询:\n"
    
    # 根据相似样本提供建议
    # 在实际应用中，这里可以集成更高级的NL2SQL模型
    if similar_samples:
        most_similar = similar_samples[0]
        suggested_sql = most_similar["sql"]
        result += f"```sql\n{suggested_sql}\n```\n\n"
        result += "注意: 此查询基于相似样本生成，可能需要根据您的具体需求进行调整。\n"
    else:
        result += "无法生成SQL建议，请提供更多信息或使用更具体的查询。\n"
    
    return result

def explain_table(table_name: str, database: Optional[str] = None) -> str:
    """
    解释表的业务用途和结构
    
    Args:
        table_name: 表名
        database: 数据库名，如果为None则使用当前数据库
        
    Returns:
        str: 表的解释
    """
    try:
        # 构建完整表名
        full_table_name = f"`{database}`.`{table_name}`" if database else f"`{table_name}`"
        
        # 获取表信息
        if database:
            table_info_query = f"""
                SELECT 
                    TABLE_SCHEMA, 
                    TABLE_NAME, 
                    TABLE_TYPE, 
                    ENGINE, 
                    TABLE_COMMENT
                FROM 
                    information_schema.TABLES 
                WHERE 
                    TABLE_SCHEMA = '{database}' 
                    AND TABLE_NAME = '{table_name}'
            """
        else:
            table_info_query = f"""
                SELECT 
                    TABLE_SCHEMA, 
                    TABLE_NAME, 
                    TABLE_TYPE, 
                    ENGINE, 
                    TABLE_COMMENT
                FROM 
                    information_schema.TABLES 
                WHERE 
                    TABLE_NAME = '{table_name}'
            """
        
        table_info = execute_query(table_info_query)
        
        if not table_info:
            return f"找不到表 {full_table_name}"
        
        # 获取表的第一行信息
        table_info = table_info[0]
        db_name = table_info["TABLE_SCHEMA"]
        
        # 获取列信息
        columns_query = f"""
            SELECT 
                COLUMN_NAME, 
                COLUMN_TYPE, 
                IS_NULLABLE, 
                COLUMN_KEY, 
                COLUMN_DEFAULT, 
                COLUMN_COMMENT
            FROM 
                information_schema.COLUMNS
            WHERE 
                TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{table_name}'
            ORDER BY 
                ORDINAL_POSITION
        """
        
        columns = execute_query(columns_query)
        
        # 获取索引信息
        indexes_query = f"""
            SELECT 
                INDEX_NAME, 
                COLUMN_NAME, 
                NON_UNIQUE
            FROM 
                information_schema.STATISTICS
            WHERE 
                TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{table_name}'
            ORDER BY 
                INDEX_NAME, 
                SEQ_IN_INDEX
        """
        
        indexes = execute_query(indexes_query)
        
        # 格式化表信息
        result = f"# 表 {full_table_name} 分析\n\n"
        result += f"## 基本信息\n\n"
        result += f"- 表名: {table_name}\n"
        result += f"- 数据库: {db_name}\n"
        result += f"- 类型: {table_info['TABLE_TYPE']}\n"
        result += f"- 引擎: {table_info['ENGINE']}\n"
        
        # 添加表注释（业务说明）
        if table_info["TABLE_COMMENT"]:
            result += f"- 业务描述: {table_info['TABLE_COMMENT']}\n"
        else:
            result += f"- 业务描述: 无\n"
        
        # 添加列信息
        result += f"\n## 列信息\n\n"
        result += "| 列名 | 类型 | 可空 | 键 | 默认值 | 说明 |\n"
        result += "| ---- | ---- | ---- | -- | ------ | ---- |\n"
        
        for col in columns:
            nullable = "是" if col["IS_NULLABLE"] == "YES" else "否"
            key = col["COLUMN_KEY"] if col["COLUMN_KEY"] else ""
            default = col["COLUMN_DEFAULT"] if col["COLUMN_DEFAULT"] is not None else ""
            comment = col["COLUMN_COMMENT"] if col["COLUMN_COMMENT"] else ""
            
            result += f"| {col['COLUMN_NAME']} | {col['COLUMN_TYPE']} | {nullable} | {key} | {default} | {comment} |\n"
        
        # 添加索引信息
        if indexes:
            # 整理索引数据
            index_dict = {}
            for idx in indexes:
                index_name = idx["INDEX_NAME"]
                if index_name not in index_dict:
                    index_dict[index_name] = {
                        "columns": [],
                        "unique": not idx["NON_UNIQUE"]
                    }
                index_dict[index_name]["columns"].append(idx["COLUMN_NAME"])
            
            result += f"\n## 索引信息\n\n"
            result += "| 索引名 | 唯一性 | 列 |\n"
            result += "| ------ | ------ | -- |\n"
            
            for index_name, index_info in index_dict.items():
                unique = "唯一" if index_info["unique"] else "非唯一"
                columns = ", ".join(index_info["columns"])
                result += f"| {index_name} | {unique} | {columns} |\n"
        
        # 添加表关系信息
        try:
            relations_query = f"""
                SELECT 
                    k.COLUMN_NAME, 
                    k.REFERENCED_TABLE_NAME, 
                    k.REFERENCED_COLUMN_NAME
                FROM 
                    information_schema.KEY_COLUMN_USAGE k
                WHERE 
                    k.TABLE_SCHEMA = '{db_name}'
                    AND k.TABLE_NAME = '{table_name}' 
                    AND k.REFERENCED_TABLE_NAME IS NOT NULL
            """
            
            relations = execute_query(relations_query)
            
            if relations:
                result += f"\n## 表关系\n\n"
                result += "| 本表列 | 关联表 | 关联表列 |\n"
                result += "| ------ | ------ | -------- |\n"
                
                for rel in relations:
                    result += f"| {rel['COLUMN_NAME']} | {rel['REFERENCED_TABLE_NAME']} | {rel['REFERENCED_COLUMN_NAME']} |\n"
        except:
            # 如果查询外键关系失败，可能是Doris不支持或没有配置
            pass
        
        return result
    except Exception as e:
        return f"分析表 {table_name} 时出错: {str(e)}" 