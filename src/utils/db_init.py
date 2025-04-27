#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Doris 元数据表初始化

本模块用于创建和初始化存储元数据的数据库表
用于在服务器启动前执行，确保所需的元数据表存在
"""

import logging
from .db import execute_query
from src.prompts.metadata_schema import METADATA_DB_NAME, METADATA_TABLES, CREATE_DATABASE_SQL

# 配置日志
logger = logging.getLogger(__name__)

def create_metadata_database():
    """创建元数据数据库"""
    try:
        # 创建元数据数据库
        execute_query(CREATE_DATABASE_SQL)
        logger.info(f"已创建元数据数据库: {METADATA_DB_NAME}")
        return True
    except Exception as e:
        logger.error(f"创建元数据数据库时出错: {str(e)}")
        return False

def create_metadata_tables():
    """创建所有元数据表"""
    try:
        # 首先确保数据库存在
        if not create_metadata_database():
            return False
        
        # 创建所有元数据表
        success = True
        for table_name, create_table_sql in METADATA_TABLES.items():
            try:
                # 检查表是否已存在
                check_table_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}` LIKE '{table_name}'"
                table_exists = execute_query(check_table_query)
                
                if not table_exists:
                    # 执行建表语句
                    execute_query(create_table_sql)
                    logger.info(f"已创建元数据表: {table_name}")
                else:
                    logger.info(f"元数据表已存在: {table_name}")
            except Exception as e:
                logger.error(f"创建元数据表 {table_name} 时出错: {str(e)}")
                success = False
        
        return success
    except Exception as e:
        logger.error(f"创建元数据表时出错: {str(e)}")
        return False

def init_metadata_tables():
    """初始化元数据表，在服务器启动时调用"""
    logger.info("开始初始化元数据表...")
    try:
        # 输出元数据数据库名称和表数量
        logger.info(f"元数据数据库名称: {METADATA_DB_NAME}")
        logger.info(f"需要初始化的元数据表数量: {len(METADATA_TABLES)}")
        
        # 检查数据库连接
        from src.utils.db import test_connection
        conn_test = test_connection()
        logger.info(f"数据库连接测试结果: {conn_test}")
        
        # 初始化元数据表
        success = create_metadata_tables()
        
        if success:
            logger.info("元数据表初始化完成")
        else:
            logger.warning("元数据表初始化过程中出现错误")
            
        # 列出已创建的表
        try:
            # 检查数据库是否存在
            check_db_query = f"SHOW DATABASES LIKE '{METADATA_DB_NAME}'"
            db_exists = execute_query(check_db_query)
            
            if db_exists:
                # 列出所有表
                list_tables_query = f"SHOW TABLES FROM `{METADATA_DB_NAME}`"
                tables = execute_query(list_tables_query)
                if tables:
                    table_names = [list(t.values())[0] for t in tables]
                    logger.info(f"已创建的元数据表: {table_names}")
                else:
                    logger.warning(f"元数据数据库 {METADATA_DB_NAME} 中没有表")
            else:
                logger.warning(f"元数据数据库 {METADATA_DB_NAME} 不存在")
        except Exception as e:
            logger.error(f"列出已创建的元数据表时出错: {str(e)}")
            
        return success
    except Exception as e:
        logger.error(f"初始化元数据表时发生未预期的错误: {str(e)}", exc_info=True)
        return False 