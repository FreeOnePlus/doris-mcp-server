#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Doris 元数据表模式定义

本模块集中管理所有元数据表结构的定义
提供统一的表创建语句和字段定义
"""

# 元数据数据库名称
METADATA_DB_NAME = "doris_metadata"

# 数据库创建语句
CREATE_DATABASE_SQL = """
CREATE DATABASE IF NOT EXISTS `doris_metadata`;
"""

# 元数据表创建语句
METADATA_TABLES = {
    # 业务元数据表
    "business_metadata": """
CREATE TABLE IF NOT EXISTS `doris_metadata`.`business_metadata` (
  `db_name` varchar(100) NOT NULL COMMENT '数据库名',
  `table_name` varchar(100) NULL COMMENT '表名',
  `metadata_type` varchar(50) NOT NULL COMMENT '元数据类型',
  `metadata_value` text NULL COMMENT '元数据内容',
  `business_keywords` text NULL COMMENT '业务关键词',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(`db_name`, `table_name`, `metadata_type`)
COMMENT '业务元数据表'
DISTRIBUTED BY HASH(`db_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3"
);
    """,
    
    # 表结构元数据表
    "table_metadata": """
CREATE TABLE IF NOT EXISTS `doris_metadata`.`table_metadata` (
  `database_name` varchar(100) NOT NULL COMMENT '数据库名',
  `table_name` varchar(100) NOT NULL COMMENT '表名',
  `table_type` varchar(50) NULL COMMENT '表类型',
  `engine` varchar(50) NULL COMMENT '存储引擎',
  `table_comment` text NULL COMMENT '表注释',
  `column_info` text NULL COMMENT '列信息',
  `partition_info` text NULL COMMENT '分区信息',
  `business_summary` text NULL COMMENT '业务含义',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(`database_name`, `table_name`)
COMMENT '表结构元数据'
DISTRIBUTED BY HASH(`database_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3"
);
    """,
    
    # 业务关键词表
    "business_keywords": """
CREATE TABLE IF NOT EXISTS `doris_metadata`.`business_keywords` (
  `database_name` varchar(100) NULL COMMENT '数据库名称',
  `keyword` varchar(255) NULL COMMENT '业务关键词',
  `confidence` float NULL COMMENT '置信度',
  `category` varchar(100) NULL COMMENT '类别',
  `source` varchar(100) NULL COMMENT '来源',
  `create_time` datetime NULL COMMENT '创建时间',
  `update_time` datetime NULL COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(`database_name`, `keyword`)
COMMENT '业务关键词表'
DISTRIBUTED BY HASH(`database_name`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3"
);
    """,
    
    # NL2SQL问答示例表
    "qa_examples": """
CREATE TABLE IF NOT EXISTS `doris_metadata`.`qa_examples` (
  `id` bigint NOT NULL AUTO_INCREMENT(1),
  `question` text NOT NULL,
  `sql` text NOT NULL,
  `tables` text NULL,
  `explanation` text NULL,
  `created_at` datetime NOT NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'NL2SQL问答示例'
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3"
);
    """,
    
    # SQL查询模式表
    "sql_patterns": """
CREATE TABLE IF NOT EXISTS `doris_metadata`.`sql_patterns` (
  `pattern_id` bigint NOT NULL COMMENT '模式ID',
  `sql_type` varchar(20) NULL COMMENT 'SQL类型 (SELECT, INSERT等)',
  `pattern_type` varchar(20) NULL DEFAULT "GENERAL" COMMENT '模式类型',
  `simplified_sql` text NULL COMMENT '简化后的SQL',
  `examples` text NULL COMMENT 'SQL示例 (JSON格式)',
  `comments` text NULL COMMENT 'SQL注释 (JSON格式)',
  `frequency` int NULL COMMENT '出现频率',
  `tables` text NULL COMMENT '相关表 (JSON格式)',
  `update_time` datetime NULL COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(`pattern_id`)
COMMENT 'SQL查询模式'
DISTRIBUTED BY HASH(`pattern_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3"
);
    """
} 