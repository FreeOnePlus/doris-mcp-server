# Doris MCP API 使用指南

## 简介

Doris MCP服务提供了一系列API，用于查询Doris数据库的元数据、执行自然语言转SQL查询等功能。

## 可用资源

### 数据库信息

- URI: `doris://database/info`
- 功能: 获取Doris数据库的基本信息，包括版本、连接状态等

### 数据库表列表

- URI: `doris://database/tables`
- 功能: 获取数据库中所有表的列表

### 数据库详细元数据

- URI: `doris://database/metadata`
- 功能: 获取数据库的详细元数据信息，包括表结构和关系

### 数据库统计信息

- URI: `doris://database/stats`
- 功能: 获取数据库的统计信息，如大小、表数量等

### 表结构信息

- URI: `schema://{database}/{table}`
- 功能: 获取指定表的结构信息
- 参数:
  - database: 数据库名称
  - table: 表名称

### 表元数据

- URI: `metadata://{database}/{table}`
- 功能: 获取指定表的元数据和业务含义
- 参数:
  - database: 数据库名称
  - table: 表名称

### 文档

- URI: `docs://{topic}`
- 功能: 获取指定主题的文档
- 参数:
  - topic: 文档主题，例如 "usage/api" 表示API使用文档

## 使用提示模板

### 自然语言转SQL查询

- ID: `nl2sql`
- 功能: 将自然语言问题转换为SQL查询语句
- 参数:
  - question: 自然语言问题

### 数据分析方案

- ID: `data_analysis`
- 功能: 设计数据分析方案和SQL查询
- 参数:
  - analysis_task: 分析任务描述

### 表结构与数据探索

- ID: `table_exploration`
- 功能: 探索数据表结构和内容
- 参数:
  - table_name: 要探索的表名
  - database: 数据库名称（可选） 