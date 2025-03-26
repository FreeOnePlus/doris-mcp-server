# Doris NL2SQL 入门指南

## 简介

Doris NL2SQL是一个强大的自然语言转SQL查询工具，帮助用户使用日常语言查询Apache Doris数据库，无需编写复杂的SQL语句。本指南将帮助您快速上手Doris NL2SQL服务。

## 快速开始

### 连接服务

1. 确保服务器已经启动：
   ```bash
   python src/main.py
   ```

2. 使用MCP客户端连接到服务：
   ```javascript
   import { MCPClient } from 'mcp-client';
   
   const client = new MCPClient('http://localhost:3000');
   await client.connect();
   ```

### 基本使用

使用自然语言查询：

```javascript
// 使用nl2sql工具将自然语言转换为SQL
const result = await client.tools.nl2sql_query("统计最近7天的订单总数");

console.log("生成的SQL:", result.sql);
console.log("查询结果:", result.data);
```

获取数据库信息：

```javascript
// 获取数据库基本信息
const dbInfo = await client.resources.read("doris://database/info");
console.log("数据库信息:", dbInfo);

// 获取数据库表列表
const tables = await client.resources.read("doris://database/tables");
console.log("表列表:", tables);
```

## 核心功能

### 自然语言转SQL

Doris NL2SQL能够理解多种形式的自然语言查询：

- 简单查询：「查询最近10条订单信息」
- 条件查询：「查询上海地区的销售额」
- 分组查询：「按产品类别统计销售额」
- 排序查询：「查询销售额最高的前5个客户」
- 聚合查询：「计算各地区的平均订单金额」

### 元数据检索

系统提供丰富的元数据查询能力：

- 获取表结构
- 获取表之间的关系
- 获取业务含义
- 获取数据统计信息

## 高级功能

### 数据分析

提供多种数据分析能力：

- 趋势分析
- 对比分析
- 异常检测
- 关联分析

### 可视化建议

为数据提供合适的可视化方案：

- 图表类型建议
- 维度和指标选择
- 可视化最佳实践

## 下一步

- 查看[API参考文档](../api/reference)
- 了解[NL2SQL使用技巧](./nl2sql)
- 探索[数据分析示例](../examples/analysis) 