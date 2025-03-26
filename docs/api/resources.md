# Doris MCP 资源API文档

本文档详细说明了Doris MCP服务器提供的所有资源API端点，包括请求方式、参数、返回值和使用示例。

## 资源请求方式

所有资源请求都通过MCP协议的`resource`方法发送，格式为：

```json
{
  "method": "resource",
  "params": {
    "uri": "资源URI"
  }
}
```

资源URI是资源的唯一标识符，格式为`协议://路径`，例如`doris://database/info`。

## 数据库相关资源

### 1. doris://database/info

获取数据库基本信息。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "doris://database/info"
  }
}
```

**响应**:
```json
{
  "database": "ssb",
  "connection_status": "connected",
  "server_version": "5.7.99",
  "server_time": "2023-07-01 12:34:56",
  "table_count": 8,
  "last_refresh_time": null
}
```

**参数说明**:
- 无参数

**返回字段**:
- `database`: 数据库名称
- `connection_status`: 连接状态，可选值：`connected`、`disconnected`
- `server_version`: 服务器版本
- `server_time`: 服务器当前时间
- `table_count`: 表数量
- `last_refresh_time`: 上次元数据刷新时间

### 2. doris://database/tables

获取数据库中的所有表列表。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "doris://database/tables"
  }
}
```

**响应**:
```json
{
  "database": "ssb",
  "tables": [
    {
      "name": "customer",
      "type": "TABLE",
      "rows": 3000000,
      "size": 65536000
    },
    {
      "name": "lineorder",
      "type": "TABLE", 
      "rows": 60000000,
      "size": 1073741824
    }
  ],
  "count": 2
}
```

**参数说明**:
- 无参数

**返回字段**:
- `database`: 数据库名称
- `tables`: 表列表，每个表包含以下字段：
  - `name`: 表名
  - `type`: 表类型，如TABLE、VIEW等
  - `rows`: 表中的行数（估计值）
  - `size`: 表大小（字节）
- `count`: 表总数

### 3. doris://database/metadata

获取数据库的详细元数据信息。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "doris://database/metadata"
  }
}
```

**响应**:
```json
{
  "database": "ssb",
  "tables": [
    {
      "name": "customer",
      "comment": "客户表",
      "columns": [
        {
          "name": "c_custkey",
          "type": "int",
          "comment": "客户主键",
          "nullable": false
        },
        {
          "name": "c_name",
          "type": "varchar(25)",
          "comment": "客户名称",
          "nullable": true
        }
      ]
    },
    {
      "name": "lineorder",
      "comment": "订单明细表",
      "columns": [
        {
          "name": "lo_orderkey",
          "type": "int",
          "comment": "订单主键",
          "nullable": false
        },
        {
          "name": "lo_custkey",
          "type": "int",
          "comment": "客户外键",
          "nullable": true
        }
      ]
    }
  ],
  "relationships": [
    {
      "table": "lineorder",
      "column": "lo_custkey",
      "references_table": "customer",
      "references_column": "c_custkey"
    }
  ],
  "business_overview": "这是一个星型模型数据仓库，包含销售订单相关表"
}
```

**参数说明**:
- 无参数

**返回字段**:
- `database`: 数据库名称
- `tables`: 表列表，每个表包含以下字段：
  - `name`: 表名
  - `comment`: 表注释
  - `columns`: 列列表，每列包含以下字段：
    - `name`: 列名
    - `type`: 数据类型
    - `comment`: 列注释
    - `nullable`: 是否可为空
- `relationships`: 表关系列表，每个关系包含以下字段：
  - `table`: 表名
  - `column`: 列名
  - `references_table`: 引用的表名
  - `references_column`: 引用的列名
- `business_overview`: 业务概览

### 4. doris://database/stats

获取数据库的统计信息。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "doris://database/stats"
  }
}
```

**响应**:
```json
{
  "database": "ssb",
  "total_size": 1073807360,
  "total_tables": 2,
  "total_rows": 63000000,
  "recent_tables": [
    {
      "name": "lineorder",
      "last_modified": "2023-06-30 15:45:23"
    },
    {
      "name": "customer",
      "last_modified": "2023-06-29 09:12:45"
    }
  ],
  "largest_tables": [
    {
      "name": "lineorder",
      "size": 1073741824,
      "rows": 60000000
    },
    {
      "name": "customer",
      "size": 65536000,
      "rows": 3000000
    }
  ]
}
```

**参数说明**:
- 无参数

**返回字段**:
- `database`: 数据库名称
- `total_size`: 总大小（字节）
- `total_tables`: 表总数
- `total_rows`: 总行数
- `recent_tables`: 最近修改的表列表
- `largest_tables`: 最大的表列表

## 表和架构相关资源

### 1. schema://{database}/{table}

获取指定表的架构信息。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "schema://ssb/customer"
  }
}
```

**响应**:
```
<h1>表 ssb.customer 的结构</h1>
<p><strong>表说明:</strong> 客户表</p>
<h2>列信息</h2>
<table border='1'>
<tr><th>列名</th><th>类型</th><th>可空</th><th>主键</th><th>默认值</th><th>说明</th></tr>
<tr><td>c_custkey</td><td>int</td><td>否</td><td>是</td><td></td><td>客户主键</td></tr>
<tr><td>c_name</td><td>varchar(25)</td><td>是</td><td>否</td><td></td><td>客户名称</td></tr>
</table>
<h2>索引信息</h2>
<table border='1'>
<tr><th>索引名</th><th>列</th><th>类型</th></tr>
<tr><td>PRIMARY</td><td>c_custkey</td><td>PRIMARY</td></tr>
</table>
```

**参数说明**:
- `{database}`: 数据库名称
- `{table}`: 表名

**返回内容**:
返回HTML格式的表结构信息，包括列信息和索引信息。

### 2. metadata://{database}/{table}

获取指定表的元数据和业务含义。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "metadata://ssb/customer"
  }
}
```

**响应**:
```
<h1>表 ssb.customer 的元数据</h1>
<p><strong>表说明:</strong> 客户表</p>
<h2>业务含义</h2>
<p>存储所有客户的基本信息，包括客户ID、名称、地址等。用于分析客户相关的销售数据。</p>
<h2>列信息和业务含义</h2>
<table border='1'>
<tr><th>列名</th><th>类型</th><th>业务含义</th></tr>
<tr><td>c_custkey</td><td>int</td><td>客户主键，唯一标识一个客户</td></tr>
<tr><td>c_name</td><td>varchar(25)</td><td>客户的全名</td></tr>
</table>
<h2>表关系</h2>
<ul>
<li>被 <strong>lineorder.lo_custkey</strong> 引用</li>
</ul>
```

**参数说明**:
- `{database}`: 数据库名称
- `{table}`: 表名

**返回内容**:
返回HTML格式的表元数据信息，包括业务含义、列信息和表关系。

## 文档资源

### docs://{topic}

获取指定主题的文档。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "docs://guide/getting-started"
  }
}
```

**响应**:
返回HTML格式的文档内容。

**参数说明**:
- `{topic}`: 文档主题，如`guide/getting-started`

**返回内容**:
返回HTML格式的文档内容。

## 系统资源

### 1. system://status

获取系统状态信息。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "system://status"
  }
}
```

**响应**:
返回HTML格式的系统状态信息，包括基本信息、资源使用情况和进程信息。

**参数说明**:
- 无参数

### 2. system://performance

获取系统性能指标。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "system://performance"
  }
}
```

**响应**:
返回HTML格式的系统性能信息，包括CPU、内存、磁盘和网络性能。

**参数说明**:
- 无参数

### 3. system://logs

获取系统日志。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "system://logs"
  }
}
```

**响应**:
返回HTML格式的系统日志，显示最近1000条日志记录。

**参数说明**:
- 无参数

### 4. system://audit

获取审计日志。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "system://audit"
  }
}
```

**响应**:
返回HTML格式的审计日志，包括时间、用户、操作、数据库、表、状态和耗时等信息。

**参数说明**:
- 无参数

## 预定义文档资源

### 1. docs://guide/getting-started

获取入门指南。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "docs://guide/getting-started"
  }
}
```

**响应**:
返回HTML格式的入门指南文档。

**参数说明**:
- 无参数

### 2. docs://guide/nl2sql

获取NL2SQL使用指南。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "docs://guide/nl2sql"
  }
}
```

**响应**:
返回HTML格式的NL2SQL使用指南文档。

**参数说明**:
- 无参数

### 3. docs://api/reference

获取API参考文档。

**请求**:
```json
{
  "method": "resource",
  "params": {
    "uri": "docs://api/reference"
  }
}
```

**响应**:
返回HTML格式的API参考文档。

**参数说明**:
- 无参数

## 使用示例

以下是使用Python客户端获取数据库信息的示例：

```python
from mcp_client import MCPClient

# 创建客户端
client = MCPClient("http://localhost:3000")

# 连接到服务器
client.connect()

try:
    # 获取数据库信息
    db_info = client.request("resource", {"uri": "doris://database/info"})
    print(f"数据库名称: {db_info['database']}")
    print(f"表数量: {db_info['table_count']}")
    print(f"服务器版本: {db_info['server_version']}")
    
    # 获取表列表
    tables = client.request("resource", {"uri": "doris://database/tables"})
    print(f"\n数据库中共有 {tables['count']} 个表:")
    for table in tables['tables']:
        print(f"- {table['name']} ({table['rows']} 行, {table['size']/1024/1024:.2f} MB)")
    
    # 获取表结构
    schema = client.request("resource", {"uri": f"schema://{db_info['database']}/customer"})
    print(f"\n表结构:\n{schema}")
    
finally:
    # 断开连接
    client.disconnect()
```

## 错误处理

资源请求可能会返回以下错误：

1. 资源不存在
```json
{
  "error": "Resource not found: invalid_resource"
}
```

2. 服务器错误
```json
{
  "error": "Server error: Failed to connect to database"
}
```

3. 参数错误
```json
{
  "error": "Invalid parameter: missing required parameter 'table'"
}
``` 