# Doris NL2SQL API 参考文档

## API概述

Doris NL2SQL服务提供了基于MCP协议的API接口，客户端可以通过这些接口执行自然语言查询、获取元数据信息、使用提示模板等。

## 连接方式

MCP客户端通过SSE(Server-Sent Events)与服务器建立连接：

```javascript
import { MCPClient } from 'mcp-client';

// 创建客户端实例
const client = new MCPClient('http://localhost:3000');

// 连接到服务器
await client.connect();

// 使用资源和工具
const dbInfo = await client.resources.read('doris://database/info');
```

## 资源API

### 数据库资源

#### `doris://database/info`

获取数据库基本信息。

**请求：**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "doris://database/info"
  }
}
```

**响应：**
```json
{
  "database_name": "example_db",
  "connection_status": "connected",
  "server_info": {
    "version": "2.0.0",
    "server_time": "2023-07-01 12:34:56"
  },
  "table_count": 42,
  "last_refresh_time": "2023-07-01 10:00:00"
}
```

#### `doris://database/tables`

获取数据库中的表列表。

**请求：**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "doris://database/tables"
  }
}
```

**响应：**
```json
{
  "database_name": "example_db",
  "tables": [
    {
      "name": "orders",
      "comment": "订单表",
      "column_count": 10
    },
    {
      "name": "customers",
      "comment": "客户信息表",
      "column_count": 8
    }
  ],
  "count": 2
}
```

#### `doris://database/metadata`

获取数据库详细元数据。

**请求：**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "doris://database/metadata"
  }
}
```

**响应：** 返回详细的数据库元数据信息，包括表结构、关系等。

#### `doris://database/stats`

获取数据库统计信息。

**请求：**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "doris://database/stats"
  }
}
```

**响应：**
```json
{
  "database_name": "example_db",
  "table_count": 42,
  "database_size": 1073741824,
  "database_version": "2.0.0",
  "server_time": "2023-07-01 12:34:56",
  "recent_tables": [
    {
      "table_name": "orders",
      "update_time": "2023-07-01 11:22:33"
    }
  ]
}
```

### 表资源

#### `schema://{database}/{table}`

获取指定表的结构信息。

**请求：**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "schema://example_db/orders"
  }
}
```

**响应：** 返回表结构的HTML表示。

#### `metadata://{database}/{table}`

获取指定表的元数据和业务含义。

**请求：**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "metadata://example_db/orders"
  }
}
```

**响应：** 返回表元数据的HTML表示。

### 文档资源

#### `docs://{topic}`

获取指定主题的文档。

**请求：**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "docs://guide/getting-started"
  }
}
```

**响应：** 返回文档的HTML内容。

## 工具API

### `nl2sql_query`

将自然语言转换为SQL并执行查询。

**请求：**
```json
{
  "method": "tools/invoke",
  "params": {
    "name": "nl2sql_query",
    "params": {
      "query": "查询最近10条订单记录"
    }
  }
}
```

**响应：**
```json
{
  "success": true,
  "sql": "SELECT * FROM orders ORDER BY order_time DESC LIMIT 10",
  "data": [...],
  "columns": ["id", "customer_id", "order_time", "total_amount"],
  "row_count": 10
}
```

### `list_database_tables`

列出数据库中的所有表。

**请求：**
```json
{
  "method": "tools/invoke",
  "params": {
    "name": "list_database_tables",
    "params": {}
  }
}
```

**响应：** 返回数据库中的表列表。

### `explain_table`

获取表结构的详细信息。

**请求：**
```json
{
  "method": "tools/invoke",
  "params": {
    "name": "explain_table",
    "params": {
      "table_name": "orders"
    }
  }
}
```

**响应：** 返回表结构的详细信息。

## 提示API

### 提示列表

获取可用的提示模板列表。

**请求：**
```json
{
  "method": "prompts/list",
  "params": {}
}
```

**响应：** 返回提示模板列表。

### 使用提示

使用指定的提示模板生成内容。

**请求：**
```json
{
  "method": "prompts/generate",
  "params": {
    "prompt_id": "nl2sql",
    "parameters": {
      "question": "查询最近10条订单记录"
    }
  }
}
```

**响应：** 返回生成的内容。

## 其他API

### Ping

检查服务器连接状态。

**请求：**
```json
{
  "method": "ping"
}
```

**响应：**
```json
{
  "result": {
    "server": "Doris NL2SQL MCP Server",
    "version": "1.0.0",
    "status": "active",
    "timestamp": "2023-07-01 12:34:56"
  }
}
```

### 资源模板列表

获取可用的资源模板列表。

**请求：**
```json
{
  "method": "resources/templates/list",
  "params": {}
}
```

**响应：** 返回资源模板列表。

### 根目录列表

获取可访问的根目录列表。

**请求：**
```json
{
  "method": "roots/list"
}
```

**响应：**
```json
{
  "result": {
    "roots": [
      {
        "name": "项目根目录",
        "path": "/path/to/doris-mcp-server",
        "description": "Doris NL2SQL MCP服务项目根目录"
      },
      {
        "name": "数据目录",
        "path": "/path/to/doris-mcp-server/data",
        "description": "存储数据文件的目录"
      },
      {
        "name": "文档目录",
        "path": "/path/to/doris-mcp-server/docs",
        "description": "项目文档目录"
      }
    ]
  }
}
```

## 错误处理

API可能返回以下错误响应：

```json
{
  "error": {
    "message": "错误信息描述"
  }
}
```

常见错误码及说明：

- 400: 参数错误
- 404: 资源不存在
- 500: 服务器内部错误 