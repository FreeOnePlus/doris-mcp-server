# MCP 客户端连接指南

本文档说明如何正确连接MCP客户端到Doris NL2SQL MCP服务器。

## 关于MCP通信协议

Model Context Protocol (MCP) 使用的是基于Server-Sent Events (SSE) 的通信协议，而不是标准的HTTP REST API。这意味着直接使用HTTP请求（如 `curl` 或普通的HTTP客户端）将无法正常工作。

## 连接流程

正确的连接流程应该是：

1. 客户端首先连接到SSE端点（通常是服务器的 `/sse` 路径）
2. 服务器回复一个会话ID和消息端点URL
3. 客户端使用POST请求发送到该消息端点，同时包含会话ID
4. 服务器通过已建立的SSE连接向客户端发送响应

## 使用官方MCP客户端

推荐使用官方的MCP客户端库来连接此服务器：

1. 确保服务器正常运行（默认地址为 `http://localhost:3000`）
2. 使用官方MCP客户端应用，如MCP-Inspector或Claude等支持MCP的客户端
3. 在客户端中设置服务器URL为 `http://localhost:3000`

## 可用的资源 URI

以下是服务器提供的资源URI列表:

### 数据库相关资源

- `doris://database/info` - 提供数据库基本信息，包括数据库名称、连接状态、服务器版本等
- `doris://database/tables` - 提供数据库中所有表的列表
- `doris://database/metadata` - 提供数据库的详细元数据信息，包括所有表的结构和关系
- `doris://database/stats` - 提供数据库的统计信息，如大小、表数量、最近修改的表等

### 表和架构相关资源

- `schema://{database}/{table}` - 提供指定表的架构信息
- `metadata://{database}/{table}` - 提供指定表的元数据和业务含义

### 文档资源

- `docs://{topic}` - 提供指定主题的文档

## 常见问题

### 超时错误

如果你收到类似这样的错误：

```
{
  "error": "Request timed out"
}
```

这通常表示：

1. 服务器可能未正常运行
2. 客户端未使用正确的MCP协议进行连接
3. 网络连接问题

### 解决方法

1. 确保Doris NL2SQL MCP服务器正在运行
2. 使用支持MCP协议的客户端
3. 检查网络连接和防火墙设置
4. 检查服务器日志了解更多详细信息

## 技术细节

MCP使用JSON-RPC 2.0作为消息格式，通过SSE传输。这种方式允许服务器向客户端流式传输数据，同时客户端可以通过HTTP POST请求发送命令到服务器。 