# Apache Doris NL2SQL MCP 服务器使用说明

本文档介绍如何使用 Apache Doris NL2SQL MCP 服务器，该服务器允许您使用自然语言查询 Apache Doris 数据库。

## 前提条件

在使用本服务器之前，您需要：

1. 安装 Python 3.8 或更高版本
2. 安装所有依赖项：`pip install -r requirements.txt`
3. 配置数据库连接信息（创建 `.env` 文件）
4. 确保您有权限访问 Apache Doris 数据库

## 配置数据库连接

1. 复制示例环境变量文件：`cp .env.example .env`
2. 编辑 `.env` 文件，填入您的数据库连接信息：

```
DB_HOST=your_doris_host
DB_PORT=9030
DB_USER=your_username
DB_PASSWORD=your_password
DB_DATABASE=your_database
```

## 使用方法

### 开发模式

在开发模式下运行可以快速测试和调试服务器：

```bash
mcp dev src/main.py
```

这将启动 MCP Inspector，允许您与服务器交互并测试功能。

### 安装到 Claude Desktop

要在 Claude Desktop 中使用，请安装服务器：

```bash
mcp install src/main.py --name "Doris NL2SQL"
```

或者，使用环境变量文件：

```bash
mcp install src/main.py --name "Doris NL2SQL" -f .env
```

### 直接运行

您也可以直接运行服务器：

```bash
python src/main.py
```

## 示例查询

以下是一些示例自然语言查询：

1. "查询销售额前10的产品及其销售数量"
2. "计算每个区域今年与去年同期的销售额对比"
3. "分析过去30天内每个用户的活跃度"
4. "统计各部门员工数量和平均薪资"

## 使用提示

使用本服务器时的一些提示：

1. **探索表结构**：在查询前，您可以使用 `table_exploration` 提示了解表结构
2. **分析任务**：对于复杂分析任务，使用 `data_analysis` 提示获得更系统的分析方案
3. **优化查询**：如果生成的SQL不够高效，您可以要求服务器优化查询性能
4. **引用文档**：如果您需要了解特定功能，使用 `search_doris_docs` 工具查找相关文档

## 故障排除

如果遇到问题，请检查：

1. 数据库连接信息是否正确
2. 您的用户是否有足够的权限
3. 网络连接是否畅通
4. 环境变量是否正确加载

如果问题仍然存在，请查看日志或联系管理员。