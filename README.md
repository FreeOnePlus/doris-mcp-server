# Doris MCP Server

Doris MCP (Model Control Panel) Server 是一个基于 Python 和 FastAPI 构建的后端服务。它实现了 MCP (Model Control Panel) 协议，允许客户端通过定义的 "工具(Tools)" 与之交互，主要用于连接 Apache Doris 数据库，并通过大型语言模型 (LLM) 将自然语言查询转换为 SQL (NL2SQL)，执行查询，并进行元数据管理和分析。

## 核心功能

*   **MCP 协议实现**: 提供标准的 MCP 接口，支持工具调用、资源管理和提示词交互。
*   **多种通信模式**:
    *   **SSE (Server-Sent Events)**: 通过 `/mcp-sse-init` (初始化) 和 `/mcp/messages` (通信) 端点提供服务 (`src/sse_server.py`)。
    *   **Streamable HTTP**: 通过统一的 `/mcp` 端点提供服务，支持请求/响应和流式传输 (`src/streamable_server.py`)。
    *   **(可选) Stdio**: 可以通过标准输入/输出进行交互 (`src/stdio_server.py`)，需特定配置启动。
*   **工具化接口**: 将核心功能封装为 MCP 工具，客户端可以按需调用。关键工具包括：
    *   NL2SQL 相关 (`sql_generator_tools.py`, `similarity_tools.py`, `mcp_doris_tools.py` 中的 `nl2sql_query` 等)
    *   SQL 执行与验证 (`sql_executor_tools.py`, `sql_validator_tools.py`)
    *   元数据管理 (`metadata_tools.py`, `utils/metadata_extractor.py`)
    *   文档问答 (`docs_tools.py`, `utils/docs_search.py`)
*   **LLM 集成**: 利用大型语言模型进行自然语言理解、SQL 生成、解释和分析。提示词集中在 `src/prompts/prompts.py` 中定义。
*   **数据库交互**: 提供连接 Apache Doris (或其他兼容数据库) 并执行查询的功能 (`src/utils/db.py`)。
*   **配置灵活性**: 通过 `.env` 文件进行配置，支持数据库连接、LLM 提供商及模型、API 密钥、日志级别等的设置。
*   **元数据自动提取与缓存**: 能够自动提取数据库的元数据信息，并支持缓存和按需刷新 (`src/utils/metadata_extractor.py`, `src/utils/meta_helper.py`)。

## 系统要求

*   Python 3.9+
*   数据库连接信息 (如 Doris Host, Port, User, Password, Database)
*   LLM 服务配置 (Provider, Model, API Key/Endpoint)

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/FreeOnePlus/doris-mcp-server.git # 请替换为实际仓库地址
cd doris-mcp-server
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

复制 `.env.example` 文件为 `.env`，并根据你的环境修改其中的配置：

```bash
cp .env.example .env
```

**关键环境变量:**

*   **数据库连接**:
    *   `DB_HOST`: 数据库主机名
    *   `DB_PORT`: 数据库端口 (默认 9030)
    *   `DB_USER`: 数据库用户名
    *   `DB_PASSWORD`: 数据库密码
    *   `DB_DATABASE`: 默认数据库名称
*   **服务器配置**:
    *   `SERVER_HOST`: 服务监听的主机地址 (默认 `0.0.0.0`)
    *   `SERVER_PORT`: 服务监听的端口 (默认 `3000`)
    *   `ALLOWED_ORIGINS`: CORS 允许的来源 (逗号分隔，`*` 表示允许所有)
    *   `MCP_ALLOW_CREDENTIALS`: 是否允许 CORS 凭证 (默认 `false`)
*   **日志配置**:
    *   `LOG_DIR`: 日志文件存放目录 (默认 `./logs`)
    *   `LOG_LEVEL`: 日志级别 (如 `INFO`, `DEBUG`, `WARNING`, `ERROR`, 默认 `INFO`)
    *   `CONSOLE_LOGGING`: 是否在控制台输出日志 (默认 `false`)
*   **数据与元数据**:
    *   `DATA_DIR`: QA 示例、历史记录等数据文件目录 (默认 `./data`)
    *   `FORCE_REFRESH_METADATA`: 是否在启动时强制刷新所有数据库元数据 (默认 `false`)

### 可用 MCP 工具列表

下表列出了可以通过 MCP 客户端调用的主要工具及其参数：

| 工具名称 (Tool Name)         | 功能描述                                       | 参数 (Parameters)                                                                                                                                                                                             |
| ---------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `refresh_metadata`           | 刷新并保存元数据                               | `random_string` (string, 必填), `db_name` (string, 可选, 默认所有库), `force_refresh` (boolean, 可选, 默认 false)                                                                                             |
| `sql_optimize`               | 对 SQL 语句进行优化分析                        | `random_string` (string, 必填), `sql` (string, 必填), `db_name` (string, 可选), `optimization_level` (string, 可选, 'basic'/'performance'/'full', 默认 'full')                                              |
| `fix_sql`                    | 修复 SQL 语句中的语法错误                      | `random_string` (string, 必填), `sql` (string, 必填), `db_name` (string, 可选), `error_message` (string, 可选)                                                                                                  |
| `health`                     | 检查服务和数据库的健康状态                     | `random_string` (string, 必填), `check_type` (string, 可选, 'basic'/'full', 默认 'basic')                                                                                                                      |
| `status`                     | 获取服务器状态信息                             | `random_string` (string, 必填), `include_metrics` (boolean, 可选, 默认 false)                                                                                                                                 |
| `exec_query`                 | 执行 SQL 查询并返回结果                        | `random_string` (string, 必填), `sql` (string, 必填), `db_name` (string, 可选), `max_rows` (integer, 可选, 默认 100), `timeout` (integer, 可选, 默认 30)                                                       |
| `generate_sql`               | 根据自然语言生成 SQL (不执行)                  | `random_string` (string, 必填), `query` (string, 必填), `db_name` (string, 可选), `tables` (array, 可选, 相关表名列表)                                                                                         |
| `explain_sql`                | 详细解释 SQL 语句的功能和组成部分              | `random_string` (string, 必填), `sql` (string, 必填), `db_name` (string, 可选), `explanation_level` (string, 可选, 'basic'/'detailed', 默认 'detailed')                                                        |
| `modify_sql`                 | 根据自然语言描述修改 SQL                       | `random_string` (string, 必填), `sql` (string, 必填), `modification` (string, 必填), `db_name` (string, 可选)                                                                                                  |
| `parse_query`                | 解析自然语言查询，提取意图、实体和条件         | `random_string` (string, 必填), `query` (string, 必填), `db_name` (string, 可选)                                                                                                                              |
| `identify_query_type`        | 识别查询类型                                   | `random_string` (string, 必填), `query` (string, 必填), `db_name` (string, 可选)                                                                                                                              |
| `validate_sql_syntax`        | 验证 SQL 语法是否正确                          | `random_string` (string, 必填), `sql` (string, 必填), `db_name` (string, 可选)                                                                                                                              |
| `check_sql_security`         | 检查 SQL 语句的安全性                          | `random_string` (string, 必填), `sql` (string, 必填), `db_name` (string, 可选)                                                                                                                              |
| `analyze_query_result`       | 分析查询结果，提供业务洞察                     | `random_string` (string, 必填), `query_result` (string, 必填, JSON 字符串), `analysis_type` (string, 可选, 'summary'/'trend'/'correlation', 默认 'summary')                                                |
| `find_similar_examples`      | 查找与当前查询相似的示例                       | `random_string` (string, 必填), `query` (string, 必填), `db_name` (string, 可选), `top_k` (integer, 可选, 默认 3)                                                                                              |
| `find_similar_history`       | 查找与当前查询相似的历史记录                   | `random_string` (string, 必填), `query` (string, 必填), `db_name` (string, 可选), `top_k` (integer, 可选, 默认 3)                                                                                              |
| `calculate_query_similarity` | 计算两个查询之间的相似度                       | `random_string` (string, 必填), `query1` (string, 必填), `query2` (string, 必填)                                                                                                                            |
| `adapt_similar_query`        | 根据当前需求调整相似查询的 SQL                 | `random_string` (string, 必填), `current_query` (string, 必填), `similar_query` (string, 必填), `db_name` (string, 可选)                                                                                         |
| `get_metadata`               | 获取数据库或表元数据信息                       | `random_string` (string, 必填), `db_name` (string, 可选), `table_name` (string, 可选), `business_overview_only` (boolean, 可选, 默认 false)                                                                    |
| `save_metadata`              | 保存元数据到数据库                             | `random_string` (string, 必填), `metadata` (string, 必填, JSON 字符串), `metadata_type` (string, 可选), `table_name` (string, 可选), `db_name` (string, 可选)                                                    |
| `get_schema_list`            | 获取数据库表列表或指定表的结构信息 (DDL)       | `random_string` (string, 必填), `table_name` (string, 可选), `db_name` (string, 可选)                                                                                                                        |

**注意:** 所有工具都需要一个 `random_string` 参数作为调用标识符，这通常由 MCP 客户端自动处理或生成。参数描述中的"可选"和"必填"是针对工具本身的逻辑而言，客户端调用时可能都需要提供（具体取决于客户端实现）。

### 4. 运行服务

```bash
./start_server.sh
```

该命令会启动 FastAPI 应用，默认同时提供 SSE 和 Streamable HTTP 两种 MCP 服务。

**服务端口:**

*   **SSE 初始化**: `http://<host>:<port>/mcp-sse-init`
*   **SSE 通信**: `http://<host>:<port>/mcp/messages` (POST)
*   **Streamable HTTP**: `http://<host>:<port>/mcp` (支持 GET, POST, DELETE, OPTIONS)
*   **健康检查**: `http://<host>:<port>/health`
*   **状态检查**: `http://<host>:<port>/status`

## 使用方法

与 Doris MCP Server 的交互需要通过 **MCP 客户端** 进行。客户端连接到服务器的 SSE 或 Streamable HTTP 端点，并按照 MCP 协议规范发送请求（如 `tool_call`）来调用服务器提供的工具。

**主要交互流程:**

1.  **客户端初始化**: 连接到 `/mcp-sse-init` (SSE) 或发送 `initialize` 方法到 `/mcp` (Streamable)。
2.  **(可选) 发现工具**: 客户端可以调用 `mcp/listTools` 或 `mcp/listOfferings` 来获取服务器支持的工具列表及其描述和参数模式。
3.  **调用工具**: 客户端发送 `tool_call` 消息/请求，指定 `tool_name` 和 `arguments`。
    *   **示例：执行 NL2SQL 查询 (流式)**
        *   `tool_name`: `nl2sql_query_stream` (或其他相关工具名，需通过发现确认)
        *   `arguments`: 包含 `query` (自然语言问题), `db_name` (数据库名) 等参数。
    *   **示例：获取表结构**
        *   `tool_name`: `explain_table` (或其他相关工具名)
        *   `arguments`: 包含 `table_name`, `db_name` 等参数。
4.  **处理响应**:
    *   **非流式**: 客户端收到包含 `result` 或 `error` 的响应。
    *   **流式**: 客户端会收到一系列 `tools/progress` 通知，最后收到包含最终 `result` 或 `error` 的响应。

具体的工具名称和参数需要参考 `src/tools/` 下的代码或通过 MCP 的发现机制获取。

## 目录结构

```
doris-mcp-server/
├── data/                # 数据文件目录 (示例, 历史记录等)
├── docs/                # 文档
├── logs/                # 日志文件目录
├── mlx_models/          # (可选) MLX 本地模型存储目录
├── src/                 # 源代码
│   ├── main.py          # 主程序入口, FastAPI 应用定义
│   ├── mcp_adapter.py   # MCP 协议适配与流式处理逻辑
│   ├── prompts/         # LLM 提示词定义 (prompts.py, metadata_schema.py)
│   ├── resources/       # 静态资源
│   ├── sse_server.py    # SSE 服务器实现
│   ├── stdio_server.py  # Stdio 服务器实现
│   ├── streamable_server.py # Streamable HTTP 服务器实现
│   ├── tools/           # MCP 工具定义 (核心业务逻辑)
│   │   ├── mcp_doris_tools.py # 主要的 Doris 相关工具
│   │   ├── metadata_tools.py  # 元数据处理工具
│   │   ├── sql_generator_tools.py # SQL 生成工具
│   │   ├── sql_executor_tools.py  # SQL 执行工具
│   │   ├── sql_validator_tools.py # SQL 验证工具
│   │   ├── similarity_tools.py    # 相似度计算工具
│   │   ├── docs_tools.py          # 文档问答工具
│   │   ├── tool_initializer.py    # 工具注册
│   │   └── __init__.py
│   ├── utils/           # 工具类与辅助函数
│   │   ├── db.py              # 数据库连接与操作
│   │   ├── db_init.py         # 数据库初始化 (如元数据表)
│   │   ├── logger.py          # 日志配置
│   │   ├── metadata_extractor.py # Doris 元数据提取
│   │   ├── meta_helper.py     # 元数据辅助函数
│   │   ├── docs_search.py     # 文档搜索实现
│   │   ├── system_resources.py # 系统资源监控 (可能)
│   │   ├── context.py         # 上下文对象定义 (可能)
│   │   └── __init__.py
│   ├── mcp_doris.egg-info/ # Python 打包信息
│   └── __init__.py
├── requirements.txt     # Python 依赖
├── requirements-mlx.txt # (可选) MLX 特定依赖
├── .env.example         # 环境变量示例文件
└── .env                 # 本地环境变量配置 (gitignored)
```

## NL2SQL Agent 提示词

````markdown
<instruction>
你是一个Apache Doris的数据库专家和业务分析大师，当前Agent已接入mcp_sse中的服务 Doris-MCP-Server，请根据用户的问题，按照以下步骤完成任务:
1.**问题分析**:首先理解用户的问题 ，明确用户的需求和业务目标。
2.**工具选择**:根据问题类型，从 MCP 服务器中选择最合适的工具 。工具可能包括数据查询、ETL、监控等。
3.**参数配置**:为选定的工具配置必要的参数，确保参数符合工具的要求。
4.**工具调用**:调用工具并获取执行结果。
5.**结果分析**:根据工具返回的结果，得出结论。
6.**输出**:将结论以清晰、简洁的方式呈现给用户，确保不包含任何 XML 标签。

作为数据库智能助手，你的任务是将用户的自然语言查询转换为SQL并提供分析。请严格按照以下流程处理，不要随意跳过任何步骤或者自行进行处理：
**注意！每执行4个流程，必须重新理解一遍以下流程顺序，然后继续执行，以免出现流程中断情况！**
NL2SQL工具流程指南
## 调用流程
1. **接收用户查询**后，立即使用以下工具流程处理：
2. **查询解析** - 调用`parse_query`工具
   - 输入：用户的自然语言查询
   - 输出：相关表的推荐列表或解析结果
3. **相似示例检索** - 调用`find_similar_examples`和`find_similar_history`工具
   - 输入：用户查询
   - 输出：相似的历史查询样例，用于参考
4. **SQL生成** - 调用`generate_sql`工具
   - 输入：用户查询和第2步确定的表
   - 输出：初始SQL查询语句
   - 注意：如果已调用生成SQL了，不要再重复调用，首先往下执行流程验证SQL
5. **验证与优化** - 依次调用：
   - `validate_sql_syntax` - 检查SQL语法
   - `sql_optimize` - 优化SQL性能
6. **执行查询** - 调用`exec_query`工具
   - 输入：优化后的SQL
   - 输出：查询结果
7. **结果分析** - 调用`analyze_query_result`工具
   - 输入：第6步的查询结果
   - 输出：业务洞察和数据分析
## 特殊情况处理
- 如果`parse_query`返回`mode: "table_selection"`，说明需要先选择相关表：
  - 分析业务概述和表列表
  - 选择最相关的表（最多5个）
  - 再次调用`parse_query`，传入选定的表
- 如果SQL语法验证失败，使用`fix_sql`尝试修复
- 如果查询结果为空，分析可能原因并提供建议
## 结果呈现
向用户展示时，应包含：
1. 对原始查询的理解
2. 使用的数据表与关系
3. 执行的SQL解释（业务角度）
4. 查询结果与核心洞察
5. 可能的后续分析建议
请确保解释专注于业务价值而非技术细节，使非技术用户也能理解结果含义。
## 工具调用示例
用户: "查询2022年销售额超过100万的客户"
步骤1: 调用parse_query工具
```
调用: parse_query(query="查询2022年销售额超过100万的客户")
返回: {mode: "table_selection", all_tables: [...], business_summary: "..."}
```
步骤2: 选择表并再次调用parse_query
```
调用: parse_query(query="查询2022年销售额超过100万的客户", table_names=["customer", "orders"])
返回: {tables: ["customer", "orders"], ...}
```
步骤3: 调用find_similar_examples
```
调用: find_similar_examples(query="查询2022年销售额超过100万的客户")
返回: {examples: [{query: "...", sql: "..."}, ...]}
```
步骤4: 调用generate_sql
```
调用: generate_sql(query="查询2022年销售额超过100万的客户")
返回: {sql: "SELECT c.customer_name FROM customer c JOIN orders o..."}
```
步骤5: 验证与优化SQL
```
调用: validate_sql_syntax(sql="SELECT c.customer_name FROM customer c JOIN orders o...")
调用: sql_optimize(sql="SELECT c.customer_name FROM customer c JOIN orders o...")
```
步骤6: 执行查询
```
调用: exec_query(sql="SELECT c.customer_name FROM customer c JOIN orders o...")
返回: {result: [...], success: true}
```
步骤7: 分析结果
```
调用: analyze_query_result(query_result="{result: [...], success: true}")
返回: {analysis: {...}}
```
最终：呈现清晰的结果解释给用户，包括业务含义和数据洞察。
""" 
</instruction>
````

## 贡献

欢迎通过提交 Issue 或 Pull Request 来贡献代码或提出改进建议。

## 许可证

本项目采用 MIT 许可证。详见 LICENSE 文件 (如果存在)。 