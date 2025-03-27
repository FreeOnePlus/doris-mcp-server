# Doris MCP Server

Doris MCP (Model Control Panel) Server是一个基于Python的服务框架，用于通过自然语言处理将自然语言查询转换为SQL查询，支持多种LLM服务商。

## 功能特点

- 支持多种LLM服务商：OpenAI, DeepSeek, Sijiliu, Volcengine, Qwen, Ollama以及Apple MLX
- 自然语言到SQL (NL2SQL) 转换功能
- SQL自动优化（未完成）
- 集群巡检运维（未完成）
- 数据指定格式导出（CSV、JSON）（未完成）
- 元数据提取与管理
- 数据库连接与查询执行
- 灵活的配置系统

## 系统要求

- Python 3.9+
- 外部LLM API密钥（如OpenAI, DeepSeek等）或本地模型支持（如Ollama, MLX）
- 数据库连接信息

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/yourusername/doris-mcp-server.git
cd doris-mcp-server
```

### 2. 安装依赖

```bash
pip install -r requirements.txt

# 如果使用Apple MLX（仅适用于Apple Silicon Mac）
pip install -r requirements-mlx.txt
```

### 3. 配置环境变量

复制示例配置文件并进行自定义：

```bash
cp .env.example .env
```

编辑`.env`文件，设置以下关键配置：
- LLM服务商选择
- API密钥和端点
- 数据库连接信息

### 4. 运行服务

```bash
python src/main.py
```

## 使用指南

### NL2SQL服务

NL2SQL服务可以将自然语言转换为SQL查询。示例用法：

```python
from src.nl2sql_service import NL2SQLService

# 初始化服务
nl2sql_service = NL2SQLService()

# 执行自然语言查询
result = nl2sql_service.execute_nl_query(
    "查询最近一周的订单总数",
    "订单表"
)

print(result)
```

### 配置不同的LLM提供商

在`.env`文件中，您可以通过设置`LLM_PROVIDER`环境变量来选择不同的LLM提供商：

```
# 可选值: openai, deepseek, sijiliu, qwen, ollama, mlx
LLM_PROVIDER=openai
```

每个提供商需要额外的特定配置，详见各提供商文档。

### 多LLM阶段配置

系统支持在不同处理阶段使用不同的LLM提供商和模型，这可以优化性能和成本：

#### 支持的处理阶段
- **business_check**: 业务查询判断阶段 (适合使用轻量级模型)
- **similar_example**: 相似示例查找阶段 (适合使用轻量级模型)
- **sql_generation**: SQL生成阶段 (适合使用强大的模型)
- **metadata**: 元数据处理阶段 (适合使用轻量级模型)

#### 配置方法

在`.env`文件中添加阶段特定的配置：

```
# 默认LLM提供商
LLM_PROVIDER=openai
OPENAI_MODEL=gpt-3.5-turbo

# 业务查询检查阶段 - 使用本地轻量级模型
LLM_PROVIDER_BUSINESS_CHECK=ollama
LLM_MODEL_BUSINESS_CHECK=qwen:0.5b

# SQL生成阶段 - 使用更强大的模型
LLM_PROVIDER_SQL_GENERATION=openai
LLM_MODEL_SQL_GENERATION=gpt-4o
```

#### 工作原理

1. 系统首先查找阶段特定的配置 (如`LLM_PROVIDER_BUSINESS_CHECK`)
2. 如果未找到，则使用默认配置 (`LLM_PROVIDER`)
3. 每个阶段都会使用相应提供商的其他配置 (如API密钥、温度等)

#### 优势

- **优化成本**: 为简单任务使用轻量级模型，为复杂任务使用强大模型
- **灵活性**: 可以混合使用云服务和本地模型
- **性能优化**: 为每个阶段选择最适合的模型

## MLX支持

本项目支持在Apple Silicon Mac上使用MLX本地运行大型语言模型。有关详细信息，请参阅[MLX使用指南](docs/MLX使用指南.md)。

## 目录结构

```
doris-mcp-server/
├── docs/                # 文档
├── mlx_models/          # MLX模型存储目录
├── src/                 # 源代码
│   ├── main.py          # 主程序入口
│   ├── nl2sql_service.py # NL2SQL服务
│   ├── utils/           # 工具类
│       ├── db.py        # 数据库操作
│       ├── llm_client.py # LLM客户端
│       ├── llm_provider.py # LLM提供商枚举
│       ├── metadata_extractor.py # 元数据提取器
│       └── nl2sql_processor.py # NL2SQL处理器
├── tests/               # 测试代码
│   ├── nl2sql/          # NL2SQL测试
│   └── mlx/             # MLX测试
├── requirements.txt     # 基本依赖
├── requirements-mlx.txt # MLX特定依赖
└── .env                 # 环境变量配置
```

## 测试

运行测试：

```bash
# 基本功能测试
python -m unittest discover tests

# MLX客户端测试
python tests/test_mlx_client.py
```

## 贡献

欢迎提交问题和拉取请求！

## 许可证

本项目采用MIT许可证 - 详见LICENSE文件 