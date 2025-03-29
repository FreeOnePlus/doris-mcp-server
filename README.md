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

# DeepSeek LLM API问题修复

## 问题背景

在使用DeepSeek LLM API的过程中，系统频繁返回空响应，导致NL2SQL处理失败，这影响了整个系统的正常运行。

## 问题根本原因

1. **API端点不正确**：DeepSeek API的正确端点是`/chat/completions`，而系统中使用的是`/api/v1/chat/completions`。

2. **误用OpenAI客户端处理DeepSeek响应**：尽管DeepSeek API兼容OpenAI的接口格式，但在实际实现中存在细微差别，导致使用OpenAI客户端调用DeepSeek API时无法正确获取响应。

## 解决方案

1. **更正API端点**：将`/api/v1/chat/completions`修改为`/chat/completions`。

2. **实现专用DeepSeek处理**：
   - 在`_chat_openai_compatible`方法中，为DeepSeek实现专门的处理逻辑
   - 使用`requests`库直接调用API，而不是通过OpenAI客户端
   - 解析返回的JSON响应，提取内容

3. **增强错误处理**：
   - 对请求失败的情况进行专门处理
   - 返回空内容而不是引发异常，提高系统健壮性
   - 增加详细日志记录，方便调试和监控

4. **统一返回格式**：确保即使发生错误，方法也返回一致的`LLMResponse`对象，使上层代码能够统一处理。

## 验证测试

创建了测试脚本`test_llm_client.py`，通过以下测试验证修复有效：

1. **基本调用测试**：使用`get_llm_client`函数获取客户端并调用LLM
2. **直接配置测试**：直接创建LLMConfig和LLMClient并调用LLM
3. **SQL生成查询测试**：模拟实际场景下的SQL生成查询

所有与DeepSeek相关的测试都通过，确认问题已解决。

## 后续建议

1. **API版本监控**：定期检查DeepSeek API的更新和变化，及时调整代码
2. **完善错误处理**：考虑添加重试机制和超时设置，进一步提高系统健壮性
3. **备用LLM提供商**：配置备用LLM提供商，当主要提供商不可用时自动切换 

# NL2SQL处理阶段显示优化

### 背景

用户在执行自然语言查询时，需要清晰了解当前处理进度和阶段。之前的实现中，前端只能显示"思考过程 - 处理中(0%)"这样的通用提示，缺乏具体阶段信息。

### 问题

在流式事件处理中，我们发现前端无法正确显示不同的处理阶段，主要有以下几个原因：

1. 后端在发送流式事件时，`type`和`step`字段不一致，导致前端无法正确识别当前阶段
2. 前端处理流式事件时，对事件数据结构的解析不完善
3. 事件在传递过程中的数据结构发生变化，导致处理逻辑错误

### 解决方案

1. **后端修复**:
   - 修改`mcp_adapter.py`中的`stream_nl2sql_response`方法，确保所有流式事件的`type`和`step`字段保持一致
   - 为`complete`事件添加缺失的`step`字段，与`type`值相同
   - 规范化所有事件的数据结构，确保一致性

2. **前端修复**:
   - 修改`mcp-client.js`中的流式事件处理代码，规范化接收到的事件数据
   - 对`thinking`和`progress`事件进行特殊处理，确保`type`和`step`字段一致
   - 增加详细的日志记录，方便调试和问题排查

3. **测试工具**:
   - 创建独立的HTML测试页面(`test_stage_info.html`)，用于直观测试和验证事件处理
   - 开发Python测试脚本(`test_nl2sql_events.py`)，用于系统化测试事件传递和处理

### 验证方式

1. 通过测试工具发送自然语言查询，观察流式事件数据结构
2. 在前端控制台中查看事件处理日志，确认`type`和`step`字段一致
3. 通过前端显示验证不同阶段的名称是否正确显示

### 效果提升

1. 用户现在可以看到明确的处理阶段名称，如"思考过程 - 业务查询判断阶段(10%)"
2. 进度百分比更准确地反映实际处理进度
3. 系统稳定性提高，不再因数据结构不一致导致显示错误

### 后续优化

1. 进一步完善阶段划分，增加更细粒度的处理步骤
2. 添加阶段耗时统计，帮助识别性能瓶颈
3. 为不同阶段添加更详细的说明，提高用户理解 