# Doris MCP Server

Doris MCP (Model Control Panel) Server is a Python-based service framework that converts natural language queries into SQL queries through natural language processing, supporting multiple LLM service providers.

## Features

- Supports multiple LLM providers: OpenAI, DeepSeek, Sijiliu, Volcengine, Qwen, Ollama, and Apple MLX
- Natural Language to SQL (NL2SQL) conversion functionality
- Automatic SQL optimization (Incomplete)
- Cluster inspection and maintenance (Incomplete)
- Data export in specified formats (CSV, JSON) (Incomplete)
- Metadata extraction and management
- Database connection and query execution
- Flexible configuration system

## System Requirements

- Python 3.9+
- External LLM API keys (such as OpenAI, DeepSeek, etc.) or local model support (such as Ollama, MLX)
- Database connection information

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/yourusername/doris-mcp-server.git
cd doris-mcp-server
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt

# If using Apple MLX (only for Apple Silicon Mac)
pip install -r requirements-mlx.txt
```

### 3. Configure Environment Variables

Copy the example configuration file and customize it:

```bash
cp .env.example .env
```

Edit the `.env` file to set the following key configurations:
- LLM provider selection
- API keys and endpoints
- Database connection information

### 4. Run the Service

```bash
python src/main.py
```

## Usage Guide

### NL2SQL Service

The NL2SQL service can convert natural language to SQL queries. Example usage:

```python
from src.nl2sql_service import NL2SQLService

# Initialize service
nl2sql_service = NL2SQLService()

# Execute natural language query
result = nl2sql_service.execute_nl_query(
    "Query the total number of orders in the last week",
    "orders table"
)

print(result)
```

### Configuring Different LLM Providers

In the `.env` file, you can choose different LLM providers by setting the `LLM_PROVIDER` environment variable:

```
# Available values: openai, deepseek, sijiliu, qwen, ollama, mlx
LLM_PROVIDER=openai
```

Each provider requires additional specific configuration, see respective provider documentation.

### Multi-LLM Stage Configuration

The system supports using different LLM providers and models at different processing stages, which can optimize performance and cost:

#### Supported Processing Stages
- **business_check**: Business query judgment stage (suitable for lightweight models)
- **similar_example**: Similar example finding stage (suitable for lightweight models)
- **sql_generation**: SQL generation stage (suitable for powerful models)
- **metadata**: Metadata processing stage (suitable for lightweight models)

#### Configuration Method

Add stage-specific configurations in the `.env` file:

```
# Default LLM provider
LLM_PROVIDER=openai
OPENAI_MODEL=gpt-3.5-turbo

# Business query check stage - using local lightweight model
LLM_PROVIDER_BUSINESS_CHECK=ollama
LLM_MODEL_BUSINESS_CHECK=qwen:0.5b

# SQL generation stage - using more powerful model
LLM_PROVIDER_SQL_GENERATION=openai
LLM_MODEL_SQL_GENERATION=gpt-4o
```

#### How It Works

1. The system first looks for stage-specific configuration (like `LLM_PROVIDER_BUSINESS_CHECK`)
2. If not found, it uses the default configuration (`LLM_PROVIDER`)
3. Each stage uses corresponding provider's other configurations (like API keys, temperature, etc.)

#### Advantages

- **Cost Optimization**: Use lightweight models for simple tasks and powerful models for complex tasks
- **Flexibility**: Can mix cloud services and local models
- **Performance Optimization**: Choose the most suitable model for each stage

## MLX Support

This project supports running large language models locally on Apple Silicon Mac using MLX. For detailed information, please refer to [MLX Usage Guide](docs/MLX使用指南.md).

## Directory Structure

```
doris-mcp-server/
├── docs/                # Documentation
├── mlx_models/          # MLX model storage directory
├── src/                 # Source code
│   ├── main.py          # Main program entry
│   ├── nl2sql_service.py # NL2SQL service
│   ├── utils/           # Utility classes
│       ├── db.py        # Database operations
│       ├── llm_client.py # LLM client
│       ├── llm_provider.py # LLM provider enum
│       ├── metadata_extractor.py # Metadata extractor
│       └── nl2sql_processor.py # NL2SQL processor
├── tests/               # Test code
│   ├── nl2sql/          # NL2SQL tests
│   └── mlx/             # MLX tests
├── requirements.txt     # Basic dependencies
├── requirements-mlx.txt # MLX specific dependencies
└── .env                 # Environment variable configuration
```

## Testing

Run tests:

```bash
# Basic functionality tests
python -m unittest discover tests

# MLX client tests
python tests/test_mlx_client.py
```

## Contributing

Issues and pull requests are welcome!

## License

This project is licensed under the MIT License - see the LICENSE file

# DeepSeek LLM API Issue Fix

## Problem Background

During the use of DeepSeek LLM API, the system frequently returned empty responses, causing NL2SQL processing failures, which affected the normal operation of the entire system.

## Root Cause

1. **Incorrect API endpoint**: The correct endpoint for DeepSeek API is `/chat/completions`, while the system was using `/api/v1/chat/completions`.

2. **Misuse of OpenAI client for DeepSeek responses**: Although DeepSeek API is compatible with OpenAI's interface format, there are subtle differences in actual implementation, causing failure to properly receive responses when using OpenAI client to call DeepSeek API.

## Solution

1. **Correct API endpoint**: Change `/api/v1/chat/completions` to `/chat/completions`.

2. **Implement dedicated DeepSeek handling**:
   - Implement specialized handling logic for DeepSeek in the `_chat_openai_compatible` method
   - Use the `requests` library to directly call the API instead of through the OpenAI client
   - Parse returned JSON response, extract content

3. **Enhanced error handling**:
   - Specifically handle request failure cases
   - Return empty content instead of raising exceptions to improve system robustness
   - Add detailed logging for debugging and monitoring

4. **Unified return format**: Ensure the method returns consistent `LLMResponse` objects even when errors occur, allowing upper-level code to handle uniformly.

## Verification Testing

Created test script `test_llm_client.py` to verify the fix through the following tests:

1. **Basic call test**: Get client using `get_llm_client` function and call LLM
2. **Direct configuration test**: Directly create LLMConfig and LLMClient and call LLM
3. **SQL generation query test**: Simulate SQL generation query in actual scenarios

All DeepSeek-related tests passed, confirming the issue is resolved.

## Future Recommendations

1. **API version monitoring**: Regularly check for DeepSeek API updates and changes, adjust code timely
2. **Improve error handling**: Consider adding retry mechanism and timeout settings to further improve system robustness
3. **Backup LLM providers**: Configure backup LLM providers for automatic switching when primary provider is unavailable

# NL2SQL Processing Stage Display Optimization

### Background

Users need to clearly understand the current processing progress and stage when executing natural language queries. In the previous implementation, the frontend could only display generic prompts like "Thinking process - Processing (0%)", lacking specific stage information.

### Problem

In stream event processing, we found that the frontend couldn't correctly display different processing stages, mainly due to:

1. When backend sends stream events, `type` and `step` fields are inconsistent, preventing frontend from correctly identifying current stage
2. Frontend's parsing of event data structure is incomplete when processing stream events
3. Data structure changes during event transmission, causing processing logic errors

### Solution

1. **Backend fixes**:
   - Modify `stream_nl2sql_response` method in `mcp_adapter.py` to ensure consistency of `type` and `step` fields for all stream events
   - Add missing `step` field for `complete` events, matching `type` value
   - Standardize data structure for all events to ensure consistency

2. **Frontend fixes**:
   - Modify stream event handling code in `mcp-client.js` to standardize received event data
   - Special handling for `thinking` and `progress` events to ensure `type` and `step` fields consistency
   - Add detailed logging for debugging and problem investigation

3. **Testing tools**:
   - Create standalone HTML test page (`test_stage_info.html`) for intuitive testing and event handling verification
   - Develop Python test script (`test_nl2sql_events.py`) for systematic testing of event transmission and handling

### Verification Method

1. Send natural language queries through testing tools to observe stream event data structure
2. Check event handling logs in frontend console to confirm `type` and `step` fields consistency
3. Verify correct display of different stage names through frontend display

### Improvements

1. Users can now see clear processing stage names, like "Thinking process - Business query judgment stage (10%)"
2. Progress percentage more accurately reflects actual processing progress
3. System stability improved, no more display errors due to inconsistent data structure

### Future Optimizations

1. Further refine stage division, add more granular processing steps
2. Add stage timing statistics to help identify performance bottlenecks
3. Add more detailed explanations for different stages to improve user understanding

