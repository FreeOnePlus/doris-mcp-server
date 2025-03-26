# NL2SQL with Ollama Integration Test

This directory contains test scripts for testing the integration of NL2SQLProcessor class with Ollama local models.

## Features

- Test NL2SQLProcessor class loading and initialization
- Test Ollama API connection and functionality
- Test business query detection capability
- Test complete NL2SQL processing workflow
- Provide detailed test results and performance data

## Prerequisites

1. Install Ollama from [https://ollama.com/download](https://ollama.com/download)
2. Pull the required model:
   ```bash
   ollama pull llama2
   ```
   (or any other model you plan to use)
3. Ensure Ollama service is running (default port 11434)

## Environment Variable Configuration

Before running the test, you can configure the following parameters via environment variables or a `.env` file:

- `LLM_PROVIDER`: Should be set to "ollama"
- `OLLAMA_MODEL`: The model to use, default is "llama2"
- `OLLAMA_BASE_URL`: Ollama API base URL, default is "http://localhost:11434"

## Usage

1. Create a `.env` file (optional):
   ```
   LLM_PROVIDER=ollama
   OLLAMA_MODEL=llama2
   OLLAMA_BASE_URL=http://localhost:11434
   ```

2. Run the test script:
   ```bash
   python test_nl2sql_with_ollama.py
   ```

## Test Process

The script executes tests in the following order:

1. **Load NL2SQLProcessor**: Tests correct importation of the NL2SQLProcessor class
2. **Initialize NL2SQLProcessor**: Tests creation of a NL2SQLProcessor instance
3. **Business Query Detection**: Tests the business query detection functionality
4. **Complete Processing Flow**: Tests the complete NL2SQL conversion workflow

## Output Description

The test script outputs detailed test process information and generates a JSON file containing all test results at completion: `nl2sql_ollama_test_results.json`.

## Notes

- First-time model loading may take some time depending on the model size
- Performance will depend on your hardware capabilities and the model used
- The database functionality being tested depends on correctly configured database environments

## Troubleshooting

If you encounter issues, try these solutions:

1. **Ollama Connection Errors**:
   - Ensure Ollama service is running: `ps aux | grep ollama`
   - Check if the specified port is accessible: `curl http://localhost:11434/api/version`
   - Verify your firewall settings if using a remote Ollama instance

2. **Model Not Found Errors**:
   - Ensure you've pulled the model: `ollama list`
   - Check if the model name specified matches exactly

3. **NL2SQLProcessor Initialization Failures**:
   - Check the specific exception in the error logs
   - Verify all project dependencies are installed
   - Ensure database configuration is correct

## Performance Reference

Performance reference on different hardware using various models:

| Hardware | Model | Avg. Query Processing | Memory Usage |
|----------|-------|----------------------|--------------|
| MacBook Pro M1 | llama2:7b | ~4 seconds | ~4GB |
| RTX 3080 PC | llama2:7b | ~2 seconds | ~6GB |
| MacBook Pro M1 | mistral:7b | ~3 seconds | ~5GB |

*Note: Actual performance may vary depending on hardware configuration, prompt length, and system load* 