# Ollama Model Testing Tool

This is a tool for testing Ollama models, supporting both regular generation and streaming generation modes, with real-time token speed display.

## Features

- Supports both regular and streaming generation modes
- Real-time token speed and progress display
- Custom prompt and maximum generation length
- Temperature and Top-P parameter adjustment
- Colorful output and progress bars
- Detailed performance statistics

## Installation

First, install the necessary dependencies:

```bash
pip install -r requirements.txt
```

## Prerequisites

1. Make sure Ollama is installed: [https://ollama.com/download](https://ollama.com/download)
2. Ensure the Ollama service is running (default port 11434)
3. Make sure you've pulled the models you want to test (e.g., `ollama pull qwen:1.8b`)

## Usage

### Basic Usage

```bash
python test_ollama.py
```

This will use "如何喂养宠物狐狸？" (How to feed a pet fox?) as the default prompt and load the qwen:1.8b model.

### Command Line Arguments

The following command line arguments are available:

- `--prompt`: Specify the prompt, default is "如何喂养宠物狐狸？"
- `--max_tokens`: Maximum number of tokens to generate, default is 200
- `--model`: Model name, default is "qwen:1.8b"
- `--temperature`: Temperature parameter, default is 0.7
- `--top_p`: Nucleus sampling parameter, default is 0.9
- `--stream_only`: Use only streaming generation mode, skipping regular generation
- `--update_interval`: Update token speed interval (update every N tokens), default is 5
- `--no_color`: Disable colored output
- `--base_url`: Ollama API base URL, default is "http://localhost:11434"

### Examples

#### Using a different prompt:

```bash
python test_ollama.py --prompt "Write a poem about spring"
```

#### Using a different model:

```bash
python test_ollama.py --model "llama3"
```

#### Adjusting temperature parameter:

```bash
python test_ollama.py --temperature 0.3
```

#### Using only streaming mode with custom update interval:

```bash
python test_ollama.py --stream_only --update_interval 10
```

#### Connecting to a remote Ollama service:

```bash
python test_ollama.py --base_url "http://192.168.1.100:11434"
```

## How It Works

### Regular Generation Mode
In regular generation mode, the script generates the complete response at once and then calculates overall performance metrics. This is suitable for scenarios where you need to get the complete result at once.

### Streaming Generation Mode
In streaming generation mode, the script generates the response token by token, displaying each token in real-time and periodically updating speed information. This mode more closely resembles a real conversation experience and allows you to observe the generation process in real-time.

### Parameter Effects
- **Temperature**: Controls randomness, lower values make generation more deterministic, higher values make it more diverse
- **Top-P**: Controls the range of tokens to sample, lower values focus more on high-probability tokens

## Output Description

The script output includes the following sections:

1. **Model Configuration**: Displays model name, temperature settings, etc.
2. **Loading Time**: Time required to load the model
3. **Regular Generation Results**: Complete generation results, time taken, and speed
4. **Streaming Generation Process**: Real-time output and periodic speed updates
5. **Performance Summary**: Overall performance report after completion

## Notes

- Make sure the Ollama service is running, otherwise the script will fail to connect
- Ensure you've downloaded the required models with `ollama pull <model>` before use
- The model name must exactly match the model identifier in Ollama
- For large models, generation may take some time, please be patient
- Colored output may not be supported in some terminals; use the `--no_color` parameter to disable it

## Troubleshooting

If you encounter the following issues, try the corresponding solutions:

1. **Connection Error**:
   - Make sure the Ollama service is running
   - Check if the `--base_url` parameter is correct
   - Try testing the API connection with `curl http://localhost:11434/api/tags`

2. **Model Not Found Error**:
   - Ensure you've downloaded the model with `ollama pull <model>`
   - Check if the model name is correct (case sensitive)

3. **Generation Timeout**:
   - For large models, you may need to increase the timeout
   - Try reducing the `max_tokens` parameter

4. **Poor Generation Quality**:
   - Try adjusting the temperature parameter, lower temperature can make output more deterministic
   - Try different models like llama3, mistral, etc.

5. **Slow Generation Speed**:
   - Try using smaller models
   - Make sure your computer meets the minimum hardware requirements for the model
   - If using GPU, ensure GPU drivers are correctly installed

## Performance Benchmarks

Here are reference performance data on different hardware using various models:

| Hardware | Model | Regular Generation | Streaming Generation | Memory Usage |
|----------|-------|-------------------|---------------------|--------------|
| M1 Pro | qwen:1.8b | ~20 tokens/sec | ~18 tokens/sec | ~4GB |
| M1 Pro | llama3:8b | ~12 tokens/sec | ~10 tokens/sec | ~8GB |
| RTX 3080 | mistral:7b | ~40 tokens/sec | ~35 tokens/sec | ~14GB |
| RTX 3080 | qwen:4b | ~60 tokens/sec | ~55 tokens/sec | ~8GB |

*Note: Actual performance may vary depending on specific hardware, model, and prompt length* 