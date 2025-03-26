# MLX Model Testing Tool

This is a tool for testing MLX models, supporting both regular generation and streaming generation modes, with real-time token speed display. The tool also supports various model quantization bit widths to test performance across different precision levels.

## Features

- Supports both regular and streaming generation modes
- Real-time token speed and progress display
- Custom prompt and maximum generation length
- Model quantization bit width selection (16-bit, 8-bit, 4-bit, 3-bit)
- Automatic model download and quantization
- Caching of quantized models for efficiency
- Colorful output and progress bars
- Detailed performance statistics

## Installation

First, install the necessary dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python test_mlx.py
```

This will use "如何喂养宠物狐狸？" (How to feed a pet fox?) as the default prompt and load the Qwen/QwQ-32B model.

### Command Line Arguments

The following command line arguments are available:

- `--prompt`: Specify the prompt, default is "如何喂养宠物狐狸？"
- `--max_tokens`: Maximum number of tokens to generate, default is 200
- `--model_path`: Model path or name, default is "Qwen/QwQ-1.8B"
- `--bit_width`: Model quantization bit width, options are [16, 8, 4, 3], default is 16 (fp16)
- `--group_size`: Quantization group size, default is 64 (only used when bit_width < 16)
- `--stream_only`: Use only streaming generation mode, skipping regular generation
- `--update_interval`: Update token speed interval (update every N tokens), default is 5
- `--no_color`: Disable colored output
- `--cache_dir`: Model cache directory, default is "./mlx_models"
- `--force_convert`: Force model reconversion, even if cache exists

### Examples

#### Using a different prompt:

```bash
python test_mlx.py --prompt "Write a poem about spring"
```

#### Using 4-bit quantization:

```bash
python test_mlx.py --bit_width 4
```

#### Using only streaming mode with custom update interval:

```bash
python test_mlx.py --stream_only --update_interval 10
```

#### Loading a different model:

```bash
python test_mlx.py --model_path "mistralai/Mistral-7B-Instruct-v0.2"
```

#### Using 3-bit quantization with different group size:

```bash
python test_mlx.py --bit_width 3 --group_size 32
```

#### Force model reconversion (ignoring cache):

```bash
python test_mlx.py --bit_width 4 --force_convert
```

#### Specify custom cache directory:

```bash
python test_mlx.py --cache_dir "/path/to/custom/cache"
```

## How It Works

### Model Quantization Process
1. First, the script checks if a quantized model with the corresponding precision exists in the cache directory
2. If no cache is found or the `--force_convert` parameter is used, the script will:
   - Download the original model (if not locally available)
   - Use the `mlx_lm.convert` tool to convert the model to the specified quantization precision
   - Save the quantized model to the cache directory
3. Finally, the quantized model is loaded for testing

### Regular Generation Mode
In regular generation mode, the script generates the complete response at once and then calculates overall performance metrics. This is suitable for scenarios where you need to get the complete result at once.

### Streaming Generation Mode
In streaming generation mode, the script generates the response token by token, displaying each token in real-time and periodically updating speed information. This mode more closely resembles a real conversation experience and allows you to observe the generation process in real-time.

### Impact of Quantization Bit Width
- **16-bit (FP16)**: Default precision, providing the highest generation quality but requiring the most memory
- **8-bit (INT8)**: Medium precision and memory usage, with minimal quality impact for most scenarios
- **4-bit (INT4)**: Low precision, lower memory usage, but may impact quality for complex tasks
- **3-bit (INT3)**: Ultra-low precision, lowest memory usage, but quality may be significantly affected

## Output Description

The script output includes the following sections:

1. **Model Configuration**: Displays model path, quantization bit width, etc.
2. **Model Conversion Process**: Shows real-time progress of model download and conversion
3. **Loading Time**: Time required to load the model
4. **Regular Generation Results**: Complete generation results, time taken, and speed
5. **Streaming Generation Process**: Real-time output and periodic speed updates
6. **Performance Summary**: Overall performance report after completion

## Notes

- On first run, the script needs to download the original model and quantize it, which may take time
- Lower bit width means less memory usage and usually faster generation speed
- For large models, 4-bit or 8-bit quantization is recommended to balance performance and quality
- 3-bit quantization is the most memory-efficient option but may significantly affect output quality
- Colored output may not be supported in some terminals; use the `--no_color` parameter to disable it

## Troubleshooting

If you encounter the following issues, try the corresponding solutions:

1. **Model Conversion Failure**:
   - Check your network connection
   - Ensure you have enough disk space
   - Try using `--force_convert` to reconvert
   - For large models, ensure you have enough memory

2. **Out of Memory Error**:
   - Try using lower quantization bit width (e.g., 4-bit or 3-bit)
   - Choose a smaller model (e.g., QwQ-1.8B instead of QwQ-7B)

3. **Model Loading Failure**:
   - Check if the model name or path is correct
   - Ensure your network connection is stable
   - Try using `--force_convert` to reconvert the model

4. **Poor Generation Quality**:
   - If using low bit width quantization, try higher bit width
   - For high-precision requirements, use 16-bit or 8-bit

5. **Slow Generation Speed**:
   - For large models, try using lower bit width to improve speed
   - Reducing `group_size` may improve speed but might affect quality
   - Use more powerful hardware

## Performance Benchmarks

Here are reference performance data on different hardware using various quantization bit widths:

| Hardware | Model | Bit Width | Regular Generation | Streaming Generation | Memory Usage |
|----------|-------|-----------|-------------------|---------------------|--------------|
| M1 Pro | Qwen/QwQ-1.8B | 16-bit | ~30 tokens/sec | ~25 tokens/sec | ~4GB |
| M1 Pro | Qwen/QwQ-1.8B | 4-bit | ~45 tokens/sec | ~40 tokens/sec | ~1.2GB |
| M2 Max | Mistral-7B | 16-bit | ~20 tokens/sec | ~18 tokens/sec | ~14GB |
| M2 Max | Mistral-7B | 4-bit | ~35 tokens/sec | ~32 tokens/sec | ~4GB |
| M2 Max | Mistral-7B | 3-bit | ~38 tokens/sec | ~35 tokens/sec | ~3GB |

*Note: Actual performance may vary depending on specific hardware, model, and prompt length* 