# NL2SQL与MLX集成测试

本目录包含用于测试NL2SQLProcessor类与MLX本地模型集成的测试脚本。

## 功能特点

- 测试NL2SQLProcessor类的加载和初始化
- 测试MLX库的正确安装
- 测试MLX模型的加载和文本生成
- 测试业务查询判断功能
- 测试完整的NL2SQL处理流程
- 提供详细的测试结果和性能数据

## 前提条件

1. 安装MLX相关依赖：
   ```bash
   pip install mlx mlx-lm
   ```
   
2. 确保有可用的适配MLX的模型（推荐使用Qwen系列模型）

## 环境变量配置

在运行测试前，您可以通过环境变量或`.env`文件配置以下参数：

- `LLM_PROVIDER`: 应设置为"mlx"
- `MLX_MODEL_PATH`: 模型路径，默认为"Qwen/QwQ-32B"
- `MLX_BIT_WIDTH`: 量化位宽，可选值为16（无量化）、8、4、3，默认为4
- `MLX_GROUP_SIZE`: 量化分组大小，默认为64
- `MLX_CACHE_DIR`: 模型缓存目录，默认为"./mlx_models"

## 使用方法

1. 创建`.env`文件（可选）：
   ```
   LLM_PROVIDER=mlx
   MLX_MODEL_PATH=Qwen/QwQ-32B
   MLX_BIT_WIDTH=4
   MLX_GROUP_SIZE=64
   MLX_CACHE_DIR=./mlx_models
   ```

2. 运行测试脚本：
   ```bash
   python test_nl2sql_with_mlx.py
   ```

## 测试流程

该脚本按以下顺序执行测试：

1. **检查MLX安装**：确认MLX相关库是否正确安装
2. **测试MLX模型加载**：尝试加载指定的MLX模型并进行简单的文本生成
3. **加载NL2SQLProcessor**：测试NL2SQLProcessor类的正确导入
4. **初始化NL2SQLProcessor**：测试创建NL2SQLProcessor实例
5. **业务查询判断**：测试NL2SQLProcessor的业务查询判断功能
6. **完整处理流程**：测试完整的NL2SQL转换流程

## 输出说明

测试脚本会输出详细的测试过程信息，并在完成后生成一个包含所有测试结果的JSON文件：`nl2sql_mlx_test_results.json`。

## 注意事项

- 首次运行时，脚本将下载并缓存模型，这可能需要一些时间
- MLX目前主要支持Apple Silicon芯片，在其他平台上可能性能有限
- 测试的数据库功能依赖于系统中正确配置的数据库环境

## 故障排除

如遇到以下问题，可尝试相应解决方法：

1. **MLX库导入失败**：
   - 确认已正确安装MLX：`pip install mlx mlx-lm`
   - 确认系统兼容性，特别是对于非Apple Silicon设备

2. **模型加载失败**：
   - 检查`MLX_MODEL_PATH`是否正确
   - 确认网络连接正常，可以下载模型
   - 检查磁盘空间是否充足

3. **NL2SQLProcessor初始化失败**：
   - 查看错误日志中的具体异常信息
   - 确认项目依赖安装完整
   - 检查数据库配置是否正确

## 性能参考

在Apple M1 Pro上使用不同模型和量化配置的性能参考：

| 模型 | 量化位宽 | 平均生成速度 | 内存占用 |
|------|---------|------------|---------|
| Qwen/QwQ-1.8B | 16位 | 约35 tokens/s | 约3.5GB |
| Qwen/QwQ-1.8B | 4位 | 约45 tokens/s | 约1.2GB |
| Qwen/QwQ-7B | 4位 | 约12 tokens/s | 约4GB |

*注：实际性能会因硬件配置、提示词长度和系统负载而有所差异* 