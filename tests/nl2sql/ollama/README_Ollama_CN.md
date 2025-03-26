# NL2SQL与Ollama集成测试

本目录包含用于测试NL2SQLProcessor类与Ollama本地模型集成的测试脚本。

## 功能特点

- 测试NL2SQLProcessor类的加载和初始化
- 测试Ollama API连接和功能
- 测试业务查询判断功能
- 测试完整的NL2SQL处理流程
- 提供详细的测试结果和性能数据

## 前提条件

1. 从[https://ollama.com/download](https://ollama.com/download)安装Ollama
2. 拉取所需的模型：
   ```bash
   ollama pull llama2
   ```
   (或其他您计划使用的模型)
3. 确保Ollama服务正在运行（默认端口11434）

## 环境变量配置

在运行测试前，您可以通过环境变量或`.env`文件配置以下参数：

- `LLM_PROVIDER`: 应设置为"ollama"
- `OLLAMA_MODEL`: 要使用的模型，默认为"llama2"
- `OLLAMA_BASE_URL`: Ollama API基础URL，默认为"http://localhost:11434"

## 使用方法

1. 创建`.env`文件（可选）：
   ```
   LLM_PROVIDER=ollama
   OLLAMA_MODEL=llama2
   OLLAMA_BASE_URL=http://localhost:11434
   ```

2. 运行测试脚本：
   ```bash
   python test_nl2sql_with_ollama.py
   ```

## 测试流程

该脚本按以下顺序执行测试：

1. **加载NL2SQLProcessor**：测试NL2SQLProcessor类的正确导入
2. **初始化NL2SQLProcessor**：测试创建NL2SQLProcessor实例
3. **业务查询判断**：测试业务查询判断功能
4. **完整处理流程**：测试完整的NL2SQL转换流程

## 输出说明

测试脚本会输出详细的测试过程信息，并在完成后生成一个包含所有测试结果的JSON文件：`nl2sql_ollama_test_results.json`。

## 注意事项

- 首次加载模型可能需要一些时间，具体取决于模型大小
- 性能将取决于您的硬件能力和所使用的模型
- 测试的数据库功能依赖于系统中正确配置的数据库环境

## 故障排除

如遇到以下问题，可尝试相应解决方法：

1. **Ollama连接错误**：
   - 确认Ollama服务正在运行：`ps aux | grep ollama`
   - 检查指定端口是否可访问：`curl http://localhost:11434/api/version`
   - 如使用远程Ollama实例，请验证防火墙设置

2. **模型未找到错误**：
   - 确保您已拉取模型：`ollama list`
   - 检查指定的模型名称是否完全匹配

3. **NL2SQLProcessor初始化失败**：
   - 查看错误日志中的具体异常信息
   - 确认所有项目依赖已安装
   - 确保数据库配置正确

## 性能参考

在不同硬件上使用各种模型的性能参考：

| 硬件 | 模型 | 平均查询处理时间 | 内存占用 |
|------|------|-----------------|---------|
| MacBook Pro M1 | llama2:7b | 约4秒 | 约4GB |
| RTX 3080 PC | llama2:7b | 约2秒 | 约6GB |
| MacBook Pro M1 | mistral:7b | 约3秒 | 约5GB |

*注：实际性能会因硬件配置、提示词长度和系统负载而有所差异* 