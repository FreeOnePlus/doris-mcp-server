"""
统一的LLM模型调用接口

支持多种LLM模型：
- OpenAI (GPT-3.5/4)
- DeepSeek
- 硅基流动
- 火山大模型
- 阿里云Qwen
- Ollama (本地模型)
- MLX (Apple MLX本地模型)
"""

import os
import json
import logging
from typing import Dict, List, Optional, Union, Any, Tuple, Callable
import requests
from dotenv import load_dotenv
import importlib.util
import sys
from pathlib import Path

# 导入LLMProvider
from src.utils.llm_provider import LLMProvider

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

class Message:
    """聊天消息"""
    def __init__(self, role: str, content: str):
        self.role = role
        self.content = content
        
    def to_dict(self) -> Dict[str, str]:
        """转换为字典格式"""
        return {"role": self.role, "content": self.content}
    
    @classmethod
    def system(cls, content: str) -> "Message":
        """创建系统消息"""
        return cls("system", content)
    
    @classmethod
    def user(cls, content: str) -> "Message":
        """创建用户消息"""
        return cls("user", content)
    
    @classmethod
    def assistant(cls, content: str) -> "Message":
        """创建助手消息"""
        return cls("assistant", content)

class LLMConfig:
    """LLM配置"""
    def __init__(
        self,
        provider: Union[str, LLMProvider],
        model: str,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        top_p: float = 1.0,
        timeout: int = 60,
        additional_params: Optional[Dict[str, Any]] = None
    ):
        if isinstance(provider, str):
            self.provider = LLMProvider.from_string(provider)
        else:
            self.provider = provider
        
        self.model = model
        self.api_key = api_key
        self.base_url = base_url
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.top_p = top_p
        self.timeout = timeout
        self.additional_params = additional_params or {}
    
    @classmethod
    def from_env(cls, provider_name: str) -> "LLMConfig":
        """从环境变量创建配置"""
        provider = LLMProvider.from_string(provider_name)
        env_prefix = provider.value.upper()
        
        api_key = os.getenv(f"{env_prefix}_API_KEY")
        base_url = os.getenv(f"{env_prefix}_BASE_URL")
        model = os.getenv(f"{env_prefix}_MODEL")
        temperature = float(os.getenv(f"{env_prefix}_TEMPERATURE", "0.7"))
        max_tokens = os.getenv(f"{env_prefix}_MAX_TOKENS")
        if max_tokens:
            max_tokens = int(max_tokens)
        top_p = float(os.getenv(f"{env_prefix}_TOP_P", "1.0"))
        timeout = int(os.getenv(f"{env_prefix}_TIMEOUT", "60"))
        
        # 检查必要参数
        if not api_key and provider not in [LLMProvider.OLLAMA, LLMProvider.MLX]:
            raise ValueError(f"环境变量中缺少 {env_prefix}_API_KEY")
        
        if not model:
            # 设置默认模型
            default_models = {
                LLMProvider.OPENAI: "gpt-3.5-turbo",
                LLMProvider.DEEPSEEK: "deepseek-chat",
                LLMProvider.SIJILIU: "glm-4",
                LLMProvider.VOLCENGINE: "volcengine-lm",
                LLMProvider.QWEN: "qwen-turbo",
                LLMProvider.OLLAMA: "qwq:latest",
                LLMProvider.MLX: "Qwen/QwQ-32B"
            }
            model = default_models.get(provider)
        
        # 设置默认的base_url
        if not base_url:
            default_urls = {
                LLMProvider.OPENAI: "https://api.openai.com",
                LLMProvider.DEEPSEEK: "https://api.deepseek.com",
                LLMProvider.SIJILIU: "https://api.sijiliu.com",
                LLMProvider.VOLCENGINE: "https://volcengineapi.com",
                LLMProvider.QWEN: "https://dashscope.aliyuncs.com",
                LLMProvider.OLLAMA: "http://localhost:11434"
            }
            base_url = default_urls.get(provider)
        
        # 额外参数
        additional_params = {}
        
        # MLX特定参数
        if provider == LLMProvider.MLX:
            additional_params.update({
                "bit_width": int(os.getenv("MLX_BIT_WIDTH", "4")),
                "group_size": int(os.getenv("MLX_GROUP_SIZE", "64")),
                "cache_dir": os.getenv("MLX_CACHE_DIR", "./mlx_models")
            })
        
        return cls(
            provider=provider,
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            timeout=timeout,
            additional_params=additional_params
        )

class LLMResponse:
    """LLM响应"""
    def __init__(
        self,
        content: str,
        model: str,
        usage: Optional[Dict[str, int]] = None,
        finish_reason: Optional[str] = None,
        raw_response: Optional[Any] = None
    ):
        self.content = content
        self.model = model
        self.usage = usage or {}
        self.finish_reason = finish_reason
        self.raw_response = raw_response

class LLMClient:
    """统一的LLM客户端"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self._setup_client()
    
    def _setup_client(self):
        """根据提供商设置客户端"""
        if self.config.provider in [LLMProvider.OPENAI, LLMProvider.DEEPSEEK]:
            # 检查OpenAI包是否已安装
            openai_spec = importlib.util.find_spec("openai")
            if openai_spec is None:
                logger.error("OpenAI包未安装,请运行: pip install openai")
                raise ImportError("请安装OpenAI包: pip install openai")
            
            try:
                from openai import OpenAI
                import httpx
                
                # 直接使用httpx设置超时和重试
                timeout = httpx.Timeout(connect=5.0, read=300.0, write=60.0, pool=10.0)
                
                # 捕获"can't register atexit after shutdown"错误
                try:
                    # 直接创建OpenAI客户端,设置超时和重试
                    self._client = OpenAI(
                        api_key=self.config.api_key,
                        base_url=self.config.base_url,
                        timeout=timeout,  # 直接传递timeout对象
                        max_retries=5     # 设置最大重试次数
                    )
                except RuntimeError as e:
                    if "can't register atexit after shutdown" in str(e):
                        logger.warning("程序正在退出,无法创建OpenAI客户端")
                        self._client = None
                    else:
                        raise e
                    
            except ImportError as e:
                logger.error(f"导入OpenAI包时出错: {str(e)}")
                raise ImportError(f"导入OpenAI包时出错: {str(e)}")
        elif self.config.provider == LLMProvider.MLX:
            # 检查MLX包是否已安装
            mlx_spec = importlib.util.find_spec("mlx")
            mlx_lm_spec = importlib.util.find_spec("mlx_lm")
            
            if mlx_spec is None or mlx_lm_spec is None:
                logger.error("MLX或MLX-LM包未安装,请运行: pip install mlx mlx-lm")
                raise ImportError("请安装MLX包: pip install mlx mlx-lm")
            
            try:
                # 这里不实际加载模型,而是在调用时加载
                logger.info(f"MLX模式已就绪,将使用模型: {self.config.model}")
            except Exception as e:
                logger.error(f"初始化MLX客户端时出错: {str(e)}")
                raise
        
        # 其他提供商使用requests直接调用API
    
    def chat(self, messages: List[Union[Message, Dict[str, str]]], stream: bool = False) -> LLMResponse:
        """
        调用LLM进行聊天
        
        Args:
            messages: 消息列表
            stream: 是否使用流式输出
            
        Returns:
            LLMResponse: LLM响应
        """
        # 转换消息格式
        formatted_messages = []
        for message in messages:
            if isinstance(message, Message):
                formatted_messages.append(message.to_dict())
            else:
                formatted_messages.append(message)
        
        # 验证并修复消息序列,防止连续的用户或助手消息（DeepSeek模型不支持）
        formatted_messages = self._validate_message_sequence(formatted_messages)
                
        # 根据不同提供商调用不同的方法
        if self.config.provider in [LLMProvider.OPENAI, LLMProvider.DEEPSEEK]:
            return self._chat_openai_compatible(formatted_messages, stream)
        elif self.config.provider == LLMProvider.SIJILIU:
            return self._chat_sijiliu(formatted_messages, stream)
        elif self.config.provider == LLMProvider.VOLCENGINE:
            return self._chat_volcengine(formatted_messages, stream)
        elif self.config.provider == LLMProvider.QWEN:
            return self._chat_qwen(formatted_messages, stream)
        elif self.config.provider == LLMProvider.OLLAMA:
            return self._chat_ollama(formatted_messages, stream)
        elif self.config.provider == LLMProvider.MLX:
            return self._chat_mlx(formatted_messages, stream)
        else:
            raise ValueError(f"不支持的LLM提供商: {self.config.provider}")
    
    def _validate_message_sequence(self, messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        验证并修复消息序列,确保没有连续的用户或助手消息
        
        Args:
            messages: 消息列表
            
        Returns:
            List[Dict[str, str]]: 修复后的消息列表
        """
        if not messages or len(messages) <= 1:
            return messages
            
        # 检查是否使用DeepSeek提供商（该模型特别要求消息交替）
        is_deepseek = self.config.provider == LLMProvider.DEEPSEEK
        
        # 定义合并两个消息内容的函数
        def merge_messages(msg1, msg2):
            return {
                "role": msg1["role"],
                "content": f"{msg1['content']}\n\n{msg2['content']}"
            }
            
        # 修复消息序列
        fixed_messages = [messages[0]]  # 保留第一个消息
        
        for i in range(1, len(messages)):
            curr_msg = messages[i]
            prev_msg = fixed_messages[-1]
            
            # 如果当前消息与前一个消息角色相同
            if curr_msg["role"] == prev_msg["role"]:
                if is_deepseek:
                    # 对于DeepSeek,合并相同角色的消息
                    logger.warning(f"检测到连续的 {curr_msg['role']} 角色消息,正在合并...")
                    fixed_messages[-1] = merge_messages(prev_msg, curr_msg)
                else:
                    # 对于其他模型,添加警告但允许连续消息
                    logger.warning(f"检测到连续的 {curr_msg['role']} 角色消息,某些模型可能不支持")
                    fixed_messages.append(curr_msg)
            else:
                # 角色不同,直接添加
                fixed_messages.append(curr_msg)
                
        # 确保最终消息序列的合法性
        if is_deepseek and len(fixed_messages) > 1:
            roles = [msg["role"] for msg in fixed_messages]
            logger.info(f"最终消息角色序列: {roles}")
            
        return fixed_messages
    
    def _chat_openai_compatible(self, messages: List[Dict[str, str]], stream: bool = False) -> LLMResponse:
        """
        调用OpenAI兼容接口（OpenAI和DeepSeek）
        """
        try:
            # 构建请求参数
            params = {
                "model": self.config.model,
                "messages": messages,
                "temperature": self.config.temperature,
                "top_p": self.config.top_p,
                "stream": stream
            }
            
            if self.config.max_tokens:
                params["max_tokens"] = self.config.max_tokens
                
            # 添加额外参数
            params.update(self.config.additional_params)
            
            # 调用API
            if stream:
                content = ""
                for chunk in self._client.chat.completions.create(**params):
                    if hasattr(chunk, 'choices') and len(chunk.choices) > 0:
                        delta = chunk.choices[0].delta.content
                        if delta:
                            content += delta
                            
                # 构建简单响应
                return LLMResponse(
                    content=content,
                    model=self.config.model,
                    raw_response=None
                )
            else:
                # 非流式响应
                response = self._client.chat.completions.create(**params)
                
                # 构建响应
                return LLMResponse(
                    content=response.choices[0].message.content,
                    model=response.model,
                    usage=response.usage.model_dump() if hasattr(response, 'usage') else {},
                    finish_reason=response.choices[0].finish_reason,
                    raw_response=response
                )
        except Exception as e:
            logger.error(f"调用OpenAI兼容接口时出错: {str(e)}")
            raise
    
    def _chat_mlx(self, messages: List[Dict[str, str]], stream: bool = False) -> LLMResponse:
        """
        使用MLX本地模型生成响应
        """
        try:
            # 导入MLX相关模块
            from mlx_lm import load, generate, stream_generate
            from mlx_lm.utils import make_sampler
            import time
            
            # 获取模型配置
            model_path = self.config.model
            bit_width = self.config.additional_params.get("bit_width", 4)
            group_size = self.config.additional_params.get("group_size", 64)
            cache_dir = self.config.additional_params.get("cache_dir", "./mlx_models")

            # 确保缓存目录存在
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            
            # 构建模型配置
            model_config = {}
            if bit_width < 16:
                model_config["quantize"] = True
                model_config["q_group_size"] = group_size
                model_config["q_bits"] = bit_width
            
            logger.info(f"加载MLX模型: {model_path}, 量化位宽: {bit_width}位, 分组大小: {group_size}")
            start_time = time.time()
            
            # 加载模型
            model, tokenizer = load(model_path, model_config=model_config)
            loading_time = time.time() - start_time
            logger.info(f"MLX模型加载完成,耗时: {loading_time:.2f}秒")
            
            # 提取提示词,将消息列表转换为单个字符串提示词
            prompt = self._format_messages_for_mlx(messages)
            
            # 创建采样器 - 使用温度参数
            temp = self.config.temperature
            logger.info(f"使用温度参数: {temp}")
            sampler = make_sampler(temp=temp)
            
            # 生成选项 - 注意：不同版本的MLX_LM可能支持不同的参数
            # 根据错误信息来看,generate_step()不支持temperature参数
            generation_args = {
                "prompt": prompt,
                "max_tokens": self.config.max_tokens if self.config.max_tokens else 512,
                "sampler": sampler  # 使用我们创建的采样器
            }
            
            if stream:
                # 流式生成
                content = ""
                start_time = time.time()
                logger.info("开始流式生成...")
                
                for token, _ in stream_generate(model, tokenizer, **generation_args):
                    content += token
                
                generation_time = time.time() - start_time
                logger.info(f"流式生成完成,耗时: {generation_time:.2f}秒")
            else:
                # 非流式生成
                start_time = time.time()
                logger.info("开始生成...")
                
                content = generate(model, tokenizer, **generation_args)
                
                generation_time = time.time() - start_time
                logger.info(f"生成完成,耗时: {generation_time:.2f}秒")
            
            # 构建用量统计
            usage = {
                "prompt_tokens": len(prompt),
                "completion_tokens": len(content),
                "total_tokens": len(prompt) + len(content),
                "loading_time": loading_time,
                "generation_time": generation_time
            }
            
            return LLMResponse(
                content=content,
                model=f"MLX-{model_path}",
                usage=usage,
                finish_reason="stop",
                raw_response=None
            )
        except Exception as e:
            logger.error(f"使用MLX生成时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def _format_messages_for_mlx(self, messages: List[Dict[str, str]]) -> str:
        """
        将消息列表格式化为MLX模型可以处理的提示词
        """
        prompt = ""
        for message in messages:
            role = message.get("role", "")
            content = message.get("content", "")
            
            if role == "system":
                prompt += f"<系统>: {content}\n\n"
            elif role == "user":
                prompt += f"<用户>: {content}\n\n"
            elif role == "assistant":
                prompt += f"<助手>: {content}\n\n"
            else:
                prompt += f"{content}\n\n"
        
        # 添加最后的标记,提示模型开始回复
        prompt += "<助手>: "
        
        return prompt
        
    def _chat_sijiliu(self, messages: List[Dict[str, str]], stream: bool = False) -> LLMResponse:
        """
        调用硅基流动API
        """
        try:
            # 构建请求头
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.config.api_key}"
            }
            
            # 构建请求体
            data = {
                "model": self.config.model,
                "messages": messages,
                "temperature": self.config.temperature,
                "top_p": self.config.top_p,
                "stream": stream
            }
            
            if self.config.max_tokens:
                data["max_tokens"] = self.config.max_tokens
                
            # 添加额外参数
            data.update(self.config.additional_params)
            
            # 构建URL
            url = f"{self.config.base_url}/api/v1/chat/completions"
            
            # 发送请求
            if stream:
                # 流式响应
                response = requests.post(url, headers=headers, json=data, stream=True, timeout=self.config.timeout)
                response.raise_for_status()
                
                content = ""
                for line in response.iter_lines():
                    if line:
                        line = line.decode('utf-8')
                        if line.startswith('data: '):
                            json_line = line[6:]  # 跳过"data: "前缀
                            if json_line == "[DONE]":
                                break
                            try:
                                chunk = json.loads(json_line)
                                if 'choices' in chunk and len(chunk['choices']) > 0:
                                    delta = chunk['choices'][0].get('delta', {}).get('content', '')
                                    if delta:
                                        content += delta
                            except json.JSONDecodeError:
                                pass
                                
                # 构建响应
                return LLMResponse(
                    content=content,
                    model=self.config.model,
                    raw_response=None
                )
            else:
                # 非流式响应
                response = requests.post(url, headers=headers, json=data, timeout=self.config.timeout)
                response.raise_for_status()
                
                # 解析响应
                result = response.json()
                
                # 构建响应
                return LLMResponse(
                    content=result['choices'][0]['message']['content'],
                    model=result['model'],
                    usage=result.get('usage', {}),
                    finish_reason=result['choices'][0].get('finish_reason'),
                    raw_response=result
                )
        except Exception as e:
            logger.error(f"调用硅基流动API时出错: {str(e)}")
            raise
    
    def _chat_volcengine(self, messages: List[Dict[str, str]], stream: bool = False) -> LLMResponse:
        """
        调用火山大模型API
        """
        try:
            # 构建请求头
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.config.api_key}"
            }
            
            # 构建请求体 - 火山API格式可能与OpenAI有所不同
            data = {
                "model": self.config.model,
                "parameters": {
                    "temperature": self.config.temperature,
                    "top_p": self.config.top_p,
                    "stream": stream
                },
                "messages": messages
            }
            
            if self.config.max_tokens:
                data["parameters"]["max_tokens"] = self.config.max_tokens
                
            # 添加额外参数
            data.update(self.config.additional_params)
            
            # 构建URL
            url = f"{self.config.base_url}/api/v1/chat/completions"
            
            # 发送请求
            if stream:
                # 流式响应
                response = requests.post(url, headers=headers, json=data, stream=True, timeout=self.config.timeout)
                response.raise_for_status()
                
                content = ""
                for line in response.iter_lines():
                    if line:
                        line = line.decode('utf-8')
                        if line.startswith('data: '):
                            json_line = line[6:]  # 跳过"data: "前缀
                            if json_line == "[DONE]":
                                break
                            try:
                                chunk = json.loads(json_line)
                                if 'choices' in chunk and len(chunk['choices']) > 0:
                                    delta = chunk['choices'][0].get('delta', {}).get('content', '')
                                    if delta:
                                        content += delta
                            except json.JSONDecodeError:
                                pass
                                
                # 构建响应
                return LLMResponse(
                    content=content,
                    model=self.config.model,
                    raw_response=None
                )
            else:
                # 非流式响应
                response = requests.post(url, headers=headers, json=data, timeout=self.config.timeout)
                response.raise_for_status()
                
                # 解析响应
                result = response.json()
                
                # 构建响应 - 可能需要根据实际API响应调整
                return LLMResponse(
                    content=result['choices'][0]['message']['content'],
                    model=result.get('model', self.config.model),
                    usage=result.get('usage', {}),
                    finish_reason=result['choices'][0].get('finish_reason'),
                    raw_response=result
                )
        except Exception as e:
            logger.error(f"调用火山大模型API时出错: {str(e)}")
            raise
    
    def _chat_qwen(self, messages: List[Dict[str, str]], stream: bool = False) -> LLMResponse:
        """
        调用阿里云Qwen API
        """
        try:
            # 构建请求头
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.config.api_key}",
                "X-DashScope-API-Key": self.config.api_key
            }
            
            # 构建请求体 - Qwen API专用格式
            data = {
                "model": self.config.model,
                "input": {
                    "messages": messages
                },
                "parameters": {
                    "temperature": self.config.temperature,
                    "top_p": self.config.top_p,
                    "result_format": "message",
                    "stream": stream
                }
            }
            
            if self.config.max_tokens:
                data["parameters"]["max_tokens"] = self.config.max_tokens
                
            # 添加额外参数
            for key, value in self.config.additional_params.items():
                if key not in data["parameters"]:
                    data["parameters"][key] = value
            
            # 构建URL
            url = f"{self.config.base_url}/api/v1/services/aigc/text-generation/generation"
            
            # 发送请求
            if stream:
                # 流式响应
                response = requests.post(url, headers=headers, json=data, stream=True, timeout=self.config.timeout)
                response.raise_for_status()
                
                content = ""
                for line in response.iter_lines():
                    if line:
                        line = line.decode('utf-8')
                        try:
                            chunk = json.loads(line)
                            if 'output' in chunk and 'text' in chunk['output']:
                                delta = chunk['output']['text']
                                if delta:
                                    content += delta
                        except json.JSONDecodeError:
                            pass
                
                # 构建响应
                return LLMResponse(
                    content=content,
                    model=self.config.model,
                    raw_response=None
                )
            else:
                # 非流式响应
                response = requests.post(url, headers=headers, json=data, timeout=self.config.timeout)
                response.raise_for_status()
                
                # 解析响应
                result = response.json()
                
                # 构建响应 - 针对Qwen API的响应格式
                content = result.get('output', {}).get('text', '')
                usage = result.get('usage', {})
                
                return LLMResponse(
                    content=content,
                    model=result.get('model', self.config.model),
                    usage=usage,
                    finish_reason=result.get('output', {}).get('finish_reason'),
                    raw_response=result
                )
        except Exception as e:
            logger.error(f"调用阿里云Qwen API时出错: {str(e)}")
            raise
    
    def _parse_ollama_response(self, response_text: str) -> Dict[str, Any]:
        """
        安全解析Ollama的响应,处理可能的多行JSON或格式错误
        保留原始响应完整性,采用提取而非删除的策略
        
        Args:
            response_text: Ollama的原始响应文本
            
        Returns:
            Dict[str, Any]: 解析后的JSON数据
        """
        if not response_text or not response_text.strip():
            return {}
        
        # 1. 优先从```json代码块中提取内容
        json_block_start = response_text.find('```json')
        json_block_end = response_text.find('```', json_block_start + 6) if json_block_start >= 0 else -1
        
        if json_block_start >= 0 and json_block_end > json_block_start:
            # 提取```json和```之间的内容
            json_content = response_text[json_block_start + 7:json_block_end].strip()
            logger.debug("从Markdown代码块中提取JSON内容")
            try:
                return json.loads(json_content)
            except json.JSONDecodeError:
                logger.debug("代码块内容解析失败,尝试其他方法")
        
        # 2. 检查是否有<think>标签,如果有,尝试提取标签外的JSON
        think_start = response_text.find('<think>')
        think_end = response_text.find('</think>')
        
        if think_start >= 0 and think_end > think_start:
            # 尝试在</think>标签后查找JSON
            post_think_text = response_text[think_end + 8:]
            try:
                # 尝试解析</think>后的内容
                return self._extract_json_from_text(post_think_text)
            except json.JSONDecodeError:
                logger.debug("</think>标签后的内容解析失败,尝试其他方法")
        
        # 3. 如果以上方法都失败,尝试从完整内容中提取JSON
        return self._extract_json_from_text(response_text)
    
    def _extract_json_from_text(self, text: str) -> Dict[str, Any]:
        """
        从文本中提取JSON对象
        
        Args:
            text: 可能包含JSON的文本
            
        Returns:
            Dict[str, Any]: 解析后的JSON数据
        """
        # 尝试直接解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # 尝试只解析第一行
            try:
                lines = text.strip().split('\n')
                first_line = lines[0].strip()
                if first_line and first_line.startswith('{') and first_line.endswith('}'):
                    return json.loads(first_line)
            except (json.JSONDecodeError, IndexError):
                pass
            
            # 尝试提取JSON部分 - 从第一个{到最后一个}
            try:
                json_start = text.find('{')
                json_end = text.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_part = text[json_start:json_end]
                    # 处理特殊情况,如果JSON包含转义的尖括号如\<,先将其标准化
                    json_part = json_part.replace('\\<', '<').replace('\\>', '>')
                    return json.loads(json_part)
            except (json.JSONDecodeError, ValueError):
                pass
            
            # 所有尝试失败,返回空字典
            logger.warning(f"无法从文本中提取JSON: {text[:100]}...")
            return {}
    
    def _chat_ollama(self, messages: List[Dict[str, str]], stream: bool = False) -> LLMResponse:
        """
        调用Ollama本地模型
        """
        try:
            # 构建请求体
            data = {
                "model": self.config.model,
                "messages": messages,
                "options": {
                    "temperature": self.config.temperature,
                    "top_p": self.config.top_p,
                    "stream": stream
                }
            }
            
            if self.config.max_tokens:
                data["options"]["num_predict"] = self.config.max_tokens
                
            # 添加额外参数
            for key, value in self.config.additional_params.items():
                if key not in data["options"]:
                    data["options"][key] = value
            
            # 构建URL
            url = f"{self.config.base_url}/api/chat"
            
            # 增加timeout时间,确保有足够时间获取完整响应
            timeout = self.config.timeout * 2  # 双倍超时时间
            
            # 发送请求
            if stream:
                # 流式响应
                response = requests.post(url, json=data, stream=True, timeout=timeout)
                response.raise_for_status()
                
                content = ""
                for line in response.iter_lines():
                    if line:
                        try:
                            chunk = json.loads(line)
                            if 'message' in chunk and 'content' in chunk['message']:
                                # 对于Ollama,每个流式响应包含完整的累积内容
                                # 我们需要提取新的内容部分
                                new_content = chunk['message']['content']
                                if new_content and len(new_content) > len(content):
                                    delta = new_content[len(content):]
                                    content = new_content
                        except json.JSONDecodeError:
                            pass
                                
                # 构建响应
                return LLMResponse(
                    content=content,
                    model=self.config.model,
                    raw_response=None
                )
            else:
                # 非流式响应 - 增加超时时间
                logger.info(f"向Ollama发送非流式请求,超时时间: {timeout}秒")
                response = requests.post(url, json=data, timeout=timeout)
                response.raise_for_status()
                
                # 记录响应内容的前100个字符,用于调试
                response_preview = response.text[:100] + ("..." if len(response.text) > 100 else "")
                logger.debug(f"Ollama原始响应(前100字符): {response_preview}")
                
                # 完整记录响应内容（仅开发环境）
                if os.getenv("DEVELOPMENT_MODE", "false").lower() == "true":
                    logger.debug(f"Ollama完整响应内容: {response.text}")
                
                # 安全解析响应JSON
                try:
                    # 使用专门的解析函数处理Ollama响应
                    result = self._parse_ollama_response(response.text)
                    if not result:
                        # 获取原始内容作为响应,而不是返回错误
                        # 这样可以确保即使JSON解析失败,原始文本内容也能返回
                        raw_content = response.text
                        # 检查是否有思考标签,提取有用的部分
                        if "<think>" in raw_content and "</think>" in raw_content:
                            logger.info("检测到<think>标签,提取标签后的内容")
                            post_think = raw_content.split("</think>", 1)
                            raw_content = post_think[1] if len(post_think) > 1 else raw_content
                        
                        logger.info(f"无法解析为JSON,返回原始响应内容,长度: {len(raw_content)}")
                        return LLMResponse(
                            content=raw_content.strip(),
                            model=self.config.model,
                            usage={},
                            finish_reason="error_fallback",
                            raw_response=response.text  # 保存完整原始响应
                        )
                except Exception as e:
                    logger.error(f"解析Ollama响应时出错: {str(e)}")
                    raw_content = response.text
                    # 检查是否有思考标签,提取有用的部分
                    if "<think>" in raw_content and "</think>" in raw_content:
                        logger.info("检测到<think>标签,提取标签后的内容")
                        post_think = raw_content.split("</think>", 1)
                        raw_content = post_think[1] if len(post_think) > 1 else raw_content
                    
                    return LLMResponse(
                        content=raw_content.strip(),
                        model=self.config.model, 
                        usage={},
                        finish_reason="error_fallback",
                        raw_response=response.text  # 保存完整原始响应
                    )
                
                # 构建响应
                content = result.get('message', {}).get('content', '')
                
                # 如果解析后的内容为空,但原始响应不为空,则返回原始响应
                if not content and response.text.strip():
                    content = response.text.strip()
                    logger.info(f"从解析结果中未获取到内容,使用原始响应, 长度: {len(content)}")
                
                # 构建用量统计
                usage = {}
                if 'eval_count' in result:
                    usage['completion_tokens'] = result['eval_count']
                if 'prompt_eval_count' in result:
                    usage['prompt_tokens'] = result['prompt_eval_count']
                    usage['total_tokens'] = usage.get('completion_tokens', 0) + result['prompt_eval_count']
                
                return LLMResponse(
                    content=content,
                    model=result.get('model', self.config.model),
                    usage=usage,
                    finish_reason="stop",
                    raw_response=result
                )
        except Exception as e:
            logger.error(f"调用Ollama本地模型时出错: {str(e)}")
            raise

def get_llm_client(provider_name: Optional[str] = None, stage: Optional[str] = None) -> Optional[LLMClient]:
    """
    获取LLM客户端
    
    Args:
        provider_name: 提供商名称,如果为None则使用环境变量中的LLM_PROVIDER
        stage: 处理阶段,用于选择不同的模型配置（如果配置了特定阶段的模型）
        
    Returns:
        LLMClient: LLM客户端,如果创建失败则返回None
    """
    try:
        # 如果指定了处理阶段且环境变量中有该阶段的配置,则使用该阶段的配置
        if stage:
            stage_provider_key = f"LLM_PROVIDER_{stage.upper()}"
            stage_provider = os.getenv(stage_provider_key)
            if stage_provider:
                logger.info(f"使用{stage}阶段配置的LLM供应商: {stage_provider}")
                provider_name = stage_provider
        
        # 如果没有指定提供商,则使用环境变量中的默认值
        if not provider_name:
            provider_name = os.getenv("LLM_PROVIDER", "openai")
        
        # 创建配置
        try:
            config = LLMConfig.from_env(provider_name)
            
            # 如果指定了处理阶段,尝试使用该阶段的特定模型
            if stage:
                stage_model_key = f"LLM_MODEL_{stage.upper()}"
                stage_model = os.getenv(stage_model_key)
                if stage_model:
                    logger.info(f"使用{stage}阶段配置的模型: {stage_model}")
                    config.model = stage_model
            
            # 创建客户端
            client = LLMClient(config)
            logger.info(f"使用{provider_name}供应商的模型: {config.model}")
            return client
            
        except (ValueError, ImportError) as e:
            logger.error(f"创建LLM配置时出错: {str(e)}")
            
            # 如果不是默认提供商,则尝试回退到默认提供商
            if provider_name != "openai":
                logger.info(f"尝试回退到OpenAI提供商")
                try:
                    config = LLMConfig.from_env("openai")
                    client = LLMClient(config)
                    logger.info("已回退到OpenAI提供商")
                    return client
                except Exception as e2:
                    logger.error(f"回退到OpenAI提供商时出错: {str(e2)}")
            
            # 如果是程序退出导致的错误,返回None
            if "can't register atexit after shutdown" in str(e):
                logger.warning("程序正在退出,无法创建LLM客户端")
                return None
                
            # 尝试加载DeepSeek
            try:
                logger.info("尝试使用DeepSeek提供商")
                config = LLMConfig.from_env("deepseek")
                client = LLMClient(config)
                logger.info("已使用DeepSeek提供商")
                return client
            except Exception as e3:
                logger.error(f"使用DeepSeek提供商时出错: {str(e3)}")
            
            # 最后尝试Ollama本地模型
            try:
                logger.info("尝试使用Ollama本地模型")
                config = LLMConfig.from_env("ollama")
                client = LLMClient(config)
                logger.info("已使用Ollama本地模型")
                return client
            except Exception as e4:
                logger.error(f"使用Ollama本地模型时出错: {str(e4)}")
                logger.error("所有LLM提供商均不可用")
                return None
    
    except RuntimeError as e:
        if "can't register atexit after shutdown" in str(e):
            logger.warning("程序正在退出,无法创建LLM客户端")
            return None
        else:
            logger.error(f"获取LLM客户端时出错: {str(e)}")
            return None
    except Exception as e:
        logger.error(f"获取LLM客户端时出错: {str(e)}")
        return None

def get_llm_providers() -> Dict[str, str]:
    """
    获取系统中所有可用的LLM提供商及其默认模型
    
    Returns:
        Dict[str, str]: 提供商名称到默认模型的映射
    """
    providers = {}
    
    # 检查环境变量中配置的提供商
    for provider in LLMProvider:
        env_prefix = provider.value.upper()
        if os.getenv(f"{env_prefix}_API_KEY") or provider in [LLMProvider.OLLAMA, LLMProvider.MLX]:
            model = os.getenv(f"{env_prefix}_MODEL")
            
            # 如果未设置模型，使用默认值
            if not model:
                default_models = {
                    LLMProvider.OPENAI: "gpt-3.5-turbo",
                    LLMProvider.DEEPSEEK: "deepseek-chat",
                    LLMProvider.SIJILIU: "glm-4",
                    LLMProvider.VOLCENGINE: "volcengine-lm",
                    LLMProvider.QWEN: "qwen-turbo",
                    LLMProvider.OLLAMA: "llama3:latest",
                    LLMProvider.MLX: "Qwen/QwQ-32B"
                }
                model = default_models.get(provider, "unknown")
            
            # 添加到可用提供商列表
            providers[provider.value] = model
    
    return providers 