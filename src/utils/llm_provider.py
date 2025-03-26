"""
LLM提供商枚举类

定义支持的各种LLM服务提供商
"""

from enum import Enum

class LLMProvider(str, Enum):
    """LLM服务提供商枚举"""
    OPENAI = "openai"
    DEEPSEEK = "deepseek"
    SIJILIU = "sijiliu"  # 硅基流动
    VOLCENGINE = "volcengine"  # 火山大模型
    QWEN = "qwen"  # 阿里云Qwen
    OLLAMA = "ollama"  # 本地Ollama
    MLX = "mlx"  # Apple MLX本地模型
    
    @classmethod
    def from_string(cls, provider_name: str) -> "LLMProvider":
        """从字符串获取枚举值"""
        provider_name = provider_name.lower()
        for provider in cls:
            if provider.value == provider_name:
                return provider
        raise ValueError(f"不支持的LLM提供商: {provider_name}") 