"""
统一日志配置模块

提供统一的日志处理配置,包括：
- 常规日志：记录所有程序运行信息
- 审计日志：记录关键操作和处理结果的JSON数据
- 错误日志：专门记录程序异常和错误
"""

import os
import sys
import logging
import logging.handlers
import time
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(override=True)

# 获取项目根目录
PROJECT_ROOT = Path(__file__).parents[2].absolute()

# 从环境变量获取日志配置
LOG_DIR = os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs"))
LOG_PREFIX = os.getenv("LOG_PREFIX", "doris_mcp")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_DAYS = int(os.getenv("LOG_MAX_DAYS", "30"))
# 是否在控制台输出日志（对于作为服务运行时应禁用）
CONSOLE_LOGGING = os.getenv("CONSOLE_LOGGING", "false").lower() == "true"
# 是否正在使用 stdio 传输模式
STDIO_MODE = os.getenv("MCP_TRANSPORT_TYPE", "").lower() == "stdio"

# 如果是 stdio 模式，强制禁用控制台日志输出
if STDIO_MODE:
    CONSOLE_LOGGING = False

# 创建日志目录
os.makedirs(LOG_DIR, exist_ok=True)

# 日志文件路径
LOG_FILE = os.path.join(LOG_DIR, f"{LOG_PREFIX}.log")
AUDIT_LOG_FILE = os.path.join(LOG_DIR, f"{LOG_PREFIX}.audit")
ERROR_LOG_FILE = os.path.join(LOG_DIR, f"{LOG_PREFIX}.error")

# 日志级别映射
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# 日志格式
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
AUDIT_FORMAT = '%(asctime)s - %(name)s - %(message)s'
ERROR_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s'

# 审计日志专用级别
AUDIT = 25  # 介于INFO和WARNING之间的级别
logging.addLevelName(AUDIT, "AUDIT")

# 日志对象缓存
_loggers: Dict[str, logging.Logger] = {}

# 处理器类型映射,用于确保不重复添加
_handler_types = {
    'console': logging.StreamHandler,
    'file': logging.handlers.TimedRotatingFileHandler,
    'audit': logging.handlers.TimedRotatingFileHandler,
    'error': logging.handlers.TimedRotatingFileHandler
}

def purge_old_logs():
    """清理过期日志文件"""
    try:
        now = datetime.now()
        log_dir = Path(LOG_DIR)
        for log_file in log_dir.glob(f"{LOG_PREFIX}*.20*"):
            # 解析日期
            file_name = log_file.name
            date_str = None
            
            # 尝试找到日期部分
            parts = file_name.split('.')
            for part in parts:
                if part.startswith('20') and len(part) == 8:  # 20YYMMDD 格式
                    date_str = part
                    break
            
            if date_str:
                try:
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    days_old = (now - file_date).days
                    
                    if days_old > LOG_MAX_DAYS:
                        os.remove(log_file)
                        if not STDIO_MODE:
                            print(f"已删除过期日志文件: {log_file}")
                except (ValueError, OSError) as e:
                    if not STDIO_MODE:
                        print(f"处理日志文件 {file_name} 时出错: {e}")
    except Exception as e:
        if not STDIO_MODE:
            print(f"清理日志时出错: {e}")

def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的日志器
    
    Args:
        name: 日志器名称
        
    Returns:
        logging.Logger: 配置好的日志器
    """
    if name in _loggers:
        return _loggers[name]
    
    # 创建日志器
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))
    
    # 避免日志传播导致的重复日志
    logger.propagate = False
    
    # 检查是否已有处理器,避免重复添加
    handler_types = set(type(h) for h in logger.handlers)
    
    # 添加审计日志方法
    def audit(self, message, *args, **kwargs):
        self.log(AUDIT, message, *args, **kwargs)
    
    logger.audit = audit.__get__(logger)
    
    # 常规日志处理器 - 输出到控制台（仅在启用时）
    if CONSOLE_LOGGING and _handler_types['console'] not in handler_types:
        # 使用stderr而不是stdout,避免与MCP通信冲突
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(console_handler)
    
    # 常规日志处理器 - 按天滚动文件
    if _handler_types['file'] not in handler_types:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            LOG_FILE,
            when='midnight',
            interval=1,
            backupCount=LOG_MAX_DAYS,
            encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        file_handler.suffix = "%Y%m%d"
        logger.addHandler(file_handler)
    
    # 审计日志处理器 - 只记录AUDIT级别
    if _handler_types['audit'] not in handler_types:
        audit_handler = logging.handlers.TimedRotatingFileHandler(
            AUDIT_LOG_FILE,
            when='midnight',
            interval=1,
            backupCount=LOG_MAX_DAYS,
            encoding='utf-8'
        )
        audit_handler.setFormatter(logging.Formatter(AUDIT_FORMAT))
        audit_handler.suffix = "%Y%m%d"
        audit_handler.setLevel(AUDIT)
        audit_handler.addFilter(lambda record: record.levelno == AUDIT)
        logger.addHandler(audit_handler)
    
    # 错误日志处理器 - 只记录ERROR及以上级别
    if _handler_types['error'] not in handler_types:
        error_handler = logging.handlers.TimedRotatingFileHandler(
            ERROR_LOG_FILE,
            when='midnight',
            interval=1,
            backupCount=LOG_MAX_DAYS,
            encoding='utf-8'
        )
        error_handler.setFormatter(logging.Formatter(ERROR_FORMAT))
        error_handler.suffix = "%Y%m%d"
        error_handler.setLevel(logging.ERROR)
        logger.addHandler(error_handler)
    
    # 缓存日志器
    _loggers[name] = logger
    
    return logger

# 默认日志器
logger = get_logger('doris_mcp')

# 审计日志 - 用于记录处理结果、业务操作等
audit_logger = get_logger('audit')

# 启动时清理过期日志
purge_old_logs()