import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, Dict, Any
from config.settings import config


def setup_logging(log_level: Optional[str] = None, log_file: Optional[str] = None,
                  max_bytes: int = 10 * 1024 * 1024, backup_count: int = 5):
    """设置日志配置"""

    # 获取配置
    level = log_level or config.log.level
    log_file = log_file or config.log.file

    # 创建根日志记录器
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))

    # 清除现有的处理器
    logger.handlers.clear()

    # 创建格式化器
    formatter = logging.Formatter(
        fmt=config.log.format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, level.upper()))
    logger.addHandler(console_handler)

    # 文件处理器（如果配置了日志文件）
    if log_file:
        try:
            # 使用按时间轮转的日志文件
            file_handler = TimedRotatingFileHandler(
                log_file,
                when='midnight',
                interval=1,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(getattr(logging, level.upper()))
            logger.addHandler(file_handler)
        except Exception as e:
            print(f"Failed to setup file logging: {e}")

    # 设置第三方库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    return logger


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的日志记录器"""
    logger = logging.getLogger(name)

    # 如果还没有配置，使用默认配置
    if not logger.handlers:
        setup_logging()

    return logger


class StructuredLogger:
    """结构化日志记录器"""

    def __init__(self, name: str):
        self.logger = get_logger(name)

    def debug(self, message: str, **kwargs):
        """记录调试信息"""
        self._log(logging.DEBUG, message, kwargs)

    def info(self, message: str, **kwargs):
        """记录信息"""
        self._log(logging.INFO, message, kwargs)

    def warning(self, message: str, **kwargs):
        """记录警告"""
        self._log(logging.WARNING, message, kwargs)

    def error(self, message: str, **kwargs):
        """记录错误"""
        self._log(logging.ERROR, message, kwargs)

    def critical(self, message: str, **kwargs):
        """记录严重错误"""
        self._log(logging.CRITICAL, message, kwargs)

    def _log(self, level: int, message: str, extra: Dict[str, Any]):
        """记录日志"""
        if extra:
            # 将额外信息格式化为字符串
            extra_str = ' '.join([f'{k}={v}' for k, v in extra.items()])
            full_message = f"{message} {extra_str}"
        else:
            full_message = message

        self.logger.log(level, full_message)