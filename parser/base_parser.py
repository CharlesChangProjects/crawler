from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseParser(ABC):
    def __init__(self, content: Any, url: str = None):
        self.content = content
        self.url = url

    @abstractmethod
    def parse(self) -> Dict[str, Any]:
        """解析内容"""
        pass

    @abstractmethod
    def extract_links(self) -> list:
        """提取链接"""
        pass