# parser/json_parser.py
import json
from typing import Dict, Any, List
from .base_parser import BaseParser


class JSONParser(BaseParser):
    def parse(self) -> Dict[str, Any]:
        """解析JSON数据"""
        try:
            # 处理不同类型的输入
            if isinstance(self.content, str):
                data = json.loads(self.content)
            elif isinstance(self.content, (dict, list)):
                data = self.content
            else:
                data = json.loads(self.content.decode('utf-8'))

            return {
                'data': data,
                'type': 'json',
                'url': self.url
            }
        except json.JSONDecodeError as e:
            return {
                'error': f'JSON decode error: {str(e)}',
                'raw_content': str(self.content)[:500]  # 截取部分内容用于调试
            }
        except Exception as e:
            return {
                'error': f'Parse error: {str(e)}',
                'type': 'json'
            }

    def extract_links(self) -> List[str]:
        """从JSON数据中提取链接"""
        links = []
        try:
            data = self.parse()
            if 'error' in data:
                return links

            # 递归查找所有字符串字段中的URL
            def find_urls(obj):
                if isinstance(obj, dict):
                    for value in obj.values():
                        find_urls(value)
                elif isinstance(obj, list):
                    for item in obj:
                        find_urls(item)
                elif isinstance(obj, str) and obj.startswith(('http://', 'https://')):
                    links.append(obj)

            find_urls(data['data'])
            return links

        except Exception:
            return links

    def extract_by_path(self, json_path: str, default: Any = None) -> Any:
        """根据JSON路径提取数据"""
        try:
            data = self.parse()
            if 'error' in data:
                return default

            # 支持点分隔的路径
            keys = json_path.split('.')
            current = data['data']

            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                elif isinstance(current, list) and key.isdigit():
                    index = int(key)
                    if 0 <= index < len(current):
                        current = current[index]
                    else:
                        return default
                else:
                    return default

            return current

        except Exception:
            return default

    def flatten(self, separator: str = '.') -> Dict[str, Any]:
        """将嵌套的JSON展平为一维字典"""

        def _flatten(obj, parent_key=''):
            items = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_key = f"{parent_key}{separator}{k}" if parent_key else k
                    items.extend(_flatten(v, new_key))
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    new_key = f"{parent_key}{separator}{i}" if parent_key else str(i)
                    items.extend(_flatten(v, new_key))
            else:
                items.append((parent_key, obj))
            return items

        data = self.parse()
        if 'error' in data:
            return {}

        return dict(_flatten(data['data']))