from typing import Dict, Any, List

from . import JSONParser
from .base_parser import BaseParser


class APIParser(BaseParser):
    def __init__(self, content: Any, url: str = None, api_type: str = 'rest'):
        super().__init__(content, url)
        self.api_type = api_type

    def parse(self) -> Dict[str, Any]:
        """解析API响应"""
        try:
            # 首先尝试JSON解析
            json_parser = JSONParser(self.content, self.url)
            result = json_parser.parse()

            if 'error' not in result:
                # 添加API特定信息
                result['api_type'] = self.api_type
                result['endpoint'] = self._extract_endpoint()
                return result

            # 如果不是JSON，尝试其他格式
            return {
                'raw_content': str(self.content),
                'api_type': self.api_type,
                'endpoint': self._extract_endpoint(),
                'warning': 'Content is not JSON format'
            }

        except Exception as e:
            return {
                'error': f'API parse error: {str(e)}',
                'api_type': self.api_type,
                'url': self.url
            }

    def _extract_endpoint(self) -> str:
        """从URL中提取API端点"""
        if not self.url:
            return 'unknown'

        from urllib.parse import urlparse
        parsed = urlparse(self.url)
        return parsed.path

    def extract_links(self) -> List[str]:
        """从API响应中提取链接"""
        links = []

        # 尝试JSON解析
        json_parser = JSONParser(self.content, self.url)
        json_links = json_parser.extract_links()
        links.extend(json_links)

        # 检查常见的API分页链接
        data = self.parse()
        if 'data' in data and isinstance(data['data'], dict):
            api_data = data['data']

            # 常见分页字段
            pagination_fields = ['next', 'previous', 'first', 'last', 'href', 'url']
            for field in pagination_fields:
                if field in api_data and isinstance(api_data[field], str):
                    links.append(api_data[field])

            # 检查links字段
            if 'links' in api_data and isinstance(api_data['links'], dict):
                for link_url in api_data['links'].values():
                    if isinstance(link_url, str):
                        links.append(link_url)

        return list(set(links))  # 去重

    def is_success_response(self) -> bool:
        """检查是否为成功的API响应"""
        data = self.parse()
        if 'data' in data:
            # 检查常见的成功指标
            if isinstance(data['data'], dict):
                return data['data'].get('success', True)
        return True