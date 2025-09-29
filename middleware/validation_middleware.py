import json
from typing import Dict, Any
from urllib.parse import urlparse


class ValidationMiddleware:
    """验证中间件 - 验证请求和响应的有效性"""

    def __init__(self):
        self.invalid_urls = set()
        self.validation_rules = {
            'url': self._validate_url,
            'content': self._validate_content,
            'headers': self._validate_headers,
            'status': self._validate_status
        }

    async def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """处理请求前的验证逻辑"""
        # 验证URL
        url = request.get('url', '')
        if not self._validate_url(url):
            self.invalid_urls.add(url)
            raise ValueError(f"Invalid URL: {url}")

        # 验证请求方法
        method = request.get('method', 'GET').upper()
        if method not in ['GET', 'POST', 'HEAD', 'OPTIONS']:
            raise ValueError(f"Invalid HTTP method: {method}")

        # 验证headers
        headers = request.get('headers', {})
        if not self._validate_headers(headers):
            raise ValueError("Invalid headers")

        return request

    async def process_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """处理响应后的验证逻辑"""
        # 验证状态码
        status = response.get('status', 0)
        if not self._validate_status(status):
            raise ValueError(f"Invalid status code: {status}")

        # 验证内容类型
        content_type = response.get('headers', {}).get('content-type', '')
        content = response.get('content', b'')

        if content_type.startswith('application/json') and content:
            if not self._validate_json_content(content):
                raise ValueError("Invalid JSON content")

        # 验证内容长度
        content_length = len(content)
        if content_length > 10 * 1024 * 1024:  # 10MB限制
            raise ValueError("Content too large")

        return response

    def _validate_url(self, url: str) -> bool:
        """验证URL有效性"""
        try:
            result = urlparse(url)
            return all([result.scheme in ['http', 'https'], result.netloc])
        except:
            return False

    def _validate_content(self, content: bytes) -> bool:
        """验证内容有效性"""
        if not content:
            return False

        # 检查是否是二进制文件
        text_content = content.decode('utf-8', errors='ignore')
        if len(text_content) < len(content) * 0.7:  # 70%以上可解码为文本
            return True  # 可能是二进制文件，但也是有效的

        # 检查常见错误页面
        error_indicators = [
            'error', 'exception', 'not found', 'internal server error'
        ]
        text_lower = text_content.lower()
        if any(indicator in text_lower for indicator in error_indicators):
            return False

        return True

    def _validate_headers(self, headers: Dict[str, str]) -> bool:
        """验证headers有效性"""
        if not isinstance(headers, dict):
            return False

        # 检查必要的headers
        required_headers = ['content-type', 'server', 'date']
        for header in required_headers:
            if header not in headers:
                return False

        return True

    def _validate_status(self, status: int) -> bool:
        """验证状态码有效性"""
        return 100 <= status < 600

    def _validate_json_content(self, content: bytes) -> bool:
        """验证JSON内容有效性"""
        try:
            json.loads(content.decode('utf-8'))
            return True
        except json.JSONDecodeError:
            return False

    def get_validation_stats(self) -> Dict[str, Any]:
        """获取验证统计"""
        return {
            'invalid_urls_count': len(self.invalid_urls),
            'invalid_urls': list(self.invalid_urls)[:10]  # 只返回前10个
        }

    def add_validation_rule(self, rule_name: str, validator_func):
        """添加自定义验证规则"""
        self.validation_rules[rule_name] = validator_func

    async def validate_response_schema(self, response: Dict[str, Any], schema: Dict[str, Any]) -> bool:
        """根据模式验证响应结构"""
        # 简单的模式验证实现
        for key, expected_type in schema.items():
            if key not in response:
                return False
            if not isinstance(response[key], expected_type):
                return False
        return True