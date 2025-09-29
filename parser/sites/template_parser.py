# parser/sites/template_parser.py
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from ..base_parser import BaseParser


class TemplateParser(BaseParser):
    """解析器模板，用于创建新的站点解析器"""

    def __init__(self, html: str, url: str = None, config: Dict = None):
        super().__init__(html, url)
        self.soup = BeautifulSoup(html, 'lxml')
        self.config = config or {}

    def parse(self) -> Dict[str, Any]:
        """解析页面内容"""
        try:
            return {
                'title': self.extract_title(),
                'content': self.extract_content(),
                'metadata': self.extract_metadata(),
                'links': self.extract_links(),
                'url': self.url
            }
        except Exception as e:
            return {
                'error': f'Parse error: {str(e)}',
                'url': self.url
            }

    def extract_title(self) -> str:
        """提取标题"""
        title_elem = self.soup.find('title')
        return title_elem.get_text().strip() if title_elem else ''

    def extract_content(self) -> str:
        """提取主要内容"""
        # 根据配置选择内容选择器
        content_selector = self.config.get('content_selector', 'body')
        content_elem = self.soup.select_one(content_selector)
        return content_elem.get_text().strip() if content_elem else ''

    def extract_metadata(self) -> Dict[str, str]:
        """提取元数据"""
        metadata = {}
        for meta in self.soup.select('meta[name], meta[property]'):
            name = meta.get('name') or meta.get('property')
            content = meta.get('content', '')
            if name and content:
                metadata[name] = content
        return metadata

    def extract_links(self) -> List[str]:
        """提取链接"""
        links = set()
        for a in self.soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('http'):
                links.add(href)
            elif href.startswith('/'):
                links.add(f"{self._get_base_url()}{href}")
        return list(links)

    def _get_base_url(self) -> str:
        """获取基础URL"""
        from urllib.parse import urlparse
        if self.url:
            parsed = urlparse(self.url)
            return f"{parsed.scheme}://{parsed.netloc}"
        return ''

    def extract_links(self) -> List[str]:
        """实现基类的抽象方法"""
        return self.extract_links()