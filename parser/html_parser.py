from bs4 import BeautifulSoup
import re
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin
from .base_parser import BaseParser


class HTMLParser(BaseParser):
    def __init__(self, html: str, url: str = None, encoding: str = 'utf-8'):
        super().__init__(html, url)
        self.soup = BeautifulSoup(html, 'lxml', from_encoding=encoding)

    def parse(self) -> Dict[str, Any]:
        """解析HTML页面"""
        return {
            'metadata': self.extract_metadata(),
            'structured_data': self.extract_structured_data(),
            'text': self.extract_text(),
            'links': self.extract_links(),
        }

    def extract_links(self, base_url: str = None, pattern: Optional[str] = None) -> List[str]:
        """提取链接"""
        base_url = base_url or self.url
        links = []

        for a in self.soup.find_all('a', href=True):
            href = a['href']
            full_url = self._make_absolute_url(href, base_url)
            if full_url and (not pattern or re.match(pattern, full_url)):
                links.append(full_url)

        return links

    def _make_absolute_url(self, href: str, base_url: str) -> Optional[str]:
        """转换为绝对URL"""
        try:
            return urljoin(base_url, href)
        except:
            return None

    def extract_text(self, selector: str = None) -> str:
        """提取文本"""
        if selector:
            element = self.soup.select_one(selector)
            return element.get_text().strip() if element else ""
        return self.soup.get_text().strip()

    def extract_metadata(self) -> Dict[str, str]:
        """提取元数据"""
        metadata = {}

        # title
        title_tag = self.soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.get_text().strip()

        # meta tags
        for meta in self.soup.find_all('meta'):
            name = meta.get('name') or meta.get('property') or meta.get('itemprop')
            content = meta.get('content')
            if name and content:
                metadata[name.lower()] = content.strip()

        return metadata

    def extract_structured_data(self) -> Dict[str, Any]:
        """提取结构化数据"""
        data = {}

        # JSON-LD
        for script in self.soup.find_all('script', type='application/ld+json'):
            try:
                import json
                json_data = json.loads(script.string)
                data.setdefault('json_ld', []).append(json_data)
            except:
                pass

        # Microdata
        for item in self.soup.find_all(attrs={'itemscope': True}):
            # 简单的微数据提取
            pass

        return data

    def extract_by_selector(self, selector: str, attr: str = None) -> List[Any]:
        """根据CSS选择器提取内容"""
        elements = self.soup.select(selector)
        if attr:
            return [elem.get(attr) for elem in elements if elem.get(attr)]
        return [elem.get_text().strip() for elem in elements]