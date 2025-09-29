from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List
import hashlib


class PageType(Enum):
    """页面类型枚举"""
    UNKNOWN = "unknown"
    PRODUCT = "product"
    CATEGORY = "category"
    SEARCH = "search"
    HOME = "home"
    LISTING = "listing"
    DETAIL = "detail"
    API = "api"


class ContentType(Enum):
    """内容类型枚举"""
    HTML = "text/html"
    JSON = "application/json"
    XML = "application/xml"
    TEXT = "text/plain"
    BINARY = "application/octet-stream"


@dataclass
class Page:
    """页面数据模型"""

    # 标识信息
    id: str = field(default_factory=lambda: str(hashlib.md5().hexdigest()[:16]))
    url: str
    domain: str
    page_type: PageType = PageType.UNKNOWN

    # 内容信息
    content: bytes = b''
    content_type: ContentType = ContentType.HTML
    content_hash: str = field(default_factory=lambda: hashlib.md5().hexdigest())
    content_size: int = 0
    encoding: str = 'utf-8'

    # HTTP信息
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    redirect_history: List[str] = field(default_factory=list)

    # 元数据
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    language: str = 'en'

    # 时间信息
    fetched_at: datetime = field(default_factory=datetime.now)
    modified_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    # 链接信息
    internal_links: List[str] = field(default_factory=list)
    external_links: List[str] = field(default_factory=list)
    extracted_links: List[str] = field(default_factory=list)

    # 解析信息
    metadata: Dict[str, Any] = field(default_factory=dict)
    structured_data: Dict[str, Any] = field(default_factory=dict)
    text_content: Optional[str] = None

    # 性能信息
    download_time: float = 0.0
    processing_time: float = 0.0
    worker_id: Optional[str] = None

    def __post_init__(self):
        """初始化后处理"""
        if not self.content_hash and self.content:
            self.content_hash = hashlib.md5(self.content).hexdigest()
        if not self.content_size and self.content:
            self.content_size = len(self.content)
        if not self.domain and self.url:
            from urllib.parse import urlparse
            self.domain = urlparse(self.url).netloc

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'url': self.url,
            'domain': self.domain,
            'page_type': self.page_type.value,
            'content_type': self.content_type.value,
            'content_hash': self.content_hash,
            'content_size': self.content_size,
            'encoding': self.encoding,
            'status_code': self.status_code,
            'headers': self.headers,
            'cookies': self.cookies,
            'redirect_history': self.redirect_history,
            'title': self.title,
            'description': self.description,
            'keywords': self.keywords,
            'language': self.language,
            'fetched_at': self.fetched_at.isoformat(),
            'modified_at': self.modified_at.isoformat() if self.modified_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'internal_links': self.internal_links,
            'external_links': self.external_links,
            'extracted_links': self.extracted_links,
            'metadata': self.metadata,
            'structured_data': self.structured_data,
            'text_content': self.text_content,
            'download_time': self.download_time,
            'processing_time': self.processing_time,
            'worker_id': self.worker_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Page':
        """从字典创建实例"""
        page = cls(
            id=data.get('id'),
            url=data['url'],
            domain=data.get('domain', ''),
            page_type=PageType(data.get('page_type', 'unknown')),
            content_type=ContentType(data.get('content_type', 'text/html')),
            content_hash=data.get('content_hash'),
            content_size=data.get('content_size', 0),
            encoding=data.get('encoding', 'utf-8'),
            status_code=data.get('status_code', 200),
            headers=data.get('headers', {}),
            cookies=data.get('cookies', {}),
            redirect_history=data.get('redirect_history', []),
            title=data.get('title'),
            description=data.get('description'),
            keywords=data.get('keywords', []),
            language=data.get('language', 'en'),
            internal_links=data.get('internal_links', []),
            external_links=data.get('external_links', []),
            extracted_links=data.get('extracted_links', []),
            metadata=data.get('metadata', {}),
            structured_data=data.get('structured_data', {}),
            text_content=data.get('text_content'),
            download_time=data.get('download_time', 0.0),
            processing_time=data.get('processing_time', 0.0),
            worker_id=data.get('worker_id')
        )

        # 时间字段处理
        if 'fetched_at' in data and data['fetched_at']:
            page.fetched_at = datetime.fromisoformat(data['fetched_at'])
        if 'modified_at' in data and data['modified_at']:
            page.modified_at = datetime.fromisoformat(data['modified_at'])
        if 'expires_at' in data and data['expires_at']:
            page.expires_at = datetime.fromisoformat(data['expires_at'])

        return page

    def is_successful(self) -> bool:
        """检查是否成功获取"""
        return 200 <= self.status_code < 400

    def is_redirect(self) -> bool:
        """检查是否是重定向"""
        return 300 <= self.status_code < 400

    def is_client_error(self) -> bool:
        """检查是否是客户端错误"""
        return 400 <= self.status_code < 500

    def is_server_error(self) -> bool:
        """检查是否是服务器错误"""
        return 500 <= self.status_code < 600

    def get_content_as_text(self) -> str:
        """获取文本内容"""
        if self.text_content:
            return self.text_content
        if self.content and self.content_type in [ContentType.HTML, ContentType.TEXT, ContentType.JSON,
                                                  ContentType.XML]:
            try:
                return self.content.decode(self.encoding)
            except UnicodeDecodeError:
                return self.content.decode('utf-8', errors='ignore')
        return ''

    def add_link(self, link: str, link_type: str = 'extracted'):
        """添加链接"""
        from urllib.parse import urlparse
        current_domain = self.domain

        try:
            link_domain = urlparse(link).netloc
            if link_domain == current_domain:
                if link not in self.internal_links:
                    self.internal_links.append(link)
            else:
                if link not in self.external_links:
                    self.external_links.append(link)

            if link not in self.extracted_links:
                self.extracted_links.append(link)

        except:
            pass

    def calculate_content_hash(self) -> str:
        """计算内容哈希"""
        if self.content:
            self.content_hash = hashlib.md5(self.content).hexdigest()
        return self.content_hash