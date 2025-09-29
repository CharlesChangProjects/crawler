from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List
from decimal import Decimal


class ProductCategory(Enum):
    """产品类别枚举"""
    ELECTRONIC_COMPONENT = "electronic_component"
    SEMICONDUCTOR = "semiconductor"
    CONNECTOR = "connector"
    PASSIVE_COMPONENT = "passive_component"
    POWER_SUPPLY = "power_supply"
    TOOL = "tool"
    TEST_EQUIPMENT = "test_equipment"
    OTHER = "other"


class PriceTier:
    """价格层级"""

    def __init__(self, quantity: int, price: Decimal, currency: str = "USD"):
        self.quantity = quantity
        self.price = price
        self.currency = currency

    def to_dict(self) -> Dict[str, Any]:
        return {
            'quantity': self.quantity,
            'price': float(self.price),
            'currency': self.currency
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PriceTier':
        return cls(
            quantity=data['quantity'],
            price=Decimal(str(data['price'])),
            currency=data.get('currency', 'USD')
        )


@dataclass
class Product:
    """产品数据模型（针对DigiKey等电商网站）"""

    # 基础信息
    id: str
    sku: str
    manufacturer: str
    manufacturer_part_number: str
    description: str
    category: ProductCategory = ProductCategory.OTHER

    # 价格信息
    price_tiers: List[PriceTier] = field(default_factory=list)
    currency: str = "USD"
    unit_price: Optional[Decimal] = None
    min_order_quantity: int = 1

    # 库存信息
    stock_quantity: int = 0
    stock_status: str = "In Stock"
    lead_time: Optional[str] = None
    is_rohs_compliant: bool = False
    is_active: bool = True

    # 技术规格
    specifications: Dict[str, str] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    features: List[str] = field(default_factory=list)
    applications: List[str] = field(default_factory=list)

    # 包装信息
    package_type: Optional[str] = None
    package_quantity: int = 1
    weight: Optional[float] = None
    dimensions: Optional[Dict[str, float]] = None

    # 文档和资源
    datasheet_url: Optional[str] = None
    image_urls: List[str] = field(default_factory=list)
    catalog_url: Optional[str] = None
    video_urls: List[str] = field(default_factory=list)

    # 来源信息
    source_url: str
    source_domain: str
    crawled_at: datetime = field(default_factory=datetime.now)

    # 分类信息
    digikey_category: Optional[str] = None
    digikey_subcategory: Optional[str] = None
    digikey_family: Optional[str] = None

    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        """初始化后处理"""
        if not self.source_domain and self.source_url:
            from urllib.parse import urlparse
            self.source_domain = urlparse(self.source_url).netloc

        # 设置单位价格（从价格层级中获取）
        if not self.unit_price and self.price_tiers:
            self.unit_price = self.price_tiers[0].price

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'sku': self.sku,
            'manufacturer': self.manufacturer,
            'manufacturer_part_number': self.manufacturer_part_number,
            'description': self.description,
            'category': self.category.value,
            'price_tiers': [tier.to_dict() for tier in self.price_tiers],
            'currency': self.currency,
            'unit_price': float(self.unit_price) if self.unit_price else None,
            'min_order_quantity': self.min_order_quantity,
            'stock_quantity': self.stock_quantity,
            'stock_status': self.stock_status,
            'lead_time': self.lead_time,
            'is_rohs_compliant': self.is_rohs_compliant,
            'is_active': self.is_active,
            'specifications': self.specifications,
            'parameters': self.parameters,
            'features': self.features,
            'applications': self.applications,
            'package_type': self.package_type,
            'package_quantity': self.package_quantity,
            'weight': self.weight,
            'dimensions': self.dimensions,
            'datasheet_url': self.datasheet_url,
            'image_urls': self.image_urls,
            'catalog_url': self.catalog_url,
            'video_urls': self.video_urls,
            'source_url': self.source_url,
            'source_domain': self.source_domain,
            'crawled_at': self.crawled_at.isoformat(),
            'digikey_category': self.digikey_category,
            'digikey_subcategory': self.digikey_subcategory,
            'digikey_family': self.digikey_family,
            'metadata': self.metadata,
            'tags': self.tags
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Product':
        """从字典创建实例"""
        price_tiers = [PriceTier.from_dict(tier) for tier in data.get('price_tiers', [])]
        unit_price = Decimal(str(data['unit_price'])) if data.get('unit_price') else None

        product = cls(
            id=data['id'],
            sku=data['sku'],
            manufacturer=data['manufacturer'],
            manufacturer_part_number=data['manufacturer_part_number'],
            description=data['description'],
            category=ProductCategory(data.get('category', 'other')),
            price_tiers=price_tiers,
            currency=data.get('currency', 'USD'),
            unit_price=unit_price,
            min_order_quantity=data.get('min_order_quantity', 1),
            stock_quantity=data.get('stock_quantity', 0),
            stock_status=data.get('stock_status', 'In Stock'),
            lead_time=data.get('lead_time'),
            is_rohs_compliant=data.get('is_rohs_compliant', False),
            is_active=data.get('is_active', True),
            specifications=data.get('specifications', {}),
            parameters=data.get('parameters', {}),
            features=data.get('features', []),
            applications=data.get('applications', []),
            package_type=data.get('package_type'),
            package_quantity=data.get('package_quantity', 1),
            weight=data.get('weight'),
            dimensions=data.get('dimensions', {}),
            datasheet_url=data.get('datasheet_url'),
            image_urls=data.get('image_urls', []),
            catalog_url=data.get('catalog_url'),
            video_urls=data.get('video_urls', []),
            source_url=data['source_url'],
            source_domain=data.get('source_domain', ''),
            metadata=data.get('metadata', {}),
            tags=data.get('tags', [])
        )

        # 时间字段处理
        if 'crawled_at' in data and data['crawled_at']:
            product.crawled_at = datetime.fromisoformat(data['crawled_at'])

        # DigiKey特定字段
        product.digikey_category = data.get('digikey_category')
        product.digikey_subcategory = data.get('digikey_subcategory')
        product.digikey_family = data.get('digikey_family')

        return product

    def add_price_tier(self, quantity: int, price: Decimal, currency: str = "USD"):
        """添加价格层级"""
        tier = PriceTier(quantity, price, currency)
        self.price_tiers.append(tier)
        # 按数量排序
        self.price_tiers.sort(key=lambda x: x.quantity)

        # 更新单位价格
        if not self.unit_price or quantity == 1:
            self.unit_price = price

    def get_price_for_quantity(self, quantity: int) -> Optional[Decimal]:
        """获取指定数量的价格"""
        for tier in sorted(self.price_tiers, key=lambda x: x.quantity, reverse=True):
            if quantity >= tier.quantity:
                return tier.price
        return None

    def is_in_stock(self) -> bool:
        """检查是否有库存"""
        return self.stock_quantity > 0 and self.stock_status.lower() in [
            'in stock', 'available', 'stocking'
        ]

    def add_specification(self, name: str, value: str):
        """添加技术规格"""
        self.specifications[name] = value

    def add_parameter(self, name: str, value: Any):
        """添加参数"""
        self.parameters[name] = value

    def add_feature(self, feature: str):
        """添加特性"""
        if feature not in self.features:
            self.features.append(feature)

    def add_application(self, application: str):
        """添加应用领域"""
        if application not in self.applications:
            self.applications.append(application)

    def add_image_url(self, image_url: str):
        """添加图片URL"""
        if image_url not in self.image_urls:
            self.image_urls.append(image_url)