from .settings import config


class DigiKeyConfig:
    # DigiKey特定配置
    BASE_URL = "https://www.digikey.cn"
    SEARCH_URL = f"{BASE_URL}/products/cn"
    PRODUCT_URL_PATTERN = r"https://www\.digikey\.cn/products/cn/.+"

    # 请求头
    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    # 解析选择器
    SELECTORS = {
        'product_name': 'h1.product-name',
        'price': '.price-break-container',
        'stock': '.stock-status',
        'spec_table': 'table.specs-table',
        'product_images': '.product-image img',
        'breadcrumb': '.breadcrumb',
        'description': '.product-description',
    }


digikey_config = DigiKeyConfig()