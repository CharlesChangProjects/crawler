import re
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from ..base_parser import BaseParser
from config.digikey_config import digikey_config


class DigiKeyParser(BaseParser):
    def __init__(self, html: str, url: str = None):
        super().__init__(html, url)
        self.soup = BeautifulSoup(html, 'lxml')
        self.config = digikey_config

    def parse(self) -> Dict[str, Any]:
        """解析DigiKey产品页面"""
        try:
            return {
                'product_info': self.extract_product_info(),
                'pricing': self.extract_pricing(),
                'inventory': self.extract_inventory(),
                'specifications': self.extract_specifications(),
                'description': self.extract_description(),
                'images': self.extract_images(),
                'breadcrumb': self.extract_breadcrumb(),
                'metadata': self.extract_metadata(),
                'url': self.url
            }
        except Exception as e:
            return {
                'error': f'DigiKey parse error: {str(e)}',
                'url': self.url
            }

    def extract_product_info(self) -> Dict[str, str]:
        """提取产品基本信息"""
        info = {}

        # 产品名称
        name_elem = self.soup.select_one(self.config.SELECTORS['product_name'])
        if name_elem:
            info['name'] = name_elem.get_text().strip()

        # 产品型号
        model_elem = self.soup.select_one('.product-details h2')
        if model_elem:
            info['model'] = model_elem.get_text().strip()

        # 制造商
        manufacturer_elem = self.soup.select_one('.manufacturer')
        if manufacturer_elem:
            info['manufacturer'] = manufacturer_elem.get_text().strip()

        # 产品编号
        product_number_elem = self.soup.select_one('.product-number')
        if product_number_elem:
            info['product_number'] = product_number_elem.get_text().strip()

        return info

    def extract_pricing(self) -> List[Dict[str, Any]]:
        """提取价格信息"""
        pricing = []

        # 查找价格表
        price_table = self.soup.select_one('.pricing-table')
        if price_table:
            for row in price_table.select('tr'):
                cells = row.select('td')
                if len(cells) >= 2:
                    try:
                        quantity = cells[0].get_text().strip()
                        price = cells[1].get_text().strip()
                        pricing.append({
                            'quantity': quantity,
                            'price': price
                        })
                    except:
                        continue

        return pricing

    def extract_inventory(self) -> Dict[str, Any]:
        """提取库存信息"""
        inventory = {}

        stock_elem = self.soup.select_one(self.config.SELECTORS['stock'])
        if stock_elem:
            stock_text = stock_elem.get_text().strip()
            inventory['status'] = stock_text

            # 解析库存数量
            match = re.search(r'(\d+,?\d*)', stock_text)
            if match:
                inventory['quantity'] = int(match.group(1).replace(',', ''))

        return inventory

    def extract_specifications(self) -> Dict[str, str]:
        """提取技术规格"""
        specs = {}

        spec_table = self.soup.select_one(self.config.SELECTORS['spec_table'])
        if spec_table:
            for row in spec_table.select('tr'):
                cells = row.select('td')
                if len(cells) >= 2:
                    key = cells[0].get_text().strip().rstrip(':')
                    value = cells[1].get_text().strip()
                    specs[key] = value

        return specs

    def extract_description(self) -> str:
        """提取产品描述"""
        desc_elem = self.soup.select_one(self.config.SELECTORS['description'])
        return desc_elem.get_text().strip() if desc_elem else ''

    def extract_images(self) -> List[str]:
        """提取产品图片"""
        images = []

        for img in self.soup.select(self.config.SELECTORS['product_images']):
            src = img.get('src', '')
            if src.startswith('http'):
                images.append(src)
            elif src.startswith('/'):
                images.append(f"{self.config.BASE_URL}{src}")

        return images

    def extract_breadcrumb(self) -> List[str]:
        """提取面包屑导航"""
        breadcrumb = []

        breadcrumb_elem = self.soup.select_one(self.config.SELECTORS['breadcrumb'])
        if breadcrumb_elem:
            for item in breadcrumb_elem.select('a'):
                breadcrumb.append(item.get_text().strip())

        return breadcrumb

    def extract_metadata(self) -> Dict[str, str]:
        """提取页面元数据"""
        metadata = {}

        # 标准的meta标签
        for meta in self.soup.select('meta[name], meta[property]'):
            name = meta.get('name') or meta.get('property')
            content = meta.get('content', '')
            if name and content:
                metadata[name] = content

        return metadata

    def extract_links(self) -> List[str]:
        """提取页面链接"""
        links = set()

        for a in self.soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('http'):
                links.add(href)
            elif href.startswith('/'):
                links.add(f"{self.config.BASE_URL}{href}")
            elif href.startswith('#'):
                continue
            else:
                links.add(f"{self.config.BASE_URL}/{href}")

        return list(links)

    def is_product_page(self) -> bool:
        """判断是否为产品详情页"""
        return bool(self.soup.select_one(self.config.SELECTORS['product_name']))

    def is_category_page(self) -> bool:
        """判断是否为分类页面"""
        return bool(self.soup.select_one('.category-products'))

    def is_search_page(self) -> bool:
        """判断是否为搜索页面"""
        return bool(self.soup.select_one('.search-results'))