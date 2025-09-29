from fake_useragent import UserAgent
import random

class UserAgentRotator:
    def __init__(self):
        self.ua = UserAgent()
        self.custom_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15',
        ]

    def get_random_ua(self) -> str:
        """获取随机User-Agent"""
        return random.choice([
            self.ua.random,
            *self.custom_agents
        ])