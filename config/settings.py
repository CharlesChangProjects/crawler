import os
from dataclasses import dataclass
from typing import Tuple, Optional

@dataclass
class RedisConfig:
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", 6379))
    db: int = int(os.getenv("REDIS_DB", 0))
    password: Optional[str] = os.getenv("REDIS_PASSWORD")
    task_queue: str = "crawler:tasks"
    result_queue: str = "crawler:results"
    bloomfilter_key: str = "crawler:bloomfilter"
    stats_key: str = "crawler:stats"

@dataclass
class DownloadConfig:
    max_concurrent: int = 100
    request_timeout: int = 30
    retry_times: int = 3
    delay_range: tuple = (0.5, 1.5)
    user_agent_rotation: bool = True
    proxy_enabled: bool = False
    max_redirects: int = 5

@dataclass
class StorageConfig:
    type: str = os.getenv("STORAGE_TYPE", "file")  # file, mongodb, mysql, elasticsearch
    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    mongodb_db: str = os.getenv("MONGODB_DB", "crawler")
    mysql_uri: str = os.getenv("MYSQL_URI", "mysql+pymysql://user:pass@localhost:3306/crawler")
    file_path: str = os.getenv("FILE_PATH", "./data")
    elasticsearch_hosts: Tuple[str] = ("localhost:9200",)

@dataclass
class LogConfig:
    level: str = os.getenv("LOG_LEVEL", "INFO")
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: Optional[str] = os.getenv("LOG_FILE")

@dataclass
class GlobalConfig:
    redis: RedisConfig = RedisConfig()
    download: DownloadConfig = DownloadConfig()
    storage: StorageConfig = StorageConfig()
    log: LogConfig = LogConfig()
    worker_id: str = os.getenv("WORKER_ID", "worker-1")
    master_host: str = os.getenv("MASTER_HOST", "localhost")
    master_port: int = int(os.getenv("MASTER_PORT", 8000))

config = GlobalConfig()