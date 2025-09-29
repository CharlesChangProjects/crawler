from prometheus_client import Counter, Gauge, Histogram, start_http_server
import asyncio


class MetricsCollector:
    """指标收集器 - 用于监控和性能统计"""

    def __init__(self, prometheus_port: int = 8000):
        self.prometheus_port = prometheus_port
        self.metrics = {}
        self._setup_default_metrics()

    def _setup_default_metrics(self):
        """设置默认指标"""
        # 请求相关指标
        self.metrics['requests_total'] = Counter(
            'crawler_requests_total',
            'Total number of requests',
            ['method', 'status', 'domain']
        )

        self.metrics['requests_duration'] = Histogram(
            'crawler_requests_duration_seconds',
            'Request duration in seconds',
            ['domain']
        )

        self.metrics['requests_in_progress'] = Gauge(
            'crawler_requests_in_progress',
            'Number of requests in progress',
            ['domain']
        )

        # 任务相关指标
        self.metrics['tasks_total'] = Counter(
            'crawler_tasks_total',
            'Total number of tasks processed',
            ['status', 'worker']
        )

        self.metrics['tasks_duration'] = Histogram(
            'crawler_tasks_duration_seconds',
            'Task processing duration in seconds',
            ['worker']
        )

        # 系统相关指标
        self.metrics['queue_size'] = Gauge(
            'crawler_queue_size',
            'Number of items in the queue'
        )

        self.metrics['workers_total'] = Gauge(
            'crawler_workers_total',
            'Total number of workers'
        )

        self.metrics['memory_usage'] = Gauge(
            'crawler_memory_usage_bytes',
            'Memory usage in bytes'
        )

    def start_prometheus_server(self):
        """启动Prometheus指标服务器"""
        try:
            start_http_server(self.prometheus_port)
            print(f"Prometheus metrics server started on port {self.prometheus_port}")
        except Exception as e:
            print(f"Failed to start Prometheus server: {e}")

    def record_request(self, method: str, status: int, domain: str, duration: float):
        """记录请求指标"""
        self.metrics['requests_total'].labels(method=method, status=status, domain=domain).inc()
        self.metrics['requests_duration'].labels(domain=domain).observe(duration)

    def record_task(self, status: str, worker: str, duration: float):
        """记录任务指标"""
        self.metrics['tasks_total'].labels(status=status, worker=worker).inc()
        if duration > 0:
            self.metrics['tasks_duration'].labels(worker=worker).observe(duration)

    def set_queue_size(self, size: int):
        """设置队列大小指标"""
        self.metrics['queue_size'].set(size)

    def set_workers_count(self, count: int):
        """设置工作节点数量指标"""
        self.metrics['workers_total'].set(count)

    def set_memory_usage(self, usage: int):
        """设置内存使用指标"""
        self.metrics['memory_usage'].set(usage)

    def inc_requests_in_progress(self, domain: str):
        """增加进行中的请求计数"""
        self.metrics['requests_in_progress'].labels(domain=domain).inc()

    def dec_requests_in_progress(self, domain: str):
        """减少进行中的请求计数"""
        self.metrics['requests_in_progress'].labels(domain=domain).dec()

    async def collect_system_metrics(self, interval: int = 30):
        """定期收集系统指标"""
        import psutil
        process = psutil.Process()

        while True:
            try:
                # 收集内存使用情况
                memory_info = process.memory_info()
                self.set_memory_usage(memory_info.rss)

                # 可以添加更多系统指标
                await asyncio.sleep(interval)
            except Exception as e:
                print(f"Error collecting system metrics: {e}")
                await asyncio.sleep(5)


def setup_metrics(prometheus_port: int = 8000) -> MetricsCollector:
    """设置指标收集器"""
    collector = MetricsCollector(prometheus_port)
    collector.start_prometheus_server()
    return collector