import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, List

import lancedb
import pulsar
from function_stream.module import FSModule
from function_stream.context import FSContext

logger = logging.getLogger(__name__)


class ThroughputMetrics:
    """吞吐量指标统计类，用于跟踪每秒处理的流量（MB）"""
    
    def __init__(self):
        self.total_bytes_processed = 0
        self.total_messages_processed = 0
        self.start_time = time.time()
        self.last_report_time = time.time()
        self.lock = asyncio.Lock()
        self._report_task: Optional[asyncio.Task] = None
        self._report_interval = 10  # 每10秒报告一次
    
    async def record_processed(self, bytes_count: int, message_count: int = 1):
        """记录已处理的数据量（异步安全）"""
        # 使用异步锁确保线程安全
        async with self.lock:
            self.total_bytes_processed += bytes_count
            self.total_messages_processed += message_count
    
    async def get_current_throughput_mbps(self) -> float:
        """获取当前的吞吐量（MB/s）（异步）"""
        async with self.lock:
            total_bytes = self.total_bytes_processed
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            return 0.0
        total_mb = total_bytes / (1024 * 1024)
        return total_mb / elapsed
    
    async def get_instantaneous_throughput_mbps(self, window_seconds: int = 5) -> float:
        """获取瞬时吞吐量（最近N秒的MB/s）（异步）"""
        # 这个方法需要额外的历史记录，为了简化，我们使用总体吞吐量
        return await self.get_current_throughput_mbps()
    
    async def get_total_processed_mb(self) -> float:
        """获取总处理的数据量（MB）（异步）"""
        async with self.lock:
            total_bytes = self.total_bytes_processed
        return total_bytes / (1024 * 1024)
    
    async def get_total_messages(self) -> int:
        """获取总处理的消息数（异步）"""
        async with self.lock:
            return self.total_messages_processed
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息（异步）"""
        async with self.lock:
            total_bytes = self.total_bytes_processed
            total_messages = self.total_messages_processed
        
        elapsed = time.time() - self.start_time
        total_mb = total_bytes / (1024 * 1024)
        return {
            "total_bytes_processed": total_bytes,
            "total_mb_processed": total_mb,
            "total_messages_processed": total_messages,
            "average_throughput_mbps": total_mb / elapsed if elapsed > 0 else 0.0,
            "elapsed_seconds": elapsed,
            "messages_per_second": total_messages / elapsed if elapsed > 0 else 0
        }
    
    async def _periodic_report(self):
        """定期报告吞吐量指标"""
        while True:
            try:
                await asyncio.sleep(self._report_interval)
                stats = await self.get_stats()
                logger.info(
                    f"[Metrics] Throughput: {stats['average_throughput_mbps']:.2f} MB/s, "
                    f"Total: {stats['total_mb_processed']:.2f} MB, "
                    f"Messages: {stats['total_messages_processed']}, "
                    f"Msg/s: {stats['messages_per_second']:.2f}"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Metrics] Error in periodic report: {e}")
    
    async def start_periodic_reporting(self):
        """启动定期报告任务（异步）"""
        if self._report_task is None or self._report_task.done():
            loop = asyncio.get_running_loop()
            self._report_task = loop.create_task(self._periodic_report())
    
    async def stop_periodic_reporting(self):
        """停止定期报告任务"""
        if self._report_task and not self._report_task.done():
            self._report_task.cancel()
            try:
                await self._report_task
            except asyncio.CancelledError:
                pass


class MessageWrapper:
    """包装类，用于在无法获取原始pulsar.Message时模拟消息对象"""
    
    def __init__(self, data: Dict[str, Any], topic_name: str, message_id: str = None):
        self._data = data  # 保存原始字典数据
        self._topic_name = topic_name
        self._message_id = message_id or f"wrapped-{id(self)}"
        self._bytes_data = json.dumps(data).encode('utf-8')
    
    def data(self):
        # 返回原始字典数据，这样send方法可以直接使用
        return self._data
    
    def topic_name(self) -> str:
        return self._topic_name
    
    def message_id(self) -> str:
        return self._message_id
    
    def properties(self):
        return {}
    
    def __getattr__(self, name):
        # 为了兼容pulsar.Message的其他方法
        return None

DB_PATH = "data/sample-lancedb"
_db = None  # 将在异步环境中初始化

class StreamConfig:
    def __init__(
        self,
        flush_interval_seconds: int = 30,
        flush_max_payload_bytes: int = 100 * 1024 * 1024,  # 100MB
        flush_max_msg_count: int = 10000,
        timer_check_interval: int = 5
    ):

        self.flush_interval_seconds = flush_interval_seconds
        self.flush_max_payload_bytes = flush_max_payload_bytes
        self.flush_max_msg_count = flush_max_msg_count
        self.timer_check_interval = timer_check_interval

# config
DEFAULT_CONFIG = StreamConfig()


class PulsarToLanceDBModule(FSModule):
    """
    FSModule: Import Pulsar messages into LanceDB
    """
    def __init__(self, config: Optional[StreamConfig] = None):
        self._consumer = None
        self.topic_to_stream: Dict[str, '_Stream'] = {}  # map[topic_name, Stream]
        self._lock = asyncio.Lock()
        self.context: Optional[FSContext] = None
        self.config = config or DEFAULT_CONFIG
        self.metrics = ThroughputMetrics()  # 添加吞吐量指标

    async def init(self, context: FSContext):
        """初始化模块（异步）"""
        self.context = context
        self._consumer = getattr(context, 'consumer', None)
        # 启动定期报告吞吐量指标
        await self.metrics.start_periodic_reporting()

    async def _get_or_create_stream(self, topic_name: str, consumer: Optional[pulsar.Consumer]) -> '_Stream':
        if topic_name not in self.topic_to_stream:
            async with self._lock:
                if topic_name not in self.topic_to_stream:
                    self.topic_to_stream[topic_name] = _Stream(topic_name, consumer, self.config, self.metrics)
        return self.topic_to_stream[topic_name]

    async def process(self, context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
        # 优化：快速路径 - 如果data是字典且没有message键，直接创建MessageWrapper
        # 这样可以避免大量的检查和getattr调用
        if isinstance(data, dict) and '_message' not in data and 'message' not in data:
            # 快速路径：直接使用默认topic创建MessageWrapper
            topic_name = "test-lancedb-topic"  # 从config.yaml的默认值
            msg = MessageWrapper(data, topic_name)
        else:
            # 标准路径：尝试获取pulsar.Message
            if isinstance(data, pulsar.Message):
                msg = data
            else:
                msg = data.get('_message') or data.get('message')
            
            # 如果data中没有，尝试从context获取
            if not msg or not isinstance(msg, pulsar.Message):
                # 快速检查最常见的属性
                msg = getattr(context, 'message', None) or getattr(context, '_message', None)
            
            # 如果还是没有，创建MessageWrapper
            if not msg or not isinstance(msg, pulsar.Message):
                topic_name = "test-lancedb-topic"  # 默认topic
                msg = MessageWrapper(data, topic_name)

        topic_name = msg.topic_name()
        if not topic_name:
            raise ValueError("Message must have a valid topic_name")

        consumer = getattr(context, 'consumer', None) or self._consumer
        if not consumer and not isinstance(msg, MessageWrapper):
            raise ValueError("consumer is required in context for pulsar.Message acknowledge")

        stream = await self._get_or_create_stream(topic_name, consumer)
        await stream.send(msg)

        return {"status": "processed", "topic": topic_name}

    async def close(self):
        """Close the module and clean up all streams"""
        # 停止定期报告
        await self.metrics.stop_periodic_reporting()
        
        # 输出最终统计信息
        final_stats = await self.metrics.get_stats()
        logger.info(
            f"[Metrics] Final Stats - "
            f"Total Processed: {final_stats['total_mb_processed']:.2f} MB, "
            f"Average Throughput: {final_stats['average_throughput_mbps']:.2f} MB/s, "
            f"Total Messages: {final_stats['total_messages_processed']}, "
            f"Elapsed Time: {final_stats['elapsed_seconds']:.2f}s"
        )
        
        streams = list(self.topic_to_stream.values())
        for s in streams:
            await s.close()


class _Stream:

    def __init__(self, topic_name: str, consumer: Optional[pulsar.Consumer], config: StreamConfig, metrics: Optional[ThroughputMetrics] = None):
        self.topic_name = topic_name
        self.table_name = topic_to_table_name(topic_name)
        self.consumer = consumer
        self.config = config
        self.metrics = metrics  # 添加metrics引用
        self.lock = asyncio.Lock()
        self.pending_msgs: List = []  # 可以是pulsar.Message或MessageWrapper
        self.pending_docs: List[Dict[str, Any]] = []
        self.pending_bytes = 0
        self.last_flush_ts = time.time()
        self._stop = False
        self._flush_task: Optional[asyncio.Task] = None
        self._timer_started = False
        self._table = None
        self._table_created_with_data = False  # 标记表是否是用数据创建的

    async def _timer_loop(self):
        check_interval = self.config.timer_check_interval
        while not self._stop:
            await asyncio.sleep(check_interval)
            if self._stop:
                break
            try:
                if await self._should_time_flush():
                    await self.flush()
            except Exception as e:
                logger.error(f"[{self.table_name}] Scheduled flush check failed: {e}", exc_info=True)

    async def _should_time_flush(self) -> bool:
        async with self.lock:
            return (self.pending_msgs and
                    (time.time() - self.last_flush_ts) >= self.config.flush_interval_seconds)

    async def send(self, msg):
        if not self._timer_started:
            self._flush_task = asyncio.create_task(self._timer_loop())
            self._timer_started = True

        try:
            msg_data = msg.data()
            if isinstance(msg_data, dict):
                doc = msg_data
            elif isinstance(msg_data, bytes):
                doc = json.loads(msg_data.decode('utf-8'))
            elif isinstance(msg_data, str):
                doc = json.loads(msg_data)
            else:
                doc = json.loads(str(msg_data))
        except Exception as e:
            logger.error(f"JSON parsing failed[{self.table_name}]: {e}, msg_id: {msg.message_id()}")
            return

        data_size = _calculate_doc_size(doc)

        # 立即更新metrics（即使还没flush，也记录pending的数据）
        # 这样可以更准确地反映处理速度
        if self.metrics:
            await self.metrics.record_processed(data_size, 1)

        async with self.lock:
            self.pending_msgs.append(msg)
            self.pending_docs.append(doc)
            self.pending_bytes += data_size
            should_flush = (
                    self.pending_bytes >= self.config.flush_max_payload_bytes or
                    len(self.pending_msgs) >= self.config.flush_max_msg_count
            )

        # 在锁外执行flush，避免阻塞其他消息
        if should_flush:
            await self.flush()

    async def _get_db(self):
        """获取或创建异步数据库连接"""
        # 使用全局的 get_db() 函数，这样可以支持测试中的 patch
        return await get_db()
    
    async def _get_or_create_table(self, docs: List[Dict[str, Any]]):
        if self._table is not None:
            return self._table

        table_name = self.table_name
        db = await self._get_db()
        
        # 双重检查：先检查表是否存在
        table_names = await db.table_names()
        table_exists = table_name in table_names
        
        if table_exists:
            try:
                self._table = await db.open_table(table_name)
                logger.debug(f"[{table_name}] table is already exists.")
                return self._table
            except Exception as e:
                # 如果打开表失败，可能是表被删除了，尝试重新创建
                logger.warning(f"[{table_name}] Failed to open existing table, will try to create: {e}")
                table_exists = False
        
        # 表不存在，需要创建
        if not table_exists:
            if docs and len(docs) > 0:
                try:
                    self._table = await db.create_table(table_name, docs)
                    logger.debug(f"[{table_name}] table had been created，include {len(docs)} data")
                    self._table_created_with_data = True  # 标记表是用数据创建的
                    return self._table
                except Exception as e:
                    # 如果创建失败，可能是并发创建导致的，再次尝试打开
                    logger.warning(f"[{table_name}] Failed to create table, will try to open: {e}")
                    try:
                        # 可能其他线程已经创建了表，尝试打开
                        table_names = await db.table_names()
                        if table_name in table_names:
                            self._table = await db.open_table(table_name)
                            logger.debug(f"[{table_name}] table opened after creation failure")
                            return self._table
                    except Exception as e2:
                        logger.error(f"[{table_name}] Failed to open table after creation failure: {e2}")
                        raise ValueError(f"can not create or open table [{table_name}]: {e}")
            else:
                raise ValueError(f"can not create table [{table_name}] without data")
        
        # 理论上不会到达这里
        raise ValueError(f"Unexpected state for table [{table_name}]")
    
    @property
    def table(self):
        return self._table

    async def flush(self):
        async with self.lock:
            if not self.pending_msgs:
                return
            if len(self.pending_msgs) != len(self.pending_docs):
                logger.error(f"[{self.table_name}] Data asynchronous: msgs={len(self.pending_msgs)}, docs={len(self.pending_docs)}")
                min_len = min(len(self.pending_msgs), len(self.pending_docs))
                msgs = list(self.pending_msgs[:min_len])
                docs = list(self.pending_docs[:min_len])
                self.pending_msgs = self.pending_msgs[min_len:]
                self.pending_docs = self.pending_docs[min_len:]
            else:
                msgs = list(self.pending_msgs)
                docs = list(self.pending_docs)
            msg_count = len(msgs)
            self.pending_msgs = []
            self.pending_docs = []
            self.pending_bytes = 0
            self.last_flush_ts = time.time()

        if not msgs or not docs:
            return
        
        # 获取数据库连接以检查表是否存在
        db = await self._get_db()
        table_names = await db.table_names()
        table_existed_before = self.table_name in table_names
        
        # 获取或创建表（这个方法内部会处理并发情况）
        try:
            table = await self._get_or_create_table(docs)
        except Exception as e:
            logger.error(f"[{self.table_name}] Failed to get or create table: {e}", exc_info=True)
            # 如果表创建失败，将消息重新放回队列
            async with self.lock:
                self.pending_msgs = msgs + self.pending_msgs
                self.pending_docs = docs + self.pending_docs
                self.pending_bytes = sum(_calculate_doc_size(doc) for doc in self.pending_docs)
            return
        
        # 如果表是新创建的，数据已经在 create_table 时写入了，不需要再次 add
        # 如果表已存在，需要通过 add 追加数据
        need_add = table_existed_before
        
        try:
            if need_add:
                # 使用异步API写入
                await table.add(docs)
            else:
                # 表是新创建的，数据已经在 create_table 时写入，重置标记
                self._table_created_with_data = False

            ack_failed_count = 0
            for msg in msgs:
                # 只对真正的pulsar.Message进行acknowledge，MessageWrapper不需要
                if isinstance(msg, pulsar.Message):
                    if self.consumer:
                        try:
                            self.consumer.acknowledge(msg)
                        except Exception as e:
                            ack_failed_count += 1
                            logger.warning(f"[{self.table_name}] ack failed: {e}, msg_id: {msg.message_id()}")
                    else:
                        logger.warning(f"[{self.table_name}] No consumer available to acknowledge message: {msg.message_id()}")
                else:
                    # MessageWrapper不需要acknowledge，因为它是从payload创建的
                    logger.debug(f"[{self.table_name}] Skipping acknowledge for MessageWrapper, msg_id: {msg.message_id()}")

            # 注意：metrics已经在send方法中更新了，这里不需要重复更新
            # 但如果需要，可以在这里再次确认更新（避免重复计算）
            # 由于send方法已经更新了metrics，这里不再重复更新
            
            if ack_failed_count > 0:
                logger.error(f"[{self.table_name}] write {msg_count} messages，but {ack_failed_count} messages ack failed，May lead to duplicate processing")
            else:
                # 只在debug模式下显示详细的flush信息
                if self.metrics:
                    throughput_mbps = await self.metrics.get_current_throughput_mbps()
                else:
                    throughput_mbps = 0
                logger.debug(
                    f"[{self.table_name}] write {msg_count} messages，already ack. "
                    f"Throughput: {throughput_mbps:.2f} MB/s"
                )
        except Exception as e:
            logger.error(f"[{self.table_name}] write failed: {e}", exc_info=True)
            async with self.lock:
                self.pending_msgs = msgs + self.pending_msgs
                self.pending_docs = docs + self.pending_docs

                self.pending_bytes = sum(_calculate_doc_size(doc) for doc in self.pending_docs)

    async def close(self):
        if self._stop:
            return
        
        self._stop = True
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()
        logger.debug(f"[{self.table_name}] Stream was closed.")

def _calculate_doc_size(doc: Dict[str, Any]) -> int:
    return len(json.dumps(doc).encode('utf-8'))


async def get_db():
    """获取异步数据库连接"""
    global _db
    if _db is None:
        _db = await lancedb.connect_async(DB_PATH)
    return _db


def topic_to_table_name(topic_name: str) -> str:
    return topic_name.rsplit('/', 1)[-1] if topic_name else "default_table"


async def create_or_open_table(topic_name: str, data: Optional[List[Dict[str, Any]]] = None):
    """异步创建或打开表"""
    table_name = topic_to_table_name(topic_name)
    db = await get_db()
    table_names = await db.table_names()
    if table_name not in table_names:
        if data is None or len(data) == 0:
            raise ValueError(f"can not create table [{table_name}]")
        return await db.create_table(table_name, data)
    return await db.open_table(table_name)

