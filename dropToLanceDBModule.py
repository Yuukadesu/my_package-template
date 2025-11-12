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

DB_PATH = "data/sample-lancedb"
_db = lancedb.connect(DB_PATH)

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

    def init(self, context: FSContext):
        self.context = context
        self._consumer = getattr(context, 'consumer', None)

    async def _get_or_create_stream(self, topic_name: str, consumer: pulsar.Consumer) -> '_Stream':
        if topic_name not in self.topic_to_stream:
            async with self._lock:
                if topic_name not in self.topic_to_stream:
                    self.topic_to_stream[topic_name] = _Stream(topic_name, consumer, self.config)
        return self.topic_to_stream[topic_name]

    async def process(self, context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(data, pulsar.Message):
            msg = data
        else:
            msg = data.get('_message') or data.get('message')
        
        if not msg or not isinstance(msg, pulsar.Message):
            raise ValueError("data must contain a pulsar.Message object")

        topic_name = msg.topic_name()
        if not topic_name:
            raise ValueError("Message must have a valid topic_name")

        consumer = context.consumer or self._consumer
        if not consumer:
            raise ValueError("consumer is required in context")

        stream = await self._get_or_create_stream(topic_name, consumer)

        await stream.send(msg)

        return {"status": "processed", "topic": topic_name}

    async def close(self):
        """Close the module and clean up all streams"""
        streams = list(self.topic_to_stream.values())
        for s in streams:
            await s.close()


class _Stream:

    def __init__(self, topic_name: str, consumer: pulsar.Consumer, config: StreamConfig):
        self.topic_name = topic_name
        self.table_name = topic_to_table_name(topic_name)
        self.consumer = consumer
        self.config = config
        self.lock = asyncio.Lock()
        self.pending_msgs: List[pulsar.Message] = []
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

    async def send(self, msg: pulsar.Message):
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

        async with self.lock:
            self.pending_msgs.append(msg)
            self.pending_docs.append(doc)
            self.pending_bytes += data_size
            should_flush = (
                    self.pending_bytes >= self.config.flush_max_payload_bytes or
                    len(self.pending_msgs) >= self.config.flush_max_msg_count
            )

        if should_flush:
            await self.flush()

    def _get_or_create_table(self, docs: List[Dict[str, Any]]):
        if self._table is not None:
            return self._table

        table_name = self.table_name
        table_exists = table_name in _db.table_names()
        
        if table_exists:
            self._table = _db.open_table(table_name)
            logger.info(f"[{table_name}] table is already exists.")
            return self._table
        else:
            # 创建表时传入所有数据，因为 create_table 会写入这些数据
            # 返回一个标记，表示表是新创建的，flush 时不需要再次 add
            if docs and len(docs) > 0:
                self._table = _db.create_table(table_name, docs)
                logger.info(f"[{table_name}] table had been created，include {len(docs)} data")
                self._table_created_with_data = True  # 标记表是用数据创建的
                return self._table
            else:
                raise ValueError(f"can not create table [{table_name}] without data")
    
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
        
        # 检查表是否已存在
        table_existed_before = self.table_name in _db.table_names()
        table = self._get_or_create_table(docs)
        
        # 如果表是新创建的，数据已经在 create_table 时写入了，不需要再次 add
        # 如果表已存在，需要通过 add 追加数据
        need_add = table_existed_before
        
        loop = asyncio.get_running_loop()
        try:
            if need_add:
                await loop.run_in_executor(None, table.add, docs)
            else:
                # 表是新创建的，数据已经在 create_table 时写入，重置标记
                self._table_created_with_data = False

            ack_failed_count = 0
            for msg in msgs:
                try:
                    self.consumer.acknowledge(msg)
                except Exception as e:
                    ack_failed_count += 1
                    logger.warning(f"[{self.table_name}] ack failed: {e}, msg_id: {msg.message_id()}")

            if ack_failed_count > 0:
                logger.error(f"[{self.table_name}] write {msg_count} messages，but {ack_failed_count} messages ack failed，May lead to duplicate processing")
            else:
                logger.info(f"[{self.table_name}] write {msg_count} messages，already ack")
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
        logger.info(f"[{self.table_name}] Stream was closed.")

def _calculate_doc_size(doc: Dict[str, Any]) -> int:
    return len(json.dumps(doc).encode('utf-8'))


def get_db():
    return _db


def topic_to_table_name(topic_name: str) -> str:
    return topic_name.rsplit('/', 1)[-1] if topic_name else "default_table"


def create_or_open_table(topic_name: str, data: Optional[List[Dict[str, Any]]] = None):
    table_name = topic_to_table_name(topic_name)
    if table_name not in _db.table_names():
        if data is None or len(data) == 0:
            raise ValueError(f"can not create table [{table_name}]")
        return _db.create_table(table_name, data)
    return _db.open_table(table_name)

