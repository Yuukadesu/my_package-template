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

# 全局数据库连接
DB_PATH = "data/sample-lancedb"
_db: Optional[lancedb.AsyncConnection] = None


class MessageWrapper:
    """
    包装类，用于在 function_stream 框架中模拟 pulsar.Message
    function_stream 框架传递的是字典数据，而不是原始的 pulsar.Message
    """
    
    def __init__(self, data: Dict[str, Any], topic_name: str, message_id: str = None):
        self._data = data  # 保存原始字典数据
        self._topic_name = topic_name
        self._message_id = message_id or f"wrapped-{id(self)}"
        self._bytes_data = None  # 延迟计算
    
    def data(self):
        """返回消息数据（字典格式）"""
        return self._data
    
    def topic_name(self) -> str:
        return self._topic_name
    
    def message_id(self) -> str:
        return self._message_id
    
    def properties(self):
        """返回消息属性"""
        return {}
    
    def __getattr__(self, name):
        """为了兼容 pulsar.Message 的其他方法"""
        return None


class ThroughputMetrics:
    """
    吞吐量指标收集类
    异步、无锁设计，最大化性能
    """
    
    def __init__(self, report_interval_seconds: float = 10.0):
        # 计数器（Python GIL 保证原子性，无需锁）
        self._total_bytes_processed = 0
        self._total_messages_processed = 0
        self._start_time = time.time()
        
        # 滑动窗口用于计算瞬时吞吐量
        self._window_seconds = 10.0
        self._window_samples: List[tuple[float, int, int, float]] = []  # [(timestamp, bytes, messages, duration), ...]
        
        # 报告任务
        self._report_task: Optional[asyncio.Task] = None
        self._report_interval = report_interval_seconds
        self._stop_reporting = False
    
    def record_flush(self, bytes_count: int, message_count: int, duration_seconds: float) -> Dict[str, Any]:
        """
        记录一次 flush 操作的数据量和耗时
        用于计算实际写入数据库的吞吐量
        
        Args:
            bytes_count: 本次 flush 写入的数据量（字节）
            message_count: 本次 flush 写入的消息数
            duration_seconds: 本次 flush 的耗时（秒），从开始到写入完成
        
        Returns:
            本次 flush 的指标信息
        """
        current_time = time.time()
        self._total_bytes_processed += bytes_count
        self._total_messages_processed += message_count
        
        # 计算本次 flush 的速率
        if duration_seconds > 0:
            flush_mbps = (bytes_count / (1024 * 1024)) / duration_seconds
            flush_msg_per_sec = message_count / duration_seconds
        else:
            flush_mbps = 0.0
            flush_msg_per_sec = 0.0
        
        # 添加到滑动窗口：记录 (时间戳, 数据量, 消息数, 耗时)
        # 用于计算瞬时吞吐量
        self._window_samples.append((current_time, bytes_count, message_count, duration_seconds))
        
        # 清理过期样本（保持窗口大小）
        cutoff_time = current_time - self._window_seconds
        self._window_samples = [
            sample for sample in self._window_samples
            if sample[0] > cutoff_time
        ]
        
        # 返回本次 flush 的指标
        return {
            "flush_bytes": bytes_count,
            "flush_mb": bytes_count / (1024 * 1024),
            "flush_messages": message_count,
            "flush_duration": duration_seconds,
            "flush_mbps": flush_mbps,
            "flush_msg_per_sec": flush_msg_per_sec
        }
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        current_time = time.time()
        elapsed = current_time - self._start_time
        
        # 计算平均吞吐量
        total_mb = self._total_bytes_processed / (1024 * 1024)
        avg_throughput_mbps = total_mb / elapsed if elapsed > 0 else 0.0
        avg_msg_per_sec = self._total_messages_processed / elapsed if elapsed > 0 else 0.0
        
        # 计算瞬时吞吐量（滑动窗口）
        # 基于每次 flush 的实际耗时和数据量计算
        if self._window_samples:
            # 计算窗口内所有 flush 的总数据量和总耗时
            window_bytes = sum(sample[1] for sample in self._window_samples)
            window_messages = sum(sample[2] for sample in self._window_samples)
            window_duration = sum(sample[3] for sample in self._window_samples)  # 所有 flush 的总耗时
            
            # 计算窗口的时间跨度（最早到最晚的时间差）
            window_start = min(sample[0] for sample in self._window_samples)
            window_elapsed = current_time - window_start
            
            if window_duration > 0 and window_elapsed > 0:
                # 瞬时吞吐量 = 窗口内总数据量 / 窗口内总耗时（反映实际写入速率）
                instantaneous_mbps = (window_bytes / (1024 * 1024)) / window_duration
                # 瞬时消息速率 = 窗口内总消息数 / 窗口时间跨度（反映处理频率）
                instantaneous_msg_per_sec = window_messages / window_elapsed
            else:
                instantaneous_mbps = 0.0
                instantaneous_msg_per_sec = 0.0
        else:
            instantaneous_mbps = 0.0
            instantaneous_msg_per_sec = 0.0
        
        return {
            "total_bytes_processed": self._total_bytes_processed,
            "total_mb_processed": total_mb,
            "total_messages_processed": self._total_messages_processed,
            "average_throughput_mbps": avg_throughput_mbps,
            "instantaneous_throughput_mbps": instantaneous_mbps,
            "average_messages_per_second": avg_msg_per_sec,
            "instantaneous_messages_per_second": instantaneous_msg_per_sec,
            "elapsed_seconds": elapsed
        }
    
    async def _periodic_report(self):
        """定期报告吞吐量指标"""
        while not self._stop_reporting:
            try:
                await asyncio.sleep(self._report_interval)
                if self._stop_reporting:
                    break
                
                stats = await self.get_stats()
                logger.info(
                    f"[Throughput] Instantaneous: {stats['instantaneous_throughput_mbps']:.2f} MB/s, "
                    f"Average: {stats['average_throughput_mbps']:.2f} MB/s, "
                    f"Total: {stats['total_mb_processed']:.2f} MB, "
                    f"Messages: {stats['total_messages_processed']}, "
                    f"Msg/s: {stats['instantaneous_messages_per_second']:.2f} (instant), "
                    f"{stats['average_messages_per_second']:.2f} (avg)"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Throughput] Error in periodic report: {e}")
    
    async def start_reporting(self):
        """启动定期报告任务"""
        if self._report_task is None or self._report_task.done():
            self._stop_reporting = False
            self._report_task = asyncio.create_task(self._periodic_report())
    
    async def stop_reporting(self):
        """停止定期报告任务"""
        self._stop_reporting = True
        if self._report_task and not self._report_task.done():
            self._report_task.cancel()
            try:
                await self._report_task
            except asyncio.CancelledError:
                pass


class StreamConfig:
    """Stream 配置类，定义攒批参数"""
    
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


class Stream:
    """
    Stream: 处理单个 topic 的攒批 (topic->table)
    一个 Stream 对应一个 pulsar topic，同时对应一个 lance db table
    """
    
    def __init__(
        self,
        topic_name: str,
        consumer: Optional[pulsar.Consumer],
        config: StreamConfig,
        metrics: Optional[ThroughputMetrics] = None
    ):
        self.topic_name = topic_name
        self.table_name = self._topic_to_table_name(topic_name)
        self.consumer = consumer
        self.config = config
        self.metrics = metrics  # 吞吐量指标收集器
        
        # 攒批队列（可以是 pulsar.Message 或 MessageWrapper）
        self._queue: List[Any] = []
        self._queue_bytes = 0
        self._last_flush_time = time.time()
        
        # 状态控制
        self._stop = False
        self._timer_task: Optional[asyncio.Task] = None
        self._table: Optional[Any] = None
        self._table_created_with_data = False  # 标记表是否是用数据创建的
        self._table_create_duration = 0.0  # 表创建耗时（用于性能分析）
        self._schema_cache: Optional[set] = None  # 缓存 schema 字段名，避免重复获取
        
        # 记录配置信息（用于调试）
        logger.info(
            f"[Stream] [{self.table_name}] Initialized with config: "
            f"flush_interval={config.flush_interval_seconds}s, "
            f"flush_max_payload={config.flush_max_payload_bytes/(1024*1024):.1f}MB, "
            f"flush_max_msg_count={config.flush_max_msg_count}, "
            f"timer_check_interval={config.timer_check_interval}s"
        )
        
    @staticmethod
    def _topic_to_table_name(topic_name: str) -> str:
        """将 topic 名称转换为 table 名称（同名）"""
        return topic_name.rsplit('/', 1)[-1] if topic_name else "default_table"
    
    @staticmethod
    def _calculate_msg_size(msg) -> int:
        """计算消息大小（字节）"""
        try:
            data = msg.data()
            if isinstance(data, bytes):
                return len(data)
            elif isinstance(data, str):
                return len(data.encode('utf-8'))
            elif isinstance(data, dict):
                return len(json.dumps(data).encode('utf-8'))
            else:
                return len(str(data).encode('utf-8'))
        except Exception:
            return 0
    
    def _parse_message_data(self, msg) -> Optional[Dict[str, Any]]:
        """解析消息数据为 JSON 字典"""
        try:
            data = msg.data()
            if isinstance(data, dict):
                return data
            elif isinstance(data, bytes):
                return json.loads(data.decode('utf-8'))
            elif isinstance(data, str):
                return json.loads(data)
            else:
                return json.loads(str(data))
        except Exception as e:
            logger.error(
                f"[{self.table_name}] JSON parsing failed: {e}, "
                f"msg_id: {msg.message_id()}"
            )
            return None
    
    async def _timer_loop(self):
        """定时器循环，定期检查是否需要 flush"""
        while not self._stop:
            try:
                await asyncio.sleep(self.config.timer_check_interval)
                if self._stop:
                    break
                
                # 检查是否达到时间限制
                if self._queue and (time.time() - self._last_flush_time) >= self.config.flush_interval_seconds:
                    elapsed = time.time() - self._last_flush_time
                    logger.debug(
                        f"[{self.table_name}] Flush triggered by timer: "
                        f"elapsed={elapsed:.2f}s>={self.config.flush_interval_seconds}s, "
                        f"queue_size={len(self._queue)} msgs, "
                        f"queue_bytes={self._queue_bytes/(1024*1024):.2f}MB"
                    )
                    await self._flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.table_name}] Timer loop error: {e}", exc_info=True)
    
    async def send(self, msg):
        """
        发送消息到攒批队列
        如果达到 size 或 count 限制，立即 flush
        """
        # 启动定时器（如果还没启动）
        if self._timer_task is None or self._timer_task.done():
            self._timer_task = asyncio.create_task(self._timer_loop())
        
        # 解析消息数据
        doc = self._parse_message_data(msg)
        if doc is None:
            # 解析失败，直接 ack 消息（避免重复处理）
            # 只对真正的 pulsar.Message 进行 ack
            if isinstance(msg, pulsar.Message) and self.consumer:
                try:
                    self.consumer.acknowledge(msg)
                except Exception as e:
                    logger.warning(f"[{self.table_name}] Failed to ack invalid message: {e}")
            return
        
        # 添加到队列
        msg_size = self._calculate_msg_size(msg)
        self._queue.append(msg)
        self._queue_bytes += msg_size
        
        # 注意：指标在 flush 成功后记录，反映实际写入数据库的速率
        
        # 检查是否需要立即 flush
        should_flush = (
            self._queue_bytes >= self.config.flush_max_payload_bytes or
            len(self._queue) >= self.config.flush_max_msg_count
        )
        
        if should_flush:
            # 记录 flush 触发原因（用于调试）
            trigger_reason = []
            if self._queue_bytes >= self.config.flush_max_payload_bytes:
                trigger_reason.append(f"size={self._queue_bytes/(1024*1024):.2f}MB>={self.config.flush_max_payload_bytes/(1024*1024):.1f}MB")
            if len(self._queue) >= self.config.flush_max_msg_count:
                trigger_reason.append(f"count={len(self._queue)}>={self.config.flush_max_msg_count}")
            logger.debug(f"[{self.table_name}] Flush triggered by: {', '.join(trigger_reason)}")
            await self._flush()
    
    async def _get_db(self) -> lancedb.AsyncConnection:
        """获取或创建数据库连接"""
        global _db
        if _db is None:
            _db = await lancedb.connect_async(DB_PATH)
        return _db
    
    async def _get_or_create_table(self, docs: List[Dict[str, Any]]) -> Any:
        """获取或创建表"""
        if self._table is not None:
            return self._table
        
        db = await self._get_db()
        table_names = await db.table_names()
        
        # 检查表是否存在
        if self.table_name in table_names:
            try:
                self._table = await db.open_table(self.table_name)
                self._table_created_with_data = False
                logger.debug(f"[{self.table_name}] Table opened")
                return self._table
            except Exception as e:
                logger.warning(f"[{self.table_name}] Failed to open table: {e}, will create new")
        
        # 表不存在，创建新表
        if not docs:
            raise ValueError(f"Cannot create table [{self.table_name}] without data")
        
        try:
            # 创建表（包含数据写入）
            create_start = time.time()
            self._table = await db.create_table(self.table_name, docs)
            create_duration = time.time() - create_start
            self._table_created_with_data = True  # 标记表是用数据创建的
            self._table_create_duration = create_duration  # 保存创建耗时用于性能分析
            logger.debug(f"[{self.table_name}] Table created with {len(docs)} records in {create_duration:.3f}s")
            return self._table
        except Exception as e:
            # 可能并发创建导致失败，尝试再次打开
            logger.warning(f"[{self.table_name}] Failed to create table: {e}, will try to open")
            table_names = await db.table_names()
            if self.table_name in table_names:
                self._table = await db.open_table(self.table_name)
                self._table_created_with_data = False
                logger.debug(f"[{self.table_name}] Table opened after creation failure")
                return self._table
            raise ValueError(f"Cannot create or open table [{self.table_name}]: {e}")
    
    async def _flush(self):
        """
        将队列中的消息 flush 到 lancedb table
        然后 ack 所有消息
        """
        if not self._queue:
            return
        
        # 记录 flush 开始时间
        flush_start_time = time.time()
        perf_timings = {}  # 性能分析时间点
        
        # 提取队列数据
        msgs = list(self._queue)
        self._queue.clear()
        queue_bytes = self._queue_bytes
        self._queue_bytes = 0
        self._last_flush_time = time.time()
        perf_timings['extract'] = time.time() - flush_start_time
        
        # 解析所有消息数据
        parse_start = time.time()
        docs = []
        valid_msgs = []
        for msg in msgs:
            doc = self._parse_message_data(msg)
            if doc is not None:
                docs.append(doc)
                valid_msgs.append(msg)
        perf_timings['parse'] = time.time() - parse_start
        
        if not docs:
            # 所有消息都解析失败，直接 ack（只 ack 真正的 pulsar.Message）
            for msg in msgs:
                if isinstance(msg, pulsar.Message) and self.consumer:
                    try:
                        self.consumer.acknowledge(msg)
                    except Exception:
                        pass
            return
        
        # 获取或创建表
        # 注意：如果表是新创建的，数据已经在 create_table 时写入了
        table_was_created = False
        table_start = time.time()
        try:
            table = await self._get_or_create_table(docs)
            # 检查表是否是新创建的（数据已经在 create_table 时写入）
            table_was_created = self._table_created_with_data
        except Exception as e:
            logger.error(f"[{self.table_name}] Failed to get/create table: {e}", exc_info=True)
            # 失败时重新放回队列
            self._queue = msgs + self._queue
            self._queue_bytes = queue_bytes + self._queue_bytes
            return
        perf_timings['get_table'] = time.time() - table_start
        
        # 写入数据
        try:
            # 如果表是新创建的，数据已经在 create_table 时写入了，不需要再次 add
            # 如果表是已存在的，需要通过 add 追加数据
            if not self._table_created_with_data:
                # 表已存在，使用 add 追加数据
                # 优化：先尝试直接 add，如果失败再获取 schema 并过滤
                write_start = time.time()
                try:
                    # 直接尝试 add，如果 schema 匹配就不需要过滤
                    await table.add(docs)
                    perf_timings['write'] = time.time() - write_start
                except ValueError as e:
                    # Schema 不匹配，需要获取 schema 并过滤
                    perf_timings['write'] = time.time() - write_start
                    if "not found in target schema" in str(e) or "Field" in str(e):
                        logger.debug(f"[{self.table_name}] Schema mismatch detected, filtering fields: {e}")
                        # 获取 schema（如果缓存不存在）
                        if self._schema_cache is None:
                            try:
                                schema = await table.schema() if asyncio.iscoroutinefunction(table.schema) else table.schema()
                                schema_field_names = set()
                                if hasattr(schema, 'names'):
                                    schema_field_names = set(schema.names)
                                elif hasattr(schema, 'field_names'):
                                    schema_field_names = set(schema.field_names)
                                elif hasattr(schema, '__iter__'):
                                    for field in schema:
                                        if hasattr(field, 'name'):
                                            schema_field_names.add(field.name)
                                self._schema_cache = schema_field_names
                            except Exception as schema_error:
                                logger.debug(f"[{self.table_name}] Could not get schema: {schema_error}")
                                self._schema_cache = set()  # 标记为已尝试但失败
                        
                        # 过滤字段
                        if self._schema_cache:
                            filter_start = time.time()
                            filtered_docs = []
                            for doc in docs:
                                filtered_doc = {k: v for k, v in doc.items() if k in self._schema_cache}
                                if filtered_doc:
                                    filtered_docs.append(filtered_doc)
                            perf_timings['schema_filter'] = time.time() - filter_start
                            
                            if filtered_docs:
                                # 重试 add 过滤后的数据
                                write_start = time.time()
                                try:
                                    await table.add(filtered_docs)
                                    perf_timings['write'] = (perf_timings.get('write', 0) + time.time() - write_start)
                                except Exception as add_error:
                                    perf_timings['write'] = (perf_timings.get('write', 0) + time.time() - write_start)
                                    raise add_error
                            else:
                                logger.warning(f"[{self.table_name}] All docs filtered out due to schema mismatch")
                                # 即使被过滤，也要 ack 消息（只 ack 真正的 pulsar.Message）
                                for msg in valid_msgs:
                                    if isinstance(msg, pulsar.Message) and self.consumer:
                                        try:
                                            self.consumer.acknowledge(msg)
                                        except Exception:
                                            pass
                                return
                        else:
                            # 无法获取 schema，直接抛出异常
                            raise
                    else:
                        # 其他类型的 ValueError，直接抛出
                        raise
                except Exception as e:
                    # 其他异常，记录并抛出
                    perf_timings['write'] = time.time() - write_start
                    raise
            # 记录指标：在数据成功写入数据库后记录
            # 计算 flush 耗时和实际写入的数据量
            write_end_time = time.time()
            
            if table_was_created:
                # 表是新创建的，数据已经在 create_table 时写入
                # 使用表创建时的写入耗时
                if hasattr(self, '_table_create_duration'):
                    perf_timings['write'] = self._table_create_duration
                self._table_created_with_data = False
                self._table_create_duration = 0.0  # 重置
            
            # 计算数据大小（优化：使用队列累积的大小，避免重复序列化）
            calc_size_start = time.time()
            if self.metrics:
                # 使用队列累积的字节数作为近似值（更快）
                # 注意：如果进行了 schema 过滤，实际写入可能略小，但差异通常很小
                total_bytes = queue_bytes
                # 如果需要精确值，可以取消下面的注释（但会显著增加耗时）
                # total_bytes = sum(len(json.dumps(doc).encode('utf-8')) for doc in docs)
            else:
                total_bytes = 0
            perf_timings['calc_size'] = time.time() - calc_size_start
            
            # 计算总耗时（在 ack 之前，因为 ack 不应该影响吞吐量指标）
            flush_end_time = time.time()
            flush_duration = flush_end_time - flush_start_time
            
            if self.metrics:
                # 记录本次 flush 的数据量、消息数和耗时，并获取指标
                flush_stats = self.metrics.record_flush(total_bytes, len(docs), flush_duration)
                
                # 立即输出本次 flush 的指标
                perf_info = ""
                if perf_timings:
                    # 只显示超过0.1ms的步骤，避免日志过于冗长
                    parts = []
                    if perf_timings.get('extract', 0) > 0.0001:
                        parts.append(f"Extract: {perf_timings['extract']*1000:.1f}ms")
                    if perf_timings.get('parse', 0) > 0.0001:
                        parts.append(f"Parse: {perf_timings['parse']*1000:.1f}ms")
                    if perf_timings.get('get_table', 0) > 0.0001:
                        parts.append(f"GetTable: {perf_timings['get_table']*1000:.1f}ms")
                    if perf_timings.get('write', 0) > 0.0001:
                        parts.append(f"Write: {perf_timings['write']*1000:.1f}ms")
                    if perf_timings.get('calc_size', 0) > 0.0001:
                        parts.append(f"CalcSize: {perf_timings['calc_size']*1000:.1f}ms")
                    if perf_timings.get('schema_filter', 0) > 0.0001:
                        parts.append(f"SchemaFilter: {perf_timings['schema_filter']*1000:.1f}ms")
                    # 计算其他未记录的时间（可能是等待或其他操作）
                    recorded_time = sum(perf_timings.get(k, 0) for k in ['extract', 'parse', 'get_table', 'write', 'calc_size', 'schema_filter'])
                    other_time = flush_duration - recorded_time
                    if other_time > 0.0001:
                        parts.append(f"Other: {other_time*1000:.1f}ms")
                    if parts:
                        perf_info = " | " + ", ".join(parts)
                
                logger.info(
                    f"[Flush] [{self.table_name}] "
                    f"Data: {flush_stats['flush_mb']:.2f} MB, "
                    f"Messages: {flush_stats['flush_messages']}, "
                    f"Duration: {flush_stats['flush_duration']:.3f}s, "
                    f"Throughput: {flush_stats['flush_mbps']:.2f} MB/s, "
                    f"Msg/s: {flush_stats['flush_msg_per_sec']:.2f}"
                    f"{perf_info}"
                )
            
            # 异步执行 ack 操作，不阻塞 flush 流程
            # 注意：ack 操作在数据成功写入数据库后执行，即使 ack 失败也不会影响数据完整性
            async def _async_ack_messages():
                ack_start = time.time()
                ack_failed = 0
                for msg in valid_msgs:
                    # 只对真正的 pulsar.Message 进行 acknowledge
                    if isinstance(msg, pulsar.Message):
                        if self.consumer:
                            try:
                                self.consumer.acknowledge(msg)
                            except Exception as e:
                                ack_failed += 1
                                logger.warning(f"[{self.table_name}] Ack failed: {e}, msg_id: {msg.message_id()}")
                    else:
                        # MessageWrapper 不需要 ack，因为它是从字典数据创建的
                        logger.debug(f"[{self.table_name}] Skipping acknowledge for MessageWrapper, msg_id: {msg.message_id()}")
                
                ack_duration = time.time() - ack_start
                if ack_failed > 0:
                    logger.error(
                        f"[{self.table_name}] Flushed {len(docs)} docs, "
                        f"but {ack_failed} messages ack failed (ack took {ack_duration*1000:.1f}ms)"
                    )
                else:
                    logger.debug(
                        f"[{self.table_name}] Flushed {len(docs)} docs, "
                        f"{len(valid_msgs)} messages acked (ack took {ack_duration*1000:.1f}ms)"
                    )
            
            # 创建异步任务执行 ack，不等待完成
            asyncio.create_task(_async_ack_messages())
        
        except Exception as e:
            logger.error(f"[{self.table_name}] Flush failed: {e}", exc_info=True)
            # 失败时重新放回队列
            self._queue = msgs + self._queue
            self._queue_bytes = queue_bytes + self._queue_bytes
    
    async def close(self):
        """关闭 Stream，执行最后一次 flush"""
        if self._stop:
            return
        
        self._stop = True
        
        # 取消定时器
        if self._timer_task and not self._timer_task.done():
            self._timer_task.cancel()
            try:
                await self._timer_task
            except asyncio.CancelledError:
                pass
        
        # 最后一次 flush
        try:
            await self._flush()
        except Exception as e:
            logger.error(f"[{self.table_name}] Final flush failed: {e}", exc_info=True)
        
        logger.debug(f"[{self.table_name}] Stream closed")


class PulsarToLanceDBModule(FSModule):
    """
    FSModule: Import Pulsar messages into LanceDB
    Service (FSFunction): 管理多个 Stream，按 topic 分发消息
    """
    
    def __init__(self, config: Optional[StreamConfig] = None, metrics_report_interval: float = 10.0):
        self._consumer: Optional[pulsar.Consumer] = None
        self.topic_to_stream: Dict[str, Stream] = {}  # map[topic_name, Stream]
        self.context: Optional[FSContext] = None
        self.config = config or StreamConfig()
        self.metrics = ThroughputMetrics(report_interval_seconds=metrics_report_interval)
        self._metrics_started = False
    
    def init(self, context: FSContext):
        """初始化模块"""
        self.context = context
        self._consumer = getattr(context, 'consumer', None)
    
    async def _get_or_create_stream(self, topic_name: str) -> Stream:
        """根据 topic_name 获取或创建 Stream"""
        if topic_name not in self.topic_to_stream:
            consumer = getattr(self.context, 'consumer', None) or self._consumer
            self.topic_to_stream[topic_name] = Stream(topic_name, consumer, self.config, self.metrics)
        return self.topic_to_stream[topic_name]
    
    async def process(self, context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理消息
        根据 msg.topic_name()，找到对应的 Stream
        如果 stream 不存在就创建
        然后 stream.send(msg)
        """
        # 启动指标报告（延迟启动，确保在异步环境中）
        if not self._metrics_started:
            await self.metrics.start_reporting()
            self._metrics_started = True
        
        # 获取消息对象
        # function_stream 框架传递的是字典数据，需要尝试多种方式获取 pulsar.Message
        msg = None
        
        # 方式1: data 本身就是 pulsar.Message（直接传递的情况）
        if isinstance(data, pulsar.Message):
            msg = data
        # 方式2: data 字典中包含 _message 或 message 字段
        elif isinstance(data, dict):
            msg = data.get('_message') or data.get('message')
            # 方式3: 从 context 中获取原始消息
            if not msg or not isinstance(msg, pulsar.Message):
                msg = getattr(context, 'message', None) or getattr(context, '_message', None)
            
            # 方式4: 如果仍然没有找到 pulsar.Message，创建 MessageWrapper
            # function_stream 框架可能只传递了数据字典，没有原始消息对象
            if not msg or not isinstance(msg, pulsar.Message):
                # 从 context 或 data 中获取 topic_name
                topic_name = (
                    getattr(context, 'topic_name', None) or
                    getattr(context, 'topic', None) or
                    data.get('_topic_name') or
                    data.get('topic_name') or
                    data.get('topic') or
                    "test-lancedb-topic"  # 默认 topic（从 config.yaml）
                )
                
                # 创建 MessageWrapper 来包装字典数据
                msg = MessageWrapper(data, topic_name)
                logger.debug(f"Created MessageWrapper for topic: {topic_name}")
        
        if not msg:
            raise ValueError("Cannot extract message from data or context")
        
        # 获取 topic_name
        topic_name = msg.topic_name()
        if not topic_name:
            raise ValueError("Message must have a valid topic_name")
        
        # 获取或创建 Stream
        stream = await self._get_or_create_stream(topic_name)
        
        # 发送消息到 Stream
        await stream.send(msg)
        
        return {"status": "processed", "topic": topic_name}
    
    async def close(self):
        """关闭模块，清理所有 Stream"""
        # 停止指标报告
        if self._metrics_started:
            await self.metrics.stop_reporting()
            # 输出最终统计信息
            final_stats = await self.metrics.get_stats()
            logger.info(
                f"[Throughput] Final Stats - "
                f"Total Processed: {final_stats['total_mb_processed']:.2f} MB, "
                f"Average Throughput: {final_stats['average_throughput_mbps']:.2f} MB/s, "
                f"Total Messages: {final_stats['total_messages_processed']}, "
                f"Elapsed Time: {final_stats['elapsed_seconds']:.2f}s"
            )
        
        # 关闭所有 Stream
        streams = list(self.topic_to_stream.values())
        for stream in streams:
            try:
                await stream.close()
            except Exception as e:
                logger.warning(f"Error closing stream {stream.table_name}: {e}")


# 默认配置
DEFAULT_CONFIG = StreamConfig()
