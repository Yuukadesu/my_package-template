import asyncio
import json
import os
import tempfile
from unittest.mock import Mock, patch, AsyncMock

import lancedb
import pulsar
import pytest

from dropToLanceDBModule import (
    PulsarToLanceDBModule,
    StreamConfig,
    ThroughputMetrics,
)


class MockFSContext:
    def __init__(self, consumer=None):
        self.consumer = consumer


class MockPulsarMessage:
    """模拟 pulsar.Message"""
    def __init__(self, topic_name: str, data, message_id: str = "msg-1"):
        self._topic_name = topic_name
        self._data = data
        self._message_id = message_id

    def topic_name(self) -> str:
        return self._topic_name

    def data(self):
        return self._data

    def message_id(self) -> str:
        return self._message_id

    def properties(self):
        return {}


class TestPulsarToLanceDBModule:
    @pytest.fixture
    def mock_consumer(self):
        consumer = Mock()
        consumer.acknowledge = Mock()
        return consumer

    @pytest.fixture
    def mock_db(self):
        """创建模拟的异步数据库"""
        db = Mock()
        
        # 异步版本的 table_names
        db.table_names = AsyncMock(return_value=[])
        
        table_mocks = {}
        
        # 异步版本的 create_table
        async def async_create_table_side_effect(table_name, data):
            if table_name not in table_mocks:
                table = Mock()
                # 异步版本的 add
                table.add = AsyncMock()
                # 模拟 schema 方法
                async def async_schema():
                    # 返回一个模拟的 schema，包含所有字段
                    schema = Mock()
                    schema.names = list(data[0].keys()) if data else []
                    return schema
                table.schema = AsyncMock(side_effect=async_schema)
                table_mocks[table_name] = table
            return table_mocks[table_name]
        
        # 异步版本的 open_table
        async def async_open_table_side_effect(table_name):
            if table_name not in table_mocks:
                table = Mock()
                table.add = AsyncMock()
                # 模拟 schema 方法
                async def async_schema():
                    schema = Mock()
                    schema.names = []
                    return schema
                table.schema = AsyncMock(side_effect=async_schema)
                table_mocks[table_name] = table
            return table_mocks[table_name]
        
        db.create_table = AsyncMock(side_effect=async_create_table_side_effect)
        db.open_table = AsyncMock(side_effect=async_open_table_side_effect)
        db._table_mocks = table_mocks
        
        return db

    @pytest.fixture
    def config(self):
        return StreamConfig(
            flush_interval_seconds=1,
            flush_max_payload_bytes=1024,
            flush_max_msg_count=2,
            timer_check_interval=1
        )

    @pytest.fixture
    def module(self, config):
        return PulsarToLanceDBModule(config=config)

    @pytest.fixture
    def context(self, mock_consumer):
        return MockFSContext(consumer=mock_consumer)

    @pytest.fixture
    def isinstance_patch(self):
        """Patch isinstance 以支持 MockPulsarMessage"""
        original_isinstance = isinstance
        def mock_isinstance(obj, cls):
            if cls == pulsar.Message and isinstance(obj, MockPulsarMessage):
                return True
            return original_isinstance(obj, cls)
        return patch('dropToLanceDBModule.isinstance', side_effect=mock_isinstance)

    @pytest.mark.asyncio
    async def test_multi_topic_multi_table(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """测试多个 topic 对应多个 table"""
        topics = ["topic1", "topic2", "topic3"]
        
        try:
            module.init(context)
            with isinstance_patch:
                with patch('dropToLanceDBModule._db', mock_db):
                    for i, topic in enumerate(topics):
                        data = {"id": i, "topic": topic, "value": f"test-{i}"}
                        msg = MockPulsarMessage(
                            topic_name=topic,
                            data=json.dumps(data).encode('utf-8'),
                            message_id=f"msg-{i}"
                        )
                        result = await module.process(context, {"message": msg})
                        assert result == {"status": "processed", "topic": topic}

                    # 手动 flush 所有 stream
                    for topic in topics:
                        if topic in module.topic_to_stream:
                            stream = module.topic_to_stream[topic]
                            await stream._flush()

                    assert len(module.topic_to_stream) == len(topics)
                    
                    for topic in topics:
                        assert topic in module.topic_to_stream
                        stream = module.topic_to_stream[topic]
                        assert stream.table_name == topic

                    # 等待异步 ack 完成
                    await asyncio.sleep(0.1)
                    assert mock_consumer.acknowledge.call_count >= len(topics)
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_independent_tables_per_topic(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """测试每个 topic 有独立的 table"""
        topic1 = "topic1"
        topic2 = "topic2"

        topic1_data = [{"id": i, "topic": topic1} for i in range(3)]
        topic2_data = [{"id": i, "topic": topic2} for i in range(3)]

        try:
            module.init(context)
            with isinstance_patch:
                with patch('dropToLanceDBModule._db', mock_db):
                    for data in topic1_data:
                        msg = MockPulsarMessage(
                            topic_name=topic1,
                            data=json.dumps(data).encode('utf-8'),
                            message_id=f"msg-{topic1}-{data['id']}"
                        )
                        await module.process(context, {"message": msg})

                    for data in topic2_data:
                        msg = MockPulsarMessage(
                            topic_name=topic2,
                            data=json.dumps(data).encode('utf-8'),
                            message_id=f"msg-{topic2}-{data['id']}"
                        )
                        await module.process(context, {"message": msg})

                    # 手动 flush 所有 stream
                    for topic in [topic1, topic2]:
                        if topic in module.topic_to_stream:
                            stream = module.topic_to_stream[topic]
                            await stream._flush()

                    assert len(module.topic_to_stream) == 2
                    assert topic1 in module.topic_to_stream
                    assert topic2 in module.topic_to_stream

                    stream1 = module.topic_to_stream[topic1]
                    stream2 = module.topic_to_stream[topic2]
                    assert stream1.table_name == topic1
                    assert stream2.table_name == topic2
                    assert stream1._table is not stream2._table

                    # 等待异步 ack 完成
                    await asyncio.sleep(0.1)
                    assert mock_consumer.acknowledge.call_count >= len(topic1_data) + len(topic2_data)
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_verify_data_written_to_lancedb(self, module, context, mock_consumer, isinstance_patch):
        """测试验证数据是否真正写入 lancedb 数据库"""
        # 创建临时数据库目录
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "test-lancedb")
            test_db = await lancedb.connect_async(db_path)
            
            topic = "test_topic"
            test_data = [
                {"id": 1, "name": "test1", "value": "value1"},
                {"id": 2, "name": "test2", "value": "value2"},
                {"id": 3, "name": "test3", "value": "value3"},
            ]
            
            try:
                module.init(context)
                with isinstance_patch:
                    with patch('dropToLanceDBModule._db', test_db):
                        # 处理消息
                        for data in test_data:
                            msg = MockPulsarMessage(
                                topic_name=topic,
                                data=json.dumps(data).encode('utf-8'),
                                message_id=f"msg-{data['id']}"
                            )
                            result = await module.process(context, {"message": msg})
                            assert result == {"status": "processed", "topic": topic}
                        
                        # 刷新流以确保数据写入
                        if topic in module.topic_to_stream:
                            stream = module.topic_to_stream[topic]
                            await stream._flush()
                        
                        # 使用 lancedb 接口查询数据验证是否真正写入
                        table_name = topic
                        table_names = await test_db.table_names()
                        assert table_name in table_names, f"表 {table_name} 应该存在"
                        
                        table = await test_db.open_table(table_name)
                        
                        # 使用 lancedb 接口查询所有数据
                        if hasattr(table, 'to_pandas'):
                            if asyncio.iscoroutinefunction(table.to_pandas):
                                df = await table.to_pandas()
                            else:
                                df = table.to_pandas()
                        else:
                            if hasattr(table, 'to_arrow'):
                                arrow_table = await table.to_arrow() if asyncio.iscoroutinefunction(table.to_arrow) else table.to_arrow()
                                df = arrow_table.to_pandas()
                            else:
                                raise ValueError("Table does not support to_pandas or to_arrow")
                        
                        # 验证数据是否写入
                        assert len(df) == len(test_data), f"应该写入 {len(test_data)} 条数据，实际写入 {len(df)} 条"
                        
                        # 验证每条数据的内容
                        for expected_data in test_data:
                            matching_rows = df[df['id'] == expected_data['id']]
                            assert len(matching_rows) > 0, f"未找到 id={expected_data['id']} 的数据"
                            
                            row = matching_rows.iloc[0]
                            assert row['name'] == expected_data['name'], f"name字段不匹配: 期望 {expected_data['name']}, 实际 {row['name']}"
                            assert row['value'] == expected_data['value'], f"value字段不匹配: 期望 {expected_data['value']}, 实际 {row['value']}"
                        
                        # 等待异步 ack 完成
                        await asyncio.sleep(0.1)
                        assert mock_consumer.acknowledge.call_count >= len(test_data)
            finally:
                await module.close()

    @pytest.mark.asyncio
    async def test_batch_flush_by_count(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """测试按消息数量触发 flush"""
        config = StreamConfig(
            flush_interval_seconds=100,
            flush_max_payload_bytes=1024 * 1024,
            flush_max_msg_count=3,
            timer_check_interval=1
        )
        module = PulsarToLanceDBModule(config=config)
        module.init(context)
        
        topic = "test_topic"
        test_data = [
            {"id": 1, "value": "value1"},
            {"id": 2, "value": "value2"},
            {"id": 3, "value": "value3"},
            {"id": 4, "value": "value4"},
        ]
        
        try:
            with isinstance_patch:
                with patch('dropToLanceDBModule._db', mock_db):
                    # 发送前 3 条消息，应该触发 flush
                    for i, data in enumerate(test_data[:3]):
                        msg = MockPulsarMessage(
                            topic_name=topic,
                            data=json.dumps(data).encode('utf-8'),
                            message_id=f"msg-{i}"
                        )
                        await module.process(context, {"message": msg})
                    
                    # 等待 flush 和异步 ack 完成
                    await asyncio.sleep(0.2)
                    
                    # 验证前 3 条消息已经被 ack
                    assert mock_consumer.acknowledge.call_count >= 3
                    
                    # 发送第 4 条消息
                    msg = MockPulsarMessage(
                        topic_name=topic,
                        data=json.dumps(test_data[3]).encode('utf-8'),
                        message_id="msg-3"
                    )
                    await module.process(context, {"message": msg})
                    
                    # 手动 flush 最后一条消息
                    if topic in module.topic_to_stream:
                        stream = module.topic_to_stream[topic]
                        await stream._flush()
                    
                    # 等待异步 ack 完成
                    await asyncio.sleep(0.1)
                    assert mock_consumer.acknowledge.call_count >= 4
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_batch_flush_by_size(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """测试按数据大小触发 flush"""
        large_data = {"id": 1, "value": "x" * 1000}
        config = StreamConfig(
            flush_interval_seconds=100,
            flush_max_payload_bytes=1500,
            flush_max_msg_count=1000,
            timer_check_interval=1
        )
        module = PulsarToLanceDBModule(config=config)
        module.init(context)
        
        topic = "test_topic"
        
        try:
            with isinstance_patch:
                with patch('dropToLanceDBModule._db', mock_db):
                    # 发送两条大消息，应该触发 flush
                    for i in range(2):
                        msg = MockPulsarMessage(
                            topic_name=topic,
                            data=json.dumps(large_data).encode('utf-8'),
                            message_id=f"msg-{i}"
                        )
                        await module.process(context, {"message": msg})
                    
                    # 等待 flush 和异步 ack 完成
                    await asyncio.sleep(0.2)
                    
                    # 验证消息已经被 ack
                    assert mock_consumer.acknowledge.call_count >= 2
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_timer_flush(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """测试定时器触发 flush"""
        config = StreamConfig(
            flush_interval_seconds=0.5,
            flush_max_payload_bytes=1024 * 1024,
            flush_max_msg_count=1000,
            timer_check_interval=0.1
        )
        module = PulsarToLanceDBModule(config=config)
        module.init(context)
        
        topic = "test_topic"
        
        try:
            with isinstance_patch:
                with patch('dropToLanceDBModule._db', mock_db):
                    # 发送一条消息
                    msg = MockPulsarMessage(
                        topic_name=topic,
                        data=json.dumps({"id": 1, "value": "test"}).encode('utf-8'),
                        message_id="msg-1"
                    )
                    await module.process(context, {"message": msg})
                    
                    # 等待超过 flush_interval_seconds 时间
                    await asyncio.sleep(0.7)
                    
                    # 等待异步 ack 完成
                    await asyncio.sleep(0.1)
                    # 验证消息已经被 ack（定时器应该触发了 flush）
                    assert mock_consumer.acknowledge.call_count >= 1
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_message_wrapper_support(self, module, context, mock_db, mock_consumer):
        """测试 MessageWrapper 支持（字典数据）"""
        topic = "test_topic"
        test_data = {"id": 1, "name": "test", "value": "value1", "topic_name": topic}
        
        try:
            module.init(context)
            with patch('dropToLanceDBModule._db', mock_db):
                # 使用字典数据（模拟 function_stream 框架的行为）
                # 需要在 data 中提供 topic_name，否则会使用默认值
                result = await module.process(context, test_data)
                assert result == {"status": "processed", "topic": topic}
                
                # 手动 flush
                if topic in module.topic_to_stream:
                    stream = module.topic_to_stream[topic]
                    await stream._flush()
                
                # 验证数据被处理
                assert topic in module.topic_to_stream
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_throughput_metrics(self):
        """测试吞吐量指标收集"""
        metrics = ThroughputMetrics(report_interval_seconds=1.0)
        
        # 记录几次 flush
        stats1 = metrics.record_flush(1024 * 1024, 1000, 0.1)  # 1MB, 1000条, 0.1秒
        assert stats1['flush_mb'] == 1.0
        assert stats1['flush_messages'] == 1000
        assert stats1['flush_mbps'] > 0
        
        stats2 = metrics.record_flush(2 * 1024 * 1024, 2000, 0.2)  # 2MB, 2000条, 0.2秒
        assert stats2['flush_mb'] == 2.0
        assert stats2['flush_messages'] == 2000
        
        # 获取统计信息
        overall_stats = await metrics.get_stats()
        assert overall_stats['total_mb_processed'] == 3.0
        assert overall_stats['total_messages_processed'] == 3000
        assert overall_stats['average_throughput_mbps'] > 0
        
        # 停止报告
        await metrics.stop_reporting()

    @pytest.mark.asyncio
    async def test_schema_filter_optimization(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """测试 schema 过滤优化（先尝试直接写入）"""
        topic = "test_topic"
        
        # 创建模拟表，表已存在（不是新创建的），所以会调用 add
        table_mock = Mock()
        table_mock.add = AsyncMock()
        async def async_schema():
            schema = Mock()
            schema.names = ["id", "name", "value"]
            return schema
        table_mock.schema = AsyncMock(side_effect=async_schema)
        
        # 表已存在，所以 table_names 返回包含该表
        async def async_table_names():
            return [topic]
        
        async def async_open_table(table_name):
            return table_mock
        
        mock_db.table_names = AsyncMock(side_effect=async_table_names)
        mock_db.open_table = AsyncMock(side_effect=async_open_table)
        
        try:
            module.init(context)
            with isinstance_patch:
                with patch('dropToLanceDBModule._db', mock_db):
                    # 发送消息
                    msg = MockPulsarMessage(
                        topic_name=topic,
                        data=json.dumps({"id": 1, "name": "test", "value": "value1"}).encode('utf-8'),
                        message_id="msg-1"
                    )
                    await module.process(context, {"message": msg})
                    
                    # 手动 flush
                    if topic in module.topic_to_stream:
                        stream = module.topic_to_stream[topic]
                        await stream._flush()
                    
                    # 验证直接写入被调用（没有 schema 过滤）
                    table_mock.add.assert_called()
                    # schema 方法不应该被调用（因为直接写入成功）
                    assert table_mock.schema.call_count == 0
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_schema_filter_on_mismatch(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """测试 schema 不匹配时的过滤"""
        topic = "test_topic"
        
        # 创建模拟表，表已存在（不是新创建的），所以会调用 add
        table_mock = Mock()
        call_count = 0
        
        async def async_add(data):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # 第一次调用失败（schema 不匹配）
                raise ValueError("Field 'extra_field' not found in target schema")
            # 第二次调用成功（过滤后）
        
        table_mock.add = AsyncMock(side_effect=async_add)
        
        async def async_schema():
            schema = Mock()
            schema.names = ["id", "name"]  # 只有这两个字段
            return schema
        table_mock.schema = AsyncMock(side_effect=async_schema)
        
        # 表已存在，所以 table_names 返回包含该表
        async def async_table_names():
            return [topic]
        
        async def async_open_table(table_name):
            return table_mock
        
        mock_db.table_names = AsyncMock(side_effect=async_table_names)
        mock_db.open_table = AsyncMock(side_effect=async_open_table)
        
        try:
            module.init(context)
            with isinstance_patch:
                with patch('dropToLanceDBModule._db', mock_db):
                    # 发送包含额外字段的消息
                    msg = MockPulsarMessage(
                        topic_name=topic,
                        data=json.dumps({"id": 1, "name": "test", "extra_field": "extra"}).encode('utf-8'),
                        message_id="msg-1"
                    )
                    await module.process(context, {"message": msg})
                    
                    # 手动 flush
                    if topic in module.topic_to_stream:
                        stream = module.topic_to_stream[topic]
                        await stream._flush()
                    
                    # 验证：第一次 add 失败，然后获取 schema，第二次 add 成功
                    assert table_mock.add.call_count == 2
                    assert table_mock.schema.call_count == 1
        finally:
            await module.close()
