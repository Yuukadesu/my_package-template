import asyncio
import json
import os
import tempfile
from unittest.mock import Mock, patch

import lancedb
import pulsar
import pytest

from dropToLanceDBModule import (
    PulsarToLanceDBModule,
    StreamConfig,
)


class MockFSContext:
    def __init__(self, consumer=None):
        self.consumer = consumer


class MockPulsarMessage:
    def __init__(self, topic_name: str, data: bytes, message_id: str = "msg-1"):
        self._topic_name = topic_name
        self._data = data
        self._message_id = message_id

    def topic_name(self) -> str:
        return self._topic_name

    def data(self) -> bytes:
        return self._data

    def message_id(self) -> str:
        return self._message_id


class TestPulsarToLanceDBModule:
    @pytest.fixture
    def mock_consumer(self):
        consumer = Mock()
        consumer.acknowledge = Mock()
        return consumer

    @pytest.fixture
    def mock_db(self):
        from unittest.mock import AsyncMock
        
        db = Mock()
        # 异步版本的table_names
        async def async_table_names():
            return []
        db.table_names = AsyncMock(return_value=[])
        
        table_mocks = {}
        
        # 异步版本的create_table
        async def async_create_table_side_effect(table_name, data):
            if table_name not in table_mocks:
                table = Mock()
                # 异步版本的add
                table.add = AsyncMock()
                table_mocks[table_name] = table
            return table_mocks[table_name]
        
        # 异步版本的open_table
        async def async_open_table_side_effect(table_name):
            if table_name not in table_mocks:
                table = Mock()
                # 异步版本的add
                table.add = AsyncMock()
                table_mocks[table_name] = table
            return table_mocks[table_name]
        
        db.create_table = AsyncMock(side_effect=async_create_table_side_effect)
        db.open_table = AsyncMock(side_effect=async_open_table_side_effect)
        db._table_mocks = table_mocks
        
        return db

    @pytest.fixture
    def config(self):
        return StreamConfig(
            flush_interval_seconds = 1,
            flush_max_payload_bytes = 1024,
            flush_max_msg_count = 2,
            timer_check_interval = 1
        )

    @pytest.fixture
    def module(self, config):
        return PulsarToLanceDBModule(config=config)

    @pytest.fixture
    def context(self, mock_consumer):
        return MockFSContext(consumer=mock_consumer)

    @pytest.fixture
    def isinstance_patch(self):
        original_isinstance = isinstance
        def mock_isinstance(obj, cls):
            if cls == pulsar.Message and isinstance(obj, MockPulsarMessage):
                return True
            return original_isinstance(obj, cls)
        return patch('dropToLanceDBModule.isinstance', side_effect=mock_isinstance)

    @pytest.mark.asyncio
    async def test_multi_topic_multi_table(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """Testing multiple non-partition topics corresponding to multiple tables"""
        topics = ["topic1", "topic2", "topic3"]
        
        for topic in topics:
            assert "partition" not in topic.lower()

        try:
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

                    for topic in topics:
                        if topic in module.topic_to_stream:
                            stream = module.topic_to_stream[topic]
                            await stream.flush()

                    assert len(module.topic_to_stream) == len(topics)
                    
                    for topic in topics:
                        assert topic in module.topic_to_stream
                        stream = module.topic_to_stream[topic]
                        assert stream.table_name == topic

                    assert mock_consumer.acknowledge.call_count >= len(topics)
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_independent_tables_per_topic(self, module, context, mock_db, mock_consumer, isinstance_patch):
        """Test each non-partition topic has an independent table"""
        topic1 = "topic1"
        topic2 = "topic2"

        assert "partition" not in topic1.lower()
        assert "partition" not in topic2.lower()

        topic1_data = [{"id": i, "topic": topic1} for i in range(3)]
        topic2_data = [{"id": i, "topic": topic2} for i in range(3)]

        try:
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

                    for topic in [topic1, topic2]:
                        if topic in module.topic_to_stream:
                            stream = module.topic_to_stream[topic]
                            await stream.flush()

                    assert len(module.topic_to_stream) == 2
                    assert topic1 in module.topic_to_stream
                    assert topic2 in module.topic_to_stream

                    stream1 = module.topic_to_stream[topic1]
                    stream2 = module.topic_to_stream[topic2]
                    assert stream1.table_name == topic1
                    assert stream2.table_name == topic2
                    assert stream1.table is not stream2.table

                    assert mock_consumer.acknowledge.call_count >= len(topic1_data) + len(topic2_data)
        finally:
            await module.close()

    @pytest.mark.asyncio
    async def test_verify_data_written_to_lancedb(self, module, context, mock_consumer, isinstance_patch):
        """测试验证数据是否真正写入lancedb数据库"""
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
                            await stream.flush()
                        
                        # 使用lancedb接口查询数据验证是否真正写入
                        table_name = topic  # topic_to_table_name会返回topic本身
                        table_names = await test_db.table_names()
                        assert table_name in table_names, f"表 {table_name} 应该存在"
                        
                        table = await test_db.open_table(table_name)
                        
                        # 使用lancedb接口查询所有数据
                        # 尝试使用to_pandas()，如果不存在则使用to_arrow().to_pandas()
                        if hasattr(table, 'to_pandas'):
                            # 检查是否是异步方法
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
                            # 查找匹配的记录
                            matching_rows = df[df['id'] == expected_data['id']]
                            assert len(matching_rows) > 0, f"未找到 id={expected_data['id']} 的数据"
                            
                            row = matching_rows.iloc[0]
                            assert row['name'] == expected_data['name'], f"name字段不匹配: 期望 {expected_data['name']}, 实际 {row['name']}"
                            assert row['value'] == expected_data['value'], f"value字段不匹配: 期望 {expected_data['value']}, 实际 {row['value']}"
                        
                        # 验证acknowledge被调用
                        assert mock_consumer.acknowledge.call_count >= len(test_data)
            finally:
                await module.close()
