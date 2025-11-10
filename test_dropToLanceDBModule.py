import json
from unittest.mock import Mock, patch

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
        db = Mock()
        db.table_names = Mock(return_value=[])
        
        table_mocks = {}
        
        def create_table_side_effect(table_name, data):
            if table_name not in table_mocks:
                table = Mock()
                table.add = Mock()
                table_mocks[table_name] = table
            return table_mocks[table_name]
        
        def open_table_side_effect(table_name):
            if table_name not in table_mocks:
                table = Mock()
                table.add = Mock()
                table_mocks[table_name] = table
            return table_mocks[table_name]
        
        db.create_table.side_effect = create_table_side_effect
        db.open_table.side_effect = open_table_side_effect
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
