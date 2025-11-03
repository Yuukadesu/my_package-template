from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from main import get_current_time


class MockFSContext:
    """Mock implementation of FSContext for testing"""

    def __init__(self, config_format="%Y-%m-%d %H:%M:%S"):
        self._config = {"format": config_format}

    def get_config(self, key: str):
        """Mock get_config method"""
        return self._config.get(key)


class TestGetCurrentTime:
    """Test cases for get_current_time function"""

    def test_get_current_time_default_format(self):
        """Test get_current_time with default time format"""
        # Arrange
        context = MockFSContext()
        data = {"name": "John"}

        # Act
        with patch("main.datetime.datetime") as mock_datetime:
            mock_now = datetime(2025, 8, 30, 12, 30, 45)
            mock_datetime.now.return_value = mock_now

            result = get_current_time(context, data)

        # Assert
        assert result == {"result": "Hi, John, the current time is 2025-08-30 12:30:45."}

    def test_get_current_time_custom_format(self):
        """Test get_current_time with custom time format"""
        # Arrange
        context = MockFSContext("%H:%M")
        data = {"name": "Alice"}

        # Act
        with patch("main.datetime.datetime") as mock_datetime:
            mock_now = datetime(2025, 8, 30, 12, 30, 45)
            mock_datetime.now.return_value = mock_now

            result = get_current_time(context, data)

        # Assert
        assert result == {"result": "Hi, Alice, the current time is 12:30."}
