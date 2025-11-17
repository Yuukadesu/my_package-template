import asyncio
import logging
from typing import Any, Dict

from function_stream import FSContext, FSFunction

from dropToLanceDBModule import PulsarToLanceDBModule, StreamConfig

# 配置日志，确保能看到吞吐量指标
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('function_stream').setLevel(logging.WARNING)


async def main():
    # 创建 PulsarToLanceDBModule 实例
    # 高性能配置：更大的批量以减少 flush 频率，提高吞吐量
    # metrics_report_interval: 指标报告间隔（秒），默认10秒
    module = PulsarToLanceDBModule(
        config=StreamConfig(
            flush_interval_seconds=30,  # 30秒flush一次（累积更大的批量，提高吞吐量）
            flush_max_payload_bytes=100 * 1024 * 1024,  # 100MB就flush（最大化批量以提高吞吐量）
            flush_max_msg_count=10000,  # 10000条消息就flush
            timer_check_interval=2  # 2秒检查一次（减少检查开销）
        ),
        metrics_report_interval=10.0  # 每10秒报告一次吞吐量指标
    )
    
    # Initialize the FunctionStream function
    # config_path: 指定配置文件路径，如果不指定会使用默认的 config.yaml
    function = FSFunction(
        process_funcs={
            "pulsarToLanceDB": module,
        },
        config_path="config.yaml"  # 显式指定配置文件
    )

    try:
        print("Starting PulsarToLanceDB module service...")
        print("吞吐量指标将每10秒自动输出到日志中")
        print("按 Ctrl+C 停止服务\n")
        await function.start()
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        await module.close()
        await function.close()


if __name__ == "__main__":
    try:
        # Run the main function in an asyncio event loop
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nService stopped")
