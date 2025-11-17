import asyncio
import logging
from typing import Any, Dict

from function_stream import FSContext, FSFunction
from dropToLanceDBModule import PulsarToLanceDBModule, StreamConfig

logging.getLogger('function_stream').setLevel(logging.WARNING)


async def main():
    # 实例化PulsarToLanceDBModule（超高性能配置，适配百万级发送）
    module = PulsarToLanceDBModule(
        config=StreamConfig(
            flush_interval_seconds=30,
            flush_max_payload_bytes=100 * 1024 * 1024,
            flush_max_msg_count=10000,
            timer_check_interval=5
        )
    )

    async def process_pulsar_to_lancedb(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
        if module.context is None:
            await module.init(context)

        return await module.process(context, data)
    
    # Initialize the FunctionStream function
    function = FSFunction(
        process_funcs={
            "pulsarToLanceDB": process_pulsar_to_lancedb,
        },
    )

    try:
        print("Starting PulsarToLanceDB module service...")
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
