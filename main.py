import asyncio
import datetime
from typing import Any, Dict

from function_stream import FSContext, FSFunction
from pydantic import BaseModel


class User(BaseModel):
    name: str

def get_current_time(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
    user = User.model_validate(data)
    now = datetime.datetime.now()
    time_format = context.get_config("format")
    return {"result": f"Hi, {user.name}, the current time is {now.strftime(time_format)}."}


async def main():
    # Initialize the FunctionStream function
    function = FSFunction(
        process_funcs={
            "getCurrentTime": get_current_time,
        },
    )

    try:
        print("Starting agent function service...")
        await function.start()
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        await function.close()


if __name__ == "__main__":
    try:
        # Run the main function in an asyncio event loop
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nService stopped")
