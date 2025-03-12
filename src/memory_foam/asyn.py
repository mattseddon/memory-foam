import asyncio

from fsspec.asyn import get_loop
from typing import AsyncIterable, Awaitable, Iterator, TypeVar

T = TypeVar("T")


async def queue_task_result(coro: Awaitable[T], queue: asyncio.Queue, loop=get_loop()):
    task = asyncio.ensure_future(coro, loop=loop)
    result = await task
    await queue.put(result)
    return task


def iter_over_async(
    ait: AsyncIterable[T], loop: asyncio.AbstractEventLoop
) -> Iterator[T]:
    """Wrap an asynchronous iterator into a synchronous one"""

    ait = ait.__aiter__()

    # helper async fn that just gets the next element from the async iterator
    async def get_next():
        try:
            obj = await ait.__anext__()
            return False, obj
        except StopAsyncIteration:
            return True, None

    # actual sync iterator
    while True:
        done, obj = asyncio.run_coroutine_threadsafe(get_next(), loop).result()
        if done:
            break
        yield obj
