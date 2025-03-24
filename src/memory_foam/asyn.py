from asyncio import (
    AbstractEventLoop,
    ensure_future,
    Queue,
    run_coroutine_threadsafe,
    Task,
)

from fsspec.asyn import get_loop
from typing import AsyncIterable, Awaitable, Iterator, TypeVar

T = TypeVar("T")


async def queue_task_result(coro: Awaitable[T], queue: Queue, loop=get_loop()) -> Task:
    task = ensure_future(coro, loop=loop)
    result = await task
    await queue.put(result)
    return task


def sync_iter_async(
    ait: AsyncIterable[T], loop: AbstractEventLoop = get_loop()
) -> Iterator[T]:
    """Wrap an asynchronous iterator into a synchronous one"""

    ait = ait.__aiter__()

    async def get_next():
        try:
            obj = await ait.__anext__()
            return False, obj
        except StopAsyncIteration:
            return True, None

    while True:
        done, obj = run_coroutine_threadsafe(get_next(), loop).result()
        if done:
            break
        yield obj
