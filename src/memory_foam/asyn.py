from asyncio import (
    AbstractEventLoop,
    Queue,
    Task,
    ensure_future,
    run_coroutine_threadsafe,
)
from collections.abc import AsyncIterable, Awaitable, Iterator
from typing import TypeVar

from fsspec.asyn import get_loop  # noqa: F401

T = TypeVar("T")


async def queue_task_result(
    coro: Awaitable[T], queue: Queue, loop: AbstractEventLoop
) -> Task:
    task = ensure_future(coro, loop=loop)
    result = await task
    await queue.put(result)
    return task


def sync_iter_async(ait: AsyncIterable[T], loop: AbstractEventLoop) -> Iterator[T]:
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
        yield obj  # pyright: ignore[reportReturnType]
