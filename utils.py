import asyncio


def execute_async(func, *args, **kwargs):
    """ Executes the given function. If the function is an asyncio
    coroutine, schedules it.
    """
    if not callable(func):
        return
    if asyncio.iscoroutinefunction(func):
        asyncio.create_task(func(*args, **kwargs))
    else:
        func(*args, **kwargs)
