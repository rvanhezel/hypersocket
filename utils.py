import asyncio
import logging
from typing import Coroutine



def spawn_with_exception(fun:  Coroutine):
    task = asyncio.create_task(fun)
    task.add_done_callback(_reraise_task_exception)
    return task

def _reraise_task_exception(task: asyncio.Task):
    if task.cancelled():
        return
    if exc := task.exception():
        logging.error(f"Spawned task raised exception: {exc}")
        raise exc