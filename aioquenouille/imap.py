# =============================================================================
# Aioquenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy async iterable consumer.
#

import time
import heapq
import asyncio
from itertools import count
from collections.abc import Iterable, Sized
from collections import OrderedDict

from aioquenouille.constants import (
    THE_END,
    DEFAULT_BUFFER_SIZE,
    DEFAULT_MAX_WORKERS,
    DEFAULT_PARALLELISM,
    DEFAULT_THROTTLE
)
from aioquenouille.utils import clear, smash


class Job(object):
    """
    Class representing a job to be performed by a worker.
    """

    __slots__ = ("item", "index", "group", "result")

    def __init__(self, item, index=None, group=None):
        self.item = item
        self.index = index
        self.group = group
        self.result = None


class ThrottledGroup(object):
    __slots__ = ("throttle", "groups", "groups_heap")

    def __init__(self, throttle):
        self.throttle = throttle
        self.groups = set()
        self.groups_heap = []

    def add(self, job: Job):

        throttle_time = self.throttle

        if callable(self.throttle):
            throttle_time = self.throttle(
                job.group,
                job.item,
                job.result
            )

            if throttle_time is None:
                throttle_time = 0

            if not isinstance(throttle_time, (int, float)) or throttle_time < 0:
                raise TypeError("callable 'throttle' must return numbers >= 0")

        if throttle_time != 0:

            self.groups.add(job.group)
            heapq.heappush(self.groups_heap, (time.time() + throttle_time, job.group))

    def cleanup(self):
        while self.groups_heap and self.groups_heap[0][0] < time.time():
            self.groups.remove(self.groups_heap[0][1])
            heapq.heappop(self.groups_heap)


class Buffer(object):
    """
    Class representing the buffer.
    """

    __slots__ = ("items", "maxsize", "parallelism", "worked_groups", "throttle")

    def __init__(self, maxsize, parallelism, throttle):
        self.maxsize = maxsize
        self.items = OrderedDict()
        self.parallelism = parallelism
        self.worked_groups = {}
        self.throttle = throttle

    def full(self):
        count = len(self.items)
        assert count <= self.maxsize
        return count == self.maxsize

    def empty(self):
        count = len(self.items)
        assert count <= self.maxsize
        return count == 0

    def __can_work(self, job):

        if job.group is None:
            return True

        if job.group in self.throttle.groups:
            return False

        count = self.worked_groups.get(job.group, 0)

        parallelism = self.parallelism
        if callable(self.parallelism):
            parallelism = self.parallelism(job.group)

            if parallelism is None:
                return True

            if not isinstance(parallelism, int) or parallelism < 1:
                raise TypeError("callable 'parallelism' must return positive integers")

        return count < parallelism

    def __find_suitable_job(self):
        for job in self.items.values():
            if self.__can_work(job):
                return job

        return None

    def put(self, job: Job):
        self.items[id(job)] = job

    def get(self):

        self.throttle.cleanup()

        if len(self.items) == 0:
            return None

        job = self.__find_suitable_job()

        if job is not None:
            del self.items[id(job)]
            return job

        return None

    def register_job(self, job: Job):
        group = job.group

        if group is None:
            return

        if group not in self.worked_groups:
                self.worked_groups[group] = 1
        else:
            self.worked_groups[group] += 1

    def unregister_job(self, job: Job):
        group = job.group

        if group is None:
            return

        assert group in self.worked_groups
        if self.worked_groups[group] == 1:
            del self.worked_groups[group]
        else:
            self.worked_groups[group] -= 1

        self.throttle.add(job)


class OrderedOutputBuffer(object):
    """
    Class in charge of yielding values in the same order as they were extracted
    from the iterated stream.
    Note that this requires to buffer values into memory until the next valid
    item has been processed by a worker thread.
    """

    def __init__(self):
        self.last_index = 0
        self.items = {}

    def flush(self):
        while self.last_index in self.items:
            yield self.items.pop(self.last_index).result
            self.last_index += 1

    def output(self, job):
        if job.index == self.last_index:
            self.last_index += 1
            yield job.result
            yield from self.flush()
        else:
            self.items[job.index] = job


def validate_queue_kwargs(iterable, func, max_workers, ordered, key, parallelism, buffer_size, throttle):

    if not isinstance(iterable, Iterable):
        raise TypeError("target is not iterable")

    if not callable(func):
        raise TypeError('worker function is not callable')

    if not isinstance(ordered, bool):
        raise TypeError("'ordered' is not a boolean")

    if not asyncio.iscoroutinefunction(func):
        raise TypeError("'func' is not async")

    if not isinstance(max_workers, int) or max_workers < 1:
        raise TypeError("'max_workers' is not an integer > 0")

    if key is not None and not callable(key):
        raise TypeError("'key' is not callable")

    if not isinstance(parallelism, (int, float)) and not callable(parallelism):
        raise TypeError("'parallelism' is not a number nor callable")

    if isinstance(parallelism, int) and parallelism < 0:
        raise TypeError("'parallelism' is not a positive integer")

    if not isinstance(buffer_size, int) or buffer_size < 1:
        raise TypeError("'buffer_size' is not an integer > 0")

    if not isinstance(throttle, (int, float)) and not callable(throttle):
        raise TypeError("'throttle' is not a number nor callable")

    if isinstance(throttle, (int, float)) and throttle < 0:
        raise TypeError("'throttle' cannot be negative")


async def worker(func, job_queue, output_queue, event_job_queue_blocks_buffer):
    try:

        while True:
            job = await job_queue.get()
            job.result = await func(job.item)
            output_queue.put_nowait(job)
            event_job_queue_blocks_buffer.set()
            job_queue.task_done()

    except BaseException as e:
        job_queue.task_done()
        clear(job_queue)
        smash(output_queue, e)


async def iterable_to_queue(iterable, buffer, job_counter, job_queue, output_queue, event_job_queue_blocks_buffer, key=None):
    while True:
        try:
            job = buffer.get()

        except BaseException as e:
            clear(job_queue)
            smash(output_queue, e)
            await output_queue.join()
            raise e

        if job is None:
            try:
                item = next(iterable)

            except StopIteration:

                if not buffer.empty():

                    if job_queue.empty() and output_queue.empty():
                        await job_queue.join()
                        continue

                    await event_job_queue_blocks_buffer.wait()
                    event_job_queue_blocks_buffer.clear()

                    continue

                await job_queue.join()
                output_queue.put_nowait(THE_END)
                await output_queue.join()
                break

            group = None

            if key is not None:

                try:
                    group = key(item)

                except BaseException as e:
                    clear(job_queue)
                    smash(output_queue, e)
                    await output_queue.join()
                    raise e

            job = Job(item=item, index=next(job_counter), group=group)

            if not buffer.full():
                buffer.put(job)
                continue

            job_to_process = buffer.get()

            while job_to_process is None:

                if job_queue.empty() and output_queue.empty():
                    await job_queue.join()

                else:
                    await event_job_queue_blocks_buffer.wait()
                    event_job_queue_blocks_buffer.clear()

                job_to_process = buffer.get()

            buffer.register_job(job_to_process)

            await job_queue.put(job_to_process)

            buffer.put(job)
            continue

        buffer.register_job(job)

        await job_queue.put(job)


async def generate_from_output_queue(output_queue, buffer, ordered_output_buffer=None):
    while True:
        job = await output_queue.get()
        output_queue.task_done()

        if isinstance(job, Exception):
            raise job

        if job is THE_END:
            break

        buffer.unregister_job(job)

        if isinstance(ordered_output_buffer, OrderedOutputBuffer):
            for result in ordered_output_buffer.output(job):
                yield result
        else:
            yield job.result

        del job


def imap(iterable, func, max_workers=DEFAULT_MAX_WORKERS, ordered=False, key=None, parallelism=DEFAULT_PARALLELISM, buffer_size=DEFAULT_BUFFER_SIZE, throttle=DEFAULT_THROTTLE):

    validate_queue_kwargs(iterable=iterable, func=func, ordered=ordered, max_workers=max_workers, key=key, parallelism=parallelism, buffer_size=buffer_size, throttle=throttle)

    if isinstance(iterable, Sized):
        max_workers = min(max_workers, len(iterable))

    iterable = iter(iterable)
    buffer = Buffer(buffer_size, parallelism, ThrottledGroup(throttle))
    job_queue = asyncio.Queue(maxsize=max_workers)

    async def get_generator():
        job_counter = count()
        event_job_queue_blocks_buffer = asyncio.Event()
        output_queue = asyncio.Queue()
        ordered_output_buffer = OrderedOutputBuffer()

        tasks = []

        for i in range(max_workers):
            task = asyncio.create_task(worker(func, job_queue, output_queue, event_job_queue_blocks_buffer))
            tasks.append(task)

        task_job_queue = asyncio.create_task(iterable_to_queue(iterable, buffer, job_counter, job_queue, output_queue, event_job_queue_blocks_buffer, key))
        tasks.append(task_job_queue)

        async def cleanup(failed=False):
            if not failed:
                await task_job_queue

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            if ordered:
                async for i in generate_from_output_queue(output_queue, buffer, ordered_output_buffer):
                    yield i
            else:
                async for i in generate_from_output_queue(output_queue, buffer):
                    yield i
        except BaseException as e:
            await cleanup(failed=True)
            raise e

        await cleanup()

    return get_generator()
