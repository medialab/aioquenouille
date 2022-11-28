# =============================================================================
# Aioquenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy async iterable consumer.
#
# Job.index is not used yet
# Exceptions doesn't seem to be propagated
#

import time
import heapq
import asyncio
from itertools import count
from collections import OrderedDict
from collections.abc import Iterable

from aioquenouille.constants import (
    THE_END,
    DEFAULT_BUFFER_SIZE,
    DEFAULT_MAX_WORKERS,
    DEFAULT_PARALLELISM,
    DEFAULT_THROTTLE
)


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

    def __full(self):
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
        assert not self.__full()
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


def validate_queue_kwargs(iterable, func, max_workers, key, parallelism, buffer_size, throttle):

    if not isinstance(iterable, Iterable):
        raise TypeError("target is not iterable")

    if not callable(func):
        raise TypeError('worker function is not callable')

    if not asyncio.iscoroutinefunction(func):
        raise TypeError("worker function is not async")

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
        raise TypeError('"throttle" is not a number nor callable')

    if isinstance(throttle, (int, float)) and throttle < 0:
        raise TypeError('"throttle" cannot be negative')


async def worker(job_queue, output_queue, func, event_queue_full):
    while True:
        job = await job_queue.get()
        job.result = await func(job.item)
        output_queue.put_nowait(job)
        job_queue.task_done()
        event_queue_full.set()


async def iterable_to_queue(iterable, job_queue, output_queue, job_counter, buffer, event_queue_full, key=None):
    while True:
        job = buffer.get()

        if job is None:
            try:
                item = next(iterable)

            except StopIteration:

                if not buffer.empty():

                    if job_queue.empty() and output_queue.empty():
                        await job_queue.join()
                        continue

                    await event_queue_full.wait()
                    event_queue_full.clear()
                    continue

                await job_queue.join()
                output_queue.put_nowait(THE_END)
                await output_queue.join()
                break

            group = None

            if key is not None:
                group = key(item)

            job = Job(item=item, index=next(job_counter), group=group)

            try:
                buffer.put(job)
                continue
            except AssertionError:
                job_to_process = buffer.get()

                while job_to_process is None:

                    if job_queue.empty() and output_queue.empty():
                        await job_queue.join()
                    else:
                        await event_queue_full.wait()
                        event_queue_full.clear()

                    job_to_process = buffer.get()

                buffer.register_job(job_to_process)
                await job_queue.put(job_to_process)

                buffer.put(job)
                continue

        buffer.register_job(job)
        await job_queue.put(job)


async def generate_from_output_queue(output_queue, buffer):
    while True:
        job = await output_queue.get()
        output_queue.task_done()

        if job is THE_END:
            break

        buffer.unregister_job(job)

        yield job.result

        del job


def iterator_to_async_generator(iterable, func, max_workers=DEFAULT_MAX_WORKERS, key=None, parallelism=DEFAULT_PARALLELISM, buffer_size=DEFAULT_BUFFER_SIZE, throttle=DEFAULT_THROTTLE):

    validate_queue_kwargs(iterable=iterable, func=func, max_workers=max_workers, key=key, parallelism=parallelism, buffer_size=buffer_size, throttle=throttle)

    nb_workers = min(max_workers, len(iterable))
    iterable = iter(iterable)
    buffer = Buffer(buffer_size, parallelism, ThrottledGroup(throttle))
    job_queue = asyncio.Queue(maxsize=max_workers)

    async def get_generator():
        job_counter = count()
        event_queue_full = asyncio.Event()
        output_queue = asyncio.Queue()

        tasks = []

        for i in range(nb_workers):
            task = asyncio.create_task(worker(job_queue, output_queue, func, event_queue_full))
            tasks.append(task)

        task_job_queue = asyncio.create_task(iterable_to_queue(iterable, job_queue, output_queue, job_counter, buffer, event_queue_full, key))
        tasks.append(task_job_queue)

        async for i in generate_from_output_queue(output_queue, buffer):
            yield i

        await task_job_queue

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    return get_generator()
