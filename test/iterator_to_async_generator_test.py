# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
#
# We have some warning during the tests

import time
import pytest
import asyncio

from aioquenouille import iterator_to_async_generator


DATA_TEST = [3, 7, 3, 2, 6, 6]
TIME_TEST = [14, 7, 27, 3, 17, 17, 19, 14]

DATA_TEST_2 = ["A", "A", "B", "B", "B", "B", "B", "C", "D", "D"]


async def sleeper(item):
    await asyncio.sleep(item / 10)
    return item


async def sleeper_constant(item):
    await asyncio.sleep(0.0001)
    return item

async def sleeper_raise(item):
    await asyncio.sleep(0.0001)
    raise RuntimeError

def not_async(item):
    return item * 2


def making_groups(x):
    if x < 3:
        return "first"
    if x < 5:
        return "second"
    if x < 7:
        return "third"
    return "fourth"


def identity(x):
    return x


class TestIteratorToAsyncGenerator(object):
    def test_arguments(self):
        with pytest.raises(TypeError):
            iterator_to_async_generator(None, sleeper)

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, "test")

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, not_async)

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, sleeper, max_workers="test")

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, sleeper, max_workers=0)

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, sleeper, key="test")

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, sleeper, parallelism="test")

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, sleeper, parallelism=-1)

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, sleeper, buffer_size="test")

        with pytest.raises(TypeError):
            iterator_to_async_generator(DATA_TEST, sleeper, buffer_size=0)

    @pytest.mark.asyncio
    async def test_basics(self):

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST, sleeper, max_workers=2)]
        total_slept_for = time.monotonic() - started_at

        assert len(results) == len(DATA_TEST)
        assert set(results) == set(DATA_TEST)
        assert int(total_slept_for * 10) == TIME_TEST[0]

    @pytest.mark.asyncio
    async def test_none_iterator(self):
        iterable = [None] * 3

        results = [el async for el in iterator_to_async_generator(iterable, sleeper_constant, max_workers=2)]
        assert results == iterable

    @pytest.mark.asyncio
    async def test_less_jobs_than_tasks(self):

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST[:2], sleeper, max_workers=2)]
        total_slept_for = time.monotonic() - started_at

        assert set(results) == set([3, 7])
        assert int(total_slept_for * 10) == TIME_TEST[1]

    @pytest.mark.asyncio
    async def test_one_task(self):

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST, sleeper, max_workers=1)]
        total_slept_for = time.monotonic() - started_at

        assert len(results) == len(DATA_TEST)
        assert set(results) == set(DATA_TEST)
        assert int(total_slept_for * 10) == TIME_TEST[2]

    @pytest.mark.asyncio
    async def test_one_item(self):

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST[:1], sleeper, max_workers=2)]
        total_slept_for = time.monotonic() - started_at

        assert results == [3]
        assert int(total_slept_for * 10) == TIME_TEST[3]

    @pytest.mark.asyncio
    async def test_empty(self):
        results = [el async for el in iterator_to_async_generator([], sleeper, max_workers=5)]

        assert results == []

    @pytest.mark.asyncio
    async def test_group_parallelism(self):

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST, sleeper, max_workers=2, parallelism=1, key=making_groups)]
        total_slept_for = time.monotonic() - started_at

        assert set(results) == set(DATA_TEST)
        assert int(total_slept_for * 10) == TIME_TEST[4]

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST, sleeper, max_workers=2, parallelism=1, key=making_groups, buffer_size=3)]
        total_slept_for = time.monotonic() - started_at

        assert set(results) == set(DATA_TEST)
        assert int(total_slept_for * 10) == TIME_TEST[5]

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST, sleeper, max_workers=2, parallelism=1, key=making_groups, buffer_size=1)]
        total_slept_for = time.monotonic() - started_at

        assert set(results) == set(DATA_TEST)
        assert int(total_slept_for * 10) == TIME_TEST[6]

        started_at = time.monotonic()
        results = [el async for el in iterator_to_async_generator(DATA_TEST, sleeper, max_workers=2, parallelism=3, key=making_groups, buffer_size=3)]
        total_slept_for = time.monotonic() - started_at

        assert set(results) == set(DATA_TEST)
        assert int(total_slept_for * 10) == TIME_TEST[7]

    @pytest.mark.asyncio
    async def test_callable_parallelism(self):
        def per_group(g):
            if g == 'B':
                return 3
            else:
                return 1

        result = [el async for el in iterator_to_async_generator(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group, key=identity)]
        assert set(result) == set(DATA_TEST_2)

        def per_group_with_special(g):
            if g == 'B':
                return None

            return 1

        result = [el async for el in iterator_to_async_generator(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_with_special, key=identity)]
        assert set(result) == set(DATA_TEST_2)

        # def per_group_raising(g):
        #     if g == 'B':
        #         raise RuntimeError

        #     return 1

        # with pytest.raises(RuntimeError):
        #     result = [el async for el in iterator_to_async_generator(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_raising, key=identity)]

        # def per_group_invalid(g):
        #     if g == 'B':
        #         return 'test'

        #     return 1

        # with pytest.raises(TypeError):
        #     result = [el async for el in iterator_to_async_generator(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_invalid, key=identity)]

        # def per_group_zero(g):
        #     if g == 'B':
        #         return 0

        #     return 1

        # with pytest.raises(TypeError):
        #     result = [el async for el in iterator_to_async_generator(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_zero, key=identity)]

        # def per_group_negative(g):
        #     if g == 'B':
        #         return -3

        #     return 1

        # with pytest.raises(TypeError):
        #     result = [el async for el in iterator_to_async_generator(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_negative, key=identity)]
