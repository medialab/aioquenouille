# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
#
# We have some warning during the tests

import pytest
import asyncio

from aioquenouille import imap


DATA_TEST = [3, 7, 3, 2, 6, 6]

DATA_TEST_2 = ["A", "A", "B", "B", "B", "B", "B", "C", "D", "D"]


async def sleeper(item):
    await asyncio.sleep(item / 10)
    return item


async def sleeper_constant(item):
    await asyncio.sleep(0.0001)
    return item


async def enumerated_sleeper(item):
    await asyncio.sleep(item[0] / 10)
    return item[0]


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


class TestImap(object):
    def test_arguments(self):
        with pytest.raises(TypeError):
            imap(None, sleeper)

        with pytest.raises(TypeError):
            imap(DATA_TEST, "test")

        with pytest.raises(TypeError):
            imap(DATA_TEST, not_async)

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, ordered="test")

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, max_workers="test")

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, max_workers=0)

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, key="test")

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, parallelism="test")

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, parallelism=-1)

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, buffer_size="test")

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, buffer_size=0)

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, throttle='test')

        with pytest.raises(TypeError):
            imap(DATA_TEST, sleeper, throttle=-4)

    @pytest.mark.asyncio
    async def test_basics(self):

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2)]

        assert len(results) == len(DATA_TEST)
        assert set(results) == set(DATA_TEST)

    @pytest.mark.asyncio
    async def test_none_iterator(self):
        iterable = [None] * 3

        results = [el async for el in imap(iterable, sleeper_constant, max_workers=2)]
        assert results == iterable

    @pytest.mark.asyncio
    async def test_less_jobs_than_tasks(self):

        results = [el async for el in imap(DATA_TEST[:2], sleeper, max_workers=2)]

        assert set(results) == set([3, 7])

    @pytest.mark.asyncio
    async def test_one_task(self):

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=1)]

        assert results == DATA_TEST

    @pytest.mark.asyncio
    async def test_one_item(self):

        results = [el async for el in imap(DATA_TEST[:1], sleeper, max_workers=2)]

        assert results == [3]

    @pytest.mark.asyncio
    async def test_empty(self):
        results = [el async for el in imap([], sleeper, max_workers=5)]

        assert results == []

    @pytest.mark.asyncio
    async def test_ordered(self):

        results = [el async for el in imap(DATA_TEST, sleeper, 2, ordered=True)]

        assert results == DATA_TEST

    @pytest.mark.asyncio
    async def test_group_parallelism(self):

        # Unordered
        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, parallelism=1, key=making_groups)]

        assert set(results) == set(DATA_TEST)

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, parallelism=1, key=making_groups, buffer_size=3)]

        assert set(results) == set(DATA_TEST)

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, parallelism=1, key=making_groups, buffer_size=1)]

        assert set(results) == set(DATA_TEST)

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, parallelism=3, key=making_groups, buffer_size=3)]

        assert set(results) == set(DATA_TEST)

        # Ordered
        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, ordered=True, parallelism=1, key=making_groups)]

        assert results == DATA_TEST

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, ordered=True, parallelism=1, key=making_groups, buffer_size=3)]

        assert results == DATA_TEST

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, ordered=True, parallelism=1, key=making_groups, buffer_size=1)]

        assert results == DATA_TEST

        results = [el async for el in imap(DATA_TEST, sleeper, max_workers=2, ordered=True, parallelism=3, key=making_groups, buffer_size=3)]

        assert results == DATA_TEST

    @pytest.mark.asyncio
    async def test_break(self):

        async for i in imap(enumerate(DATA_TEST), enumerated_sleeper, 5, ordered=True):
            if i == 2:
                break

        results = [el async for el in imap(DATA_TEST, sleeper, 2)]

        assert len(results) == len(DATA_TEST)
        assert set(results) == set(DATA_TEST)

    @pytest.mark.asyncio
    async def test_callable_parallelism(self):
        def per_group(g):
            if g == 'B':
                return 3
            else:
                return 1

        result = [el async for el in imap(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group, key=identity)]
        assert set(result) == set(DATA_TEST_2)

        def per_group_with_special(g):
            if g == 'B':
                return None

            return 1

        result = [el async for el in imap(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_with_special, key=identity)]
        assert set(result) == set(DATA_TEST_2)

        def per_group_raising(g):
            if g == 'B':
                raise RuntimeError

            return 1

        with pytest.raises(RuntimeError):
            result = [el async for el in imap(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_raising, key=identity)]

        def per_group_invalid(g):
            if g == 'B':
                return 'test'

            return 1

        with pytest.raises(TypeError):
            result = [el async for el in imap(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_invalid, key=identity)]

        def per_group_zero(g):
            if g == 'B':
                return 0

            return 1

        with pytest.raises(TypeError):
            result = [el async for el in imap(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_zero, key=identity)]

        def per_group_negative(g):
            if g == 'B':
                return -3

            return 1

        with pytest.raises(TypeError):
            result = [el async for el in imap(DATA_TEST_2, sleeper_constant, max_workers=4, parallelism=per_group_negative, key=identity)]

    @pytest.mark.asyncio
    async def test_throttle(self):

        def group(x):
            return 'SAME'

        nbs = set([el async for el in imap(range(10), sleeper_constant, 10, key=group, throttle=0.01)])
        assert nbs == set(range(10))

        nbs = set([el async for el in imap(range(10), sleeper_constant, 10, key=group, throttle=0.01, buffer_size=1)])
        assert nbs == set(range(10))

        nbs = set([el async for el in imap(range(10), sleeper_constant, 10, key=group, throttle=0.01, buffer_size=3)])
        assert nbs == set(range(10))

        results = [el async for el in imap(DATA_TEST, sleeper, 4, key=identity, throttle=0.01)]
        assert set(results) == set(DATA_TEST)

        nbs = [el async for el in imap(range(10), sleeper_constant, 10, ordered=True, key=group, throttle=0.01)]
        assert nbs == list(range(10))

        nbs = [el async for el in imap(range(10), sleeper_constant, 10, ordered=True, key=group, throttle=0.01, buffer_size=1)]
        assert nbs == list(range(10))

        nbs = [el async for el in imap(range(10), sleeper_constant, 10, ordered=True, key=group, throttle=0.01, buffer_size=3)]
        assert nbs == list(range(10))

        results = [el async for el in imap(DATA_TEST, sleeper, 4, ordered=True, key=identity, throttle=0.01)]
        assert results == DATA_TEST

    @pytest.mark.asyncio
    async def test_callable_throttle(self):

        def throttling(group, nb, result):
            assert nb == result

            if group == 'odd':
                return 0

            return 0.1

        def group(x):
            return 'even' if x % 2 == 0 else 'odd'

        nbs = set([el async for el in imap(range(10), sleeper_constant, 10, key=group, throttle=throttling)])

        assert nbs == set(range(10))

        def hellraiser(g, i, result):
            if i > 2:
                raise TypeError

            return 0.01

        with pytest.raises(TypeError):
            list([el async for el in imap(range(5), sleeper_constant, 4, key=group, throttle=hellraiser)])

        def wrong_type(g, i, result):
            return 'test'

        with pytest.raises(TypeError):
            list([el async for el in imap(range(5), sleeper_constant, 2, key=group, throttle=wrong_type)])

        def negative(g, i, result):
            return -30

        with pytest.raises(TypeError):
            list([el async for el in imap(range(5), sleeper_constant, 2, key=group, throttle=negative)])

    @pytest.mark.asyncio
    async def test_raise(self):
        async def hellraiser(i):
            await asyncio.sleep(0.0001)
            if i > 5:
                raise RuntimeError

            return i

        with pytest.raises(RuntimeError):
            async for i in imap(range(10), hellraiser, 1):
                pass

        with pytest.raises(RuntimeError):
            async for i in imap(range(6, 15), hellraiser, 4):
                pass

    @pytest.mark.asyncio
    async def test_key_raise(self):
        def group(i):
            if i > 2:
                raise RuntimeError

            return 'SAME'

        with pytest.raises(RuntimeError):
            [el async for el in imap(range(5), sleeper_constant, 2, key=group)]
