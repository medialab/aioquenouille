import time
import asyncio
from aioquenouille import imap


def key(i):
    print('Grouped', i)
    if i == 5:
        raise RuntimeError

    return 'A'


async def worker(i):
    await asyncio.sleep(1)
    return i

async def main():
    async for i in imap(range(10), worker, 3, key=key, parallelism=2):
        print('Got ', i)

asyncio.run(main())