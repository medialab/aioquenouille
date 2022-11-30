# =============================================================================
# Aioquenouille Exception Testing
# =============================================================================
#
# Testing what happens when exceptions are thrown.
#
import asyncio
from aioquenouille import imap

async def crasher(i):
    await asyncio.sleep(0.0001)
    if i > 7:
        raise Exception('Die!')
    return i


async def main():
    async for result in imap(range(15), crasher, 3):
        print(result)

asyncio.run(main())