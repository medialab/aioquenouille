# =============================================================================
# Aioquenouille Various Utils
# =============================================================================
#
# Miscellaneous utility functions.
#

import asyncio


def clear(q):
    while True:
        try:
            q.get_nowait()
            q.task_done()
        except asyncio.QueueEmpty:
            break


def smash(q, v):
    clear(q)
    q.put_nowait(v)
