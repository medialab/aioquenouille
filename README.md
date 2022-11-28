[![Build Status](https://github.com/medialab/aioquenouille/workflows/Tests/badge.svg)](https://github.com/medialab/aioquenouille/actions)

# Aioquenouille

A library of async iterator workflows for python.

It is typically used to iterate over lazy streams without overflowing memory, all while respecting group parallelism constraints, e.g. when downloading massive amounts of urls from the web concurrently.