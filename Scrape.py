import json
import csv
import asyncio
import requests
import urllib3
import lxml

excel_headers = ['Game name', 'Genre', 'Publisher',
                 'Date of release', 'Year of release', 'Platforms',
                 'Metascore', 'Description']
genres_pages = json.load(open('genres.json', 'r').read())

from aiohttp import ClientSession

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(url, session)


async def run(urls, fh):
    tasks = []
    sem = asyncio.Semaphore(1000)

    async with ClientSession() as session:
        for url in urls:
            task = asyncio.ensure_future(bound_fetch(sem, url.format(i), session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses

number = 10000


def main():
    games_pages = []
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(urls, fh))
    loop.run_until_complete(future)

if __name__ == "__main__":
    main()