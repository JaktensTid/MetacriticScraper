import random
import asyncio
import requests
import urllib3
import lxml

main_page = 'http://www.metacritic.com/browse/games/genre/metascore/action/all?view=detailed'

genres_pages = {
    
}

from aiohttp import ClientSession

async def fetch(url, session):
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        return await response.read()


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(url, session)


async def run(urls):
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
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(urls))
    loop.run_until_complete(future)

if __name__ == "__main__":
    main()