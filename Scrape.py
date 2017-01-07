import json
import os
import csv
import asyncio
import requests
from pymongo import MongoClient
from lxml import html
from time import sleep
from collections import Counter

excel_headers = ['Game name', 'Genre', 'Picture', 'Publisher',
                 'Date of release', 'Year of release', 'Platforms',
                 'Metascore', 'Description']
games_urls = list(set([line for line in open('Games/all_games.csv', 'r')]))
client = MongoClient(
    'mongodb://jaktenstid:''@ds157158.mlab.com:57158/heroku_w4qsj3m6?authMechanism=SCRAM-SHA-1')
database = client.heroku_w4qsj3m6
collection = database['games']

total_checked = 0
to_scrape_again = []
from aiohttp import ClientSession


async def fetch(url, session):
    global total_checked
    async with session.get(url) as response:
        page_content = await response.read()
        item = get_item(page_content, url)
        if not item:
            raise Exception(' - - - ITEM IS NONE')
        if '429 Slow down' in page_content.decode('utf-8'):
            raise Exception(' - - - SLOWING DOWN')
        collection.insert_one(item)
        total_checked += 1
        print('Inserted: ' + url + '  - - - Total checked: ' + str(total_checked))
        return 0


async def bound_fetch(sem, url, session):
    try:
        async with sem:
            await fetch(url, session)
    except Exception as e:
        to_scrape_again.append(url)
        print(e)
        sleep(30)


async def run(urls):
    tasks = []
    sem = asyncio.Semaphore(50)

    async with ClientSession(
            headers={"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}) as session:
        for url in urls:
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses


def get_item(page_content, url):
    def get(item):
        if item:
            return item[-1]
        return None

    try:
        document = html.fromstring(page_content)
        name = get(document.xpath("//h1[@class='product_title']//span[@itemprop='name']//text()"))
        if name:
            name = name.replace('\n', '').strip()
        genre = get(document.xpath("//span[@itemprop='genre']//text()"))
        img = get(document.xpath("//img[@class='product_image large_image']/@src"))
        publisher = get(document.xpath("//span[@class='data']//span[@itemprop='name']//text()"))
        if publisher:
            publisher = publisher.strip()
        date = get(document.xpath("//span[@itemprop='datePublished']//text()"))
        year = None
        if date:
            year = date.split(',')[-1]
        main_platform = get(document.xpath("//span[@itemprop='device']//text()"))
        if main_platform:
            main_platform = main_platform.replace('\n', '').strip()
        else:
            main_platform = ''
        other_platforms = '/'.join(document.xpath("//li[@class='summary_detail product_platforms']//a//text()"))
        platforms = main_platform + '/' + other_platforms
        score = get(document.xpath("//span[@itemprop='ratingValue']//text()"))
        desc = get(document.xpath("//span[@itemprop='description']//text()"))
        if url in to_scrape_again:
            del to_scrape_again[to_scrape_again.index(url)]
        return {'url': url,
                'name': name,
                'genre': genre,
                'img': img,
                'publisher': publisher,
                'date': date,
                'year': year,
                'platforms': platforms,
                'score': score,
                'desc': desc}
    except IndexError:
        print(' - - - RETURNED NONE AT ' + url)
        return None


def main():
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(games_urls))
    loop.run_until_complete(future)
    print('Over')


if __name__ == "__main__":
    d = [item for item in collection.find({})]
    print(d)
