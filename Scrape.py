import json
import os
import csv
import asyncio
import requests
from pymongo import MongoClient
from lxml import html
from time import sleep

excel_headers = ['Game name', 'Genre', 'Picture', 'Publisher',
                 'Date of release', 'Year of release', 'Platforms',
                 'Metascore', 'Description']
games_urls = list(set([line for line in open('Games/all_games.csv', 'r')]))
client = MongoClient('mongodb://jaktenstid:' + os.environ['PASSWORD'] + '@ds157158.mlab.com:57158/heroku_w4qsj3m6?authMechanism=SCRAM-SHA-1')
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

	async with ClientSession(headers={"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}) as session:
		for url in urls:
			task = asyncio.ensure_future(bound_fetch(sem, url, session))
			tasks.append(task)
		responses = asyncio.gather(*tasks)
		await responses


def get_item(page_content, url):
	try:
		document = html.fromstring(page_content)
		name = document.xpath("//h1[@class='product_title']//span[@itemprop='name']//text()")[-1].replace('\n','').strip()
		genre = document.xpath("//span[@itemprop='genre']//text()")[-1]
		img = document.xpath("//img[@class='product_image large_image']/@src")[-1]
		publisher = document.xpath("//span[@class='data']//span[@itemprop='name']//text()")[-1].strip()
		date = document.xpath("//span[@itemprop='datePublished']//text()")[-1]
		year = date.split(',')[-1]
		main_platform = document.xpath("//span[@itemprop='device']//text()")[-1].replace('\n','').strip()
		other_platforms = '/'.join(document.xpath("//li[@class='summary_detail product_platforms']//a//text()"))
		platforms = main_platform + '/' + other_platforms
		score = document.xpath("//span[@itemprop='ratingValue']//text()")[-1]
		desc = document.xpath("//span[@itemprop='description']//text()")[-1]
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

def scrape(urls):
	loop = asyncio.get_event_loop()
	future = asyncio.ensure_future(run(urls))
	loop.run_until_complete(future)

def main():
	scrape(games_urls)
	while to_scrape_again:
		to_scrape_again = list(set(to_scrape_again))
		sleep(60)
		scrape(to_scrape_again)

if __name__ == "__main__":
	main()