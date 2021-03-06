import csv
import requests
from lxml import html
import json
from multiprocessing import Pool
from time import sleep
import random

domain = 'http://www.metacritic.com/'
SLOW_DOWN = False

def get_html(url):
    headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
    global SLOW_DOWN
    try:
        if SLOW_DOWN:
            sleep(15)
            SLOW_DOWN = False
        html = requests.get(url, headers=headers).content.decode('utf-8')
        if '429 Slow down' in html:
            SLOW_DOWN = True
            print(' - - - SLOW DOWN')
            raise TimeoutError
        return html
    except TimeoutError:
        return get_html(url)


def get_pages(genre):
    with open('Games/' + genre.split('/')[-2] + '.csv', 'w') as file:
        writer = csv.writer(file, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        genre_page_sceleton = genre + '&page=%s'
        def scrape():
            page_content = get_html(genre)
            document = html.fromstring(page_content)

            try:
                last_page_number = int(document.xpath("//li[@class='page last_page']/a/text()")[0])
                pages = [genre] + [genre_page_sceleton % str(i) for i in range(1, last_page_number)]
                for page in pages:
                    document = html.fromstring(get_html(page))
                    games = [domain + url for url in document.xpath("//ol[@class='list_products list_product_summaries']//h3[@class='product_title']/a/@href")]
                    print('Page: ' + page + " - - - Games: " + str(len(games)))
                    for game in games:
                        writer.writerow([game])
            except:
                scrape()
        scrape()



if __name__ == "__main__":
    dict = json.load(open('genres.json', 'r'))
    p = Pool(4)
    p.map(get_pages, [dict[key] for key in dict.keys()])
    print('Over')
