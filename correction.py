from Scrape import collection
import csv
def get_all():
    return [item for item in collection.find({})]

def main():
    all = get_all()
    new = []
    names = []
    for item in all:
        score = int(item['score']) if item['score'] and item['score'].isdigit() else 0
        item['priority'] = 2 if score > 84 else 0
    for item in all:
        if item['name'] not in names:
            if item['platforms'] and item['platforms'].endswith('/'):
                item['platforms'] = item['platforms'][:-1]
            item['url'] = item['url'].replace('http://www.metacritic.com//', 'http://www.metacritic.com/')
            names.append(item['name'])
            new.append(item)

    print(new)
    with open('result.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Name','Description','Genre','Img','Platforms','Publisher','Metascore','Date of release', 'Year of release', 'Priority','Url'])
        for item in new:
            writer.writerow([item['name'], item['desc'],
                             item['genre'], item['img'],
                             item['platforms'], item['publisher'],
                             item['score'], item['date'],
                             item['year'], item['priority'],
                             item['url']])


if __name__ == '__main__':
    main()