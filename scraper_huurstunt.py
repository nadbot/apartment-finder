import json
import os.path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scraper_base import ScraperBase

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate',
    'accept-language': 'en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7',
    'cache-control': 'max-age=0',
    'referer': 'https://www.huurwoningen.nl/content/expats/',
    'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
}


class ScraperHuurstunt(ScraperBase):
    def __init__(self):
        header = headers
        baseurl = 'https://www.huurstunt.nl'
        startpage = '/public/api/search'
        links_file = 'data/all_links_huurstunt.txt'
        csv_file = 'data/apartment_data_huurstunt.csv'
        super().__init__(header, baseurl, startpage, links_file, csv_file)

    def post_data(self, url, page, data_format=None):
        payload = json.dumps({
            "force": False,
            "location": {
                "location": "Utrecht",
                "distance": None,
                "suggestType": "city",
                "suggestId": 439,
                "neighborhoodSlug": None,
                "streetSlug": None,
                "districtSlug": None
            },
            "price": {
                "from": 0,
                "till": 1750
            },
            "properties": {
                "rooms": 0,
                "livingArea": 0,
                "deliveryLevel": None,
                "rentalType": None,
                "outside": []
            },
            "page": page,
            "sorting": "datum-af",
            "resultsPerPage": 21
        })
        res = requests.post(url, headers=headers, data=payload)
        if res.status_code > 299:
            raise ConnectionError("Could not get data")
        if not data_format:
            return res.text
        if data_format == 'json':
            return json.loads(res.text)
        return BeautifulSoup(res.text, 'html.parser')

    def get_data(self, url, data_format=None):
        """Overwrite supermethod because this one can return 404"""
        payload = {}
        res = requests.get(url, headers=headers, data=payload)
        if not data_format:
            return res.text
        if data_format == 'json':
            return json.loads(res.text)
        return BeautifulSoup(res.text, 'html.parser')

    def get_all_apartments(self):
        url = self.url
        data = self.post_data(url, 1, 'json')
        legacyURL_end = '/huren/utrecht/0-1750/'
        all_apartments = [self.BASE_URL + x['url'] for x in data['data']['rentals']]

        last_page = True
        # if wrong page number, the system will return the first page
        index = 2
        while last_page:
            data = self.post_data(url, index, 'json')
            legacyURL = data['data']['legacyURL']
            if legacyURL == legacyURL_end:
                break
            all_apartments.extend([self.BASE_URL + x['url'] for x in data['data']['rentals']])
            index += 1

        with open(self.links_file, 'w') as f:
            f.write(','.join(all_apartments))
        return all_apartments

    def get_apartment_details(self, url, data):
        data_dict = {'address': data.find('h1', {'class': 'mb-1 title__listing'}).text.strip(),
                     'postcode': data.find('p', {'class': 'title__sub'}).text.strip(),
                     'time': datetime.utcnow()
                     }
        # squaremeter = data.find('span', {'class': 'kenmerken-highlighted__value fd-text--nowrap'}).text.strip()
        datasheets = data.find_all('div', {'class': 'rental-characteristics-long'})[0].find_all('div', {
            'class': ['info-wrapper__block', 'info-wrapper__block-extra']})
        for datasheet in datasheets:
            # key = datasheet.find_all('div', {'class': 'info-wrapper__key'})
            # value = datasheet.find_all('div', {'class': 'info-wrapper__value'})
            a = datasheet.find_all('div', {'class': ['info-wrapper__key', 'info-wrapper__value']})
            s = [i.text.strip() for i in a]
            for index in range(0, len(a) - 1, 2):
                key = a[index].text.strip()
                value = a[index + 1].text.strip()
                if 'Balkon' in key:
                    value = a[index + 1].p.i.get('title')
                data_dict[key] = value
        data_dict['url'] = url
        return data_dict


if __name__ == '__main__':
    scraper = ScraperHuurstunt()
    scraper.scrape(rerun_all=True)
