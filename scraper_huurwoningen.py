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
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
    'Cookie': 'latest_search_locations=%5B%22utrecht%22%5D'
}

class ScraperHuurwoningen(ScraperBase):
    def __init__(self):
        baseurl = 'https://www.huurwoningen.nl'
        startpage = '/in/utrecht/?price=0-1750'
        links_file = 'data/all_links_huurwoningen.txt'
        csv_file = 'data/apartment_data_huurwoningen.csv'
        super().__init__(headers, baseurl, startpage, links_file, csv_file)

    def get_all_overview_pages(self, url, data):
        max_pages = int(
            # -2 because last one is the link for next page, rsplit to split at
            data.find_all("ul", {"class": "pagination__list"})[0].find_all('a')[-2].get('href').split('page=')[1])
        new_urls = [url + f'&page={page}' for page in range(1, max_pages + 1)]
        return new_urls

    def get_all_apartments_per_page(self, data):
        apartments = data.find_all('h2', {'class': 'listing-search-item__title'})
        links = set(self.BASE_URL + apartment.a.get('href') for apartment in apartments)
        return links

    def get_apartment_details(self, apartment_url, data):
        data_dict = {'address': data.find('h1', {'class': 'listing-detail-summary__title'}).text.strip(),
                     'postcode': data.find('div', {'class': 'listing-detail-summary__location'}).text.strip(),
                     'time': datetime.utcnow()
                     }
        # squaremeter = data.find('span', {'class': 'kenmerken-highlighted__value fd-text--nowrap'}).text.strip()
        datasheets = data.find_all('dl', {'class': 'listing-features__list'})
        for datasheet in datasheets:
            a = datasheet.find_all(['dt', 'dd'])
            s = [i.text.strip() for i in a if len(i.find_all(['dt', 'dd'])) == 0]
            for index in range(0, len(s) - 1, 2):
                data_dict[s[index]] = s[index + 1]
        data_dict['url'] = apartment_url
        return data_dict


if __name__ == '__main__':
    scraper = ScraperHuurwoningen()
    scraper.scrape(rerun_all=True)
