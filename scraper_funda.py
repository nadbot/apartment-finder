import json
import os.path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scraper_base import ScraperBase

headers_funda = {
    'content-type': 'application/x-www-form-urlencoded',
    'path': '/huur/utrecht/0-1750/',
    'scheme': 'https',
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'origin': 'https://www.funda.nl',
    'referer': 'https://www.funda.nl/huur/utrecht/0-1750/',
    'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}


class ScraperFunda(ScraperBase):
    def __init__(self):
        baseurl = 'https://www.funda.nl'
        startpage = '/huur/utrecht/0-1750/'
        links_file = 'data/all_links_funda.txt'
        csv_file = 'data/apartment_data_funda.csv'
        super().__init__(headers_funda, baseurl, startpage, links_file, csv_file)

    def get_all_overview_pages(self, url, data):
        max_pages = int(
            data.find_all("div", {"class": "pagination-pages"})[0].find_all('a')[-1].get('data-pagination-page'))
        new_urls = [url + f'p{page}' for page in range(1, max_pages + 1)]
        return new_urls

    def get_all_apartments_per_page(self, data):
        apartments = data.find_all('div', {'class': 'search-result__header-title-col'})
        similar_links = data.find_all('span', {'class': 'search-result-similar-item__name'})
        apartments.extend(similar_links)
        links = set(self.BASE_URL + apartment.a.get('href') for apartment in apartments)
        return links

    def get_apartment_details(self, apartment_url, data):
        data_dict = {'address': data.find('span', {'class': 'object-header__title'}).text.strip(),
                     'postcode': data.find('span', {'class': 'object-header__subtitle'}).text.strip(),
                     'time': datetime.utcnow()
                     }
        # squaremeter = data.find('span', {'class': 'kenmerken-highlighted__value fd-text--nowrap'}).text.strip()
        datasheets = data.find_all('dl', {'class': 'object-kenmerken-list'})
        for datasheet in datasheets:
            a = datasheet.find_all(['dt', 'dd'])
            s = [i.text.strip() for i in a if len(i.find_all(['dt', 'dd'])) == 0]
            for index in range(0, len(s) - 1, 2):
                if 'Wat betekent dit' in s[index + 1]:
                    s[index + 1] = s[index + 1].split('Wat')[0].strip()
                data_dict[s[index]] = s[index + 1]
        data_dict['url'] = apartment_url
        return data_dict


if __name__ == '__main__':
    scraper = ScraperFunda()
    scraper.scrape(rerun_all=True)
