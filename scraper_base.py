import json
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


class ScraperBase:
    def __init__(self, header, baseurl, startpage, links_file, csv_file):
        self.header = header
        self.BASE_URL = baseurl
        self.url = self.BASE_URL + startpage
        self.links_file = links_file
        self.csv_file = csv_file

    def get_data(self, url, data_format=None):
        res = requests.get(url, headers=self.header)
        if res.status_code > 299:
            raise ConnectionError("Could not get data")
        if not data_format:
            return res.text
        if data_format == 'json':
            return json.loads(res.text)
        return BeautifulSoup(res.text, 'html.parser')

    def get_all_overview_pages(self, url, data):
        return []

    def get_all_apartments_per_page(self, data):
        return []

    def get_all_apartments(self):
        data = self.get_data(self.url, 'html')
        all_pages = self.get_all_overview_pages(self.url, data)
        all_apartments = []
        for page in all_pages:
            data = self.get_data(page, 'html')
            all_apartments.extend(self.get_all_apartments_per_page(data))
        with open(self.links_file, 'w') as f:
            f.write(','.join(all_apartments))
        return all_apartments

    def get_apartment_details(self, apartment_url, data):  # TODO
        return {}

    def get_all_apartment_data(self, urls, rerun_all=False):
        all_data = []
        for apartment_url in tqdm(urls):
            data = self.get_data(apartment_url, 'html')
            try:
                data_dict = self.get_apartment_details(apartment_url, data)
            except Exception as e:
                print(f'Failed to retrieve data for {apartment_url}')
                continue
            all_data.append(data_dict)
        if len(all_data) == 0:
            return
        df = pd.DataFrame(all_data)
        if rerun_all or not os.path.isfile(self.csv_file):
            df.to_csv(self.csv_file, index=False)
        else:
            df.to_csv(self.csv_file, mode='a', header=False, index=False)
        return df

    def update_apartments(self, rerun_all=False):
        """If check_available, rescrape everything, otherwise just add the new ones."""
        try:
            with open(self.links_file, 'r') as f:
                all_apartments_old = set(f.read().split(','))
        except (FileNotFoundError, TypeError) as e:
            print(e)
            rerun_all = True
        all_apartments_now = set(self.get_all_apartments())
        if rerun_all:
            return all_apartments_now
        missing_apartments = all_apartments_now - all_apartments_old
        return missing_apartments

    def scrape(self, rerun_all=False):
        apartments_to_scrape = self.update_apartments(rerun_all)
        print(f'Updating {len(apartments_to_scrape)} entries')
        df = self.get_all_apartment_data(apartments_to_scrape, rerun_all)
        return df
