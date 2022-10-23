import json
import os.path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

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
FUNDA_BASE_URL = 'https://www.funda.nl'


def get_data(url, data_format=None):
    res = requests.get(url, headers=headers_funda)
    if res.status_code > 299:
        raise ConnectionError("Could not get data")
    if not data_format:
        return res.text
    if data_format == 'json':
        return json.loads(res.text)
    return BeautifulSoup(res.text, 'html.parser')


def get_all_overview_pages_funda(url, data):
    max_pages = int(
        data.find_all("div", {"class": "pagination-pages"})[0].find_all('a')[-1].get('data-pagination-page'))
    new_urls = [url + f'p{page}' for page in range(1, max_pages + 1)]
    return new_urls


def get_all_apartments_per_page(data):
    apartments = data.find_all('div', {'class': 'search-result__header-title-col'})
    similar_links = data.find_all('span', {'class': 'search-result-similar-item__name'})
    apartments.extend(similar_links)
    links = set(FUNDA_BASE_URL + apartment.a.get('href') for apartment in apartments)
    return links


def get_all_apartments_funda():
    url_funda = FUNDA_BASE_URL + '/huur/utrecht/0-1750/'
    data = get_data(url_funda, 'html')
    all_pages = get_all_overview_pages_funda(url_funda, data)
    all_apartments = []
    for page in all_pages:
        data = get_data(page, 'html')
        all_apartments.extend(get_all_apartments_per_page(data))
    with open('data/all_links_funda.txt', 'w') as f:
        f.write(','.join(all_apartments))
    return all_apartments


def get_apartment_details(url, data):
    data_dict = {'address': data.find('span', {'class': 'object-header__title'}).text.strip(),
                 'postcode': data.find('span', {'class': 'object-header__subtitle'}).text.strip()
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
    data_dict['url'] = url
    return data_dict


def get_all_apartment_data_funda(urls):
    all_data = []
    for apartment_url in tqdm(urls):
        data = get_data(apartment_url, 'html')
        try:
            data_dict = get_apartment_details(apartment_url, data)
        except Exception as e:
            print(f'Failed to retrieve data for {apartment_url}')
            continue
        all_data.append(data_dict)
    if len(all_data) == 0:
        return
    df = pd.DataFrame(all_data)
    if not os.path.isfile(csv_file := 'data/apartment_data_funda.csv'):
        df.to_csv(csv_file, index=False)
    else:
        df.to_csv(csv_file, mode='a', header=False, index=False)
    return df


def update_apartments(rerun_all=False):
    """If check_available, rescrape everything, otherwise just add the new ones."""
    with open('data/all_links_funda.txt', 'r') as f:
        all_apartments_old = set(f.read().split(','))
    all_apartments_now = set(get_all_apartments_funda())
    if rerun_all:
        return all_apartments_now
    missing_apartments = all_apartments_now - all_apartments_old
    return missing_apartments


def scrape_funda(rerun_all=False):
    apartments_to_scrape = update_apartments(rerun_all)
    print(f'Updating {len(apartments_to_scrape)} entries')
    df = get_all_apartment_data_funda(apartments_to_scrape)
    return df


if __name__ == '__main__':
    scrape_funda(rerun_all=False)
