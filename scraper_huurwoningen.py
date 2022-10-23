import json
import os.path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

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
HUURWONINGEN_BASE_URL = 'https://www.huurwoningen.nl'


def get_data(url, data_format=None):
    payload = {}
    res = requests.get(url, headers=headers, data=payload)
    if res.status_code > 299:
        raise ConnectionError("Could not get data")
    if not data_format:
        return res.text
    if data_format == 'json':
        return json.loads(res.text)
    return BeautifulSoup(res.text, 'html.parser')


def get_all_overview_pages_huurwoningen(url, data):
    max_pages = int(
        # -2 because last one is the link for next page, rsplit to split at
        data.find_all("ul", {"class": "pagination__list"})[0].find_all('a')[-2].get('href').split('page=')[1])
    new_urls = [url + f'&page={page}' for page in range(1, max_pages + 1)]
    return new_urls


def get_all_apartments_per_page(data):
    apartments = data.find_all('h2', {'class': 'listing-search-item__title'})
    links = set(HUURWONINGEN_BASE_URL + apartment.a.get('href') for apartment in apartments)
    return links


def get_all_apartments_huurwoningen():
    url = HUURWONINGEN_BASE_URL + '/in/utrecht/?price=0-1750'
    data = get_data(url, 'html')
    all_pages = get_all_overview_pages_huurwoningen(url, data)
    all_apartments = []
    for page in all_pages:
        data = get_data(page, 'html')
        all_apartments.extend(get_all_apartments_per_page(data))
    with open('data/all_links_huurwoningen.txt', 'w') as f:
        f.write(','.join(all_apartments))
    return all_apartments


def get_apartment_details(url, data):
    data_dict = {'address': data.find('h1', {'class': 'listing-detail-summary__title'}).text.strip(),
                 'postcode': data.find('div', {'class': 'listing-detail-summary__location'}).text.strip()
                 }
    # squaremeter = data.find('span', {'class': 'kenmerken-highlighted__value fd-text--nowrap'}).text.strip()
    datasheets = data.find_all('dl', {'class': 'listing-features__list'})
    for datasheet in datasheets:
        a = datasheet.find_all(['dt', 'dd'])
        s = [i.text.strip() for i in a if len(i.find_all(['dt', 'dd'])) == 0]
        for index in range(0, len(s) - 1, 2):
            data_dict[s[index]] = s[index + 1]
    data_dict['url'] = url
    return data_dict


def get_all_apartment_data_huurwoningen(urls):
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
    if not os.path.isfile(csv_file := 'data/apartment_data_huurwoningen.csv'):
        df.to_csv(csv_file, index=False)
    else:
        df.to_csv(csv_file, mode='a', header=False, index=False)
    return df


def update_apartments(rerun_all=False):
    """If check_available, rescrape everything, otherwise just add the new ones."""
    try:
        with open('data/all_links_huurwoningen.txt', 'r') as f:
            all_apartments_old = set(f.read().split(','))
    except (FileNotFoundError, TypeError) as e:
        print(e)
        rerun_all = True
    all_apartments_now = set(get_all_apartments_huurwoningen())
    if rerun_all:
        return all_apartments_now
    missing_apartments = all_apartments_now - all_apartments_old
    return missing_apartments


def scrape_huurwoningen(rerun_all=False):
    apartments_to_scrape = update_apartments(rerun_all)
    print(f'Updating {len(apartments_to_scrape)} entries')
    df = get_all_apartment_data_huurwoningen(apartments_to_scrape)
    return df


if __name__ == '__main__':
    scrape_huurwoningen(False)
