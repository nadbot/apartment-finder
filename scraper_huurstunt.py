import json
import os.path

from tqdm import tqdm
from bs4 import BeautifulSoup
import pandas as pd
import requests

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
HUURSTUNT_BASE_URL = 'https://www.huurstunt.nl'


def post_data(url, page, data_format=None):
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

def get_data(url, data_format=None):
    payload = {}
    res = requests.get(url, headers=headers, data=payload)
    if not data_format:
        return res.text
    if data_format == 'json':
        return json.loads(res.text)
    return BeautifulSoup(res.text, 'html.parser')

def get_all_apartments_huurstunt():
    url = HUURSTUNT_BASE_URL + '/public/api/search'
    data = post_data(url, 1, 'json')
    legacyURL_end = '/huren/utrecht/0-1750/'
    all_apartments = [HUURSTUNT_BASE_URL+x['url'] for x in data['data']['rentals']]

    last_page = True
    # if wrong page number, the system will return the first page
    index = 2
    while last_page:
        data = post_data(url, index, 'json')
        legacyURL = data['data']['legacyURL']
        if legacyURL == legacyURL_end:
            break
        all_apartments.extend([HUURSTUNT_BASE_URL+x['url'] for x in data['data']['rentals']])
        index += 1

    with open('data/all_links_huurstunt.txt', 'w') as f:
        f.write(','.join(all_apartments))
    return all_apartments


def get_apartment_details(url, data):
    data_dict = {'address': data.find('h1', {'class': 'mb-1 title__listing'}).text.strip(),
                 'postcode': data.find('p', {'class': 'title__sub'}).text.strip()
                 }
    # squaremeter = data.find('span', {'class': 'kenmerken-highlighted__value fd-text--nowrap'}).text.strip()
    datasheets = data.find_all('div', {'class': 'rental-characteristics-long'})[0].find_all('div', {'class': ['info-wrapper__block', 'info-wrapper__block-extra']})
    for datasheet in datasheets:
        # key = datasheet.find_all('div', {'class': 'info-wrapper__key'})
        # value = datasheet.find_all('div', {'class': 'info-wrapper__value'})
        a = datasheet.find_all('div', {'class': ['info-wrapper__key', 'info-wrapper__value']})
        s = [i.text.strip() for i in a]
        for index in range(0, len(a) - 1, 2):
            key = a[index].text.strip()
            value = a[index+1].text.strip()
            if 'Balkon' in key:
                value = a[index+1].p.i.get('title')
            data_dict[key] = value
    data_dict['url'] = url
    return data_dict


def get_all_apartment_data_huurstunt(urls):
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
    if not os.path.isfile(csv_file := 'apartment_data_huurstunt.csv'):
        df.to_csv(csv_file, index=False)
    else:
        df.to_csv(csv_file, mode='a', header=False, index=False)


def update_apartments(rerun_all=False):
    """If check_available, rescrape everything, otherwise just add the new ones."""
    try:
        with open('data/all_links_huurstunt.txt', 'r') as f:
            all_apartments_old = set(f.read().split(','))
    except (FileNotFoundError, TypeError) as e:
        print(e)
        rerun_all = True
    all_apartments_now = set(get_all_apartments_huurstunt())
    if rerun_all:
        return all_apartments_now
    missing_apartments = all_apartments_now - all_apartments_old
    return missing_apartments


if __name__ == '__main__':
    apartments_to_scrape = update_apartments(True)
    print(f'Updating {len(apartments_to_scrape)} entries')
    get_all_apartment_data_huurstunt(apartments_to_scrape)
