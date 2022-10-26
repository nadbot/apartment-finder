from datetime import datetime

from scraper_base import ScraperBase

headers_pararius = {
    # 'path': /apartments/utrecht/0-1750/page-3
    'scheme': 'https',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7',
    'cache-control': 'max-age=0',
    'cookie': '_ga=GA1.1.369655686.1666363971; latest_search_locations=%5B%22utrecht%22%5D; OptanonAlertBoxClosed=2022-10-21T14:53:36.645Z; eupubconsent-v2=CPhOvelPhOvelAcABBENCmCsAP_AAH_AAChQJFNf_X__b2_r-_5_f_t0eY1P9_7__-0zjhfdl-8N3f_X_L8X52M7vF36tq4KuR4ku3LBIUdlHOHcTUmw6okVryPsbk2cr7NKJ7PEmnMbOydYGH9_n1_z-ZKY7___f_7z_v-v___3____7-3f3__5___-__e_V__9zfn9_____9vP___9v-_9__________3_7997BIgAkw1biALsyxwZtowigRAjCsJDqBQAUUAwtEBhA6uCnZXAT6wgQAIBQBOBECHAFGDAIAABIAkIiAkCPBAIACIBAACABUIhAAxsAgsALAwCAAUA0LFGKAIQJCDIgIilMCAqRIKDeyoQSg_0NMIQ6ywAoNH_FQgI1kDFYEQkLByHBEgJeLJA8xRvkAIwQoBRKhWogAAA.f_gAD_gAAAAA; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Oct+21+2022+16%3A54%3A25+GMT%2B0200+(Central+European+Summer+Time)&version=6.27.0&isIABGlobal=false&hosts=&consentId=50540258-c004-40c9-94a1-4f8695ac9dae&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CSTACK42%3A1&AwaitingReconsent=false&geolocation=NL%3BZH; _ga_YCYTBWR959=GS1.1.1666363970.1.1.1666364086.0.0.0',
    'referer': 'https://www.pararius.com/english',
    'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': 'Windows',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
}


class ScraperPararius(ScraperBase):
    def __init__(self):
        header = headers_pararius
        baseurl = 'https://www.pararius.com'
        startpage = '/apartments/utrecht/0-1750/'
        links_file = 'data/all_links_pararius.txt'
        csv_file = 'data/apartment_data_pararius.csv'
        super().__init__(header, baseurl, startpage, links_file, csv_file)

    def get_all_overview_pages(self, url, data):
        max_pages = int(
            # -2 because last one is the link for next page, rsplit to split at
            data.find_all("ul", {"class": "pagination__list"})[0].find_all('a')[-2].get('href').split('page-')[1])
        new_urls = [url + f'page-{page}' for page in range(1, max_pages + 1)]
        return new_urls

    def get_all_apartments_per_page(self, data):
        apartments = data.find_all('h2', {'class': 'listing-search-item__title'})
        links = set(self.BASE_URL + apartment.a.get('href') for apartment in apartments)
        return links

    def get_apartment_details(self, apartment_url, data):
        data_dict = {
            'address': data.find('h1', {'class': 'listing-detail-summary__title'}).text.split('For rent:')[1].strip(),
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
    scraper = ScraperPararius()
    scraper.scrape(rerun_all=True)
