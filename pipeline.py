from collections import defaultdict

import numpy as np
import pandas as pd

from find_best import find_best_items
from preprocess_data import preprocess_funda, preprocess_pararius, preprocess_huurwoningen, preprocess_huurstunt
from scraper_funda import ScraperFunda
from scraper_huurstunt import ScraperHuurstunt
from scraper_huurwoningen import ScraperHuurwoningen
from scraper_pararius import ScraperPararius

energyclasses = defaultdict(int)
energyclasses['A+++'] = 1
energyclasses['A++'] = 2
energyclasses['A+'] = 3
energyclasses['A'] = 4
energyclasses['B'] = 5
energyclasses['C'] = 6
energyclasses['D'] = 7
energyclasses['E'] = 8
energyclasses['F'] = 9
energyclasses['G'] = 10
energyclasses_reversed = {a[1]: a[0] for a in energyclasses.items()}


def pipeline(threshold_good_apartment=2):
    rerun_all = save = False
    scraper_funda = ScraperFunda()
    df_funda = scraper_funda.scrape(rerun_all)
    scraper_pararius = ScraperPararius()
    df_pararius = scraper_pararius.scrape(rerun_all)
    scraper_huurwoningen = ScraperHuurwoningen()
    df_huurwoningen = scraper_huurwoningen.scrape(rerun_all)
    scraper_huurstunt = ScraperHuurstunt()
    df_huurstunt = scraper_huurstunt.scrape(rerun_all)
    all_changed = []
    if df_funda is not None:
        df_funda = preprocess_funda(df_funda, save=save)
        df_funda = df_funda.astype(
            dict.fromkeys(['rental_price', 'living_area', 'rooms', 'bedrooms', 'bathrooms'], 'float'))
        all_changed.append(df_funda)
    if df_pararius is not None:
        df_pararius = preprocess_pararius(df_pararius, save=save)
        df_pararius = df_pararius.astype(
            dict.fromkeys(['rental_price', 'living_area', 'rooms', 'bedrooms', 'bathrooms'], 'float'))
        all_changed.append(df_pararius)
    if df_huurwoningen is not None:
        df_huurwoningen = preprocess_huurwoningen(df_huurwoningen, save=save)
        df_huurwoningen = df_huurwoningen.astype(
            dict.fromkeys(['rental_price', 'living_area', 'rooms', 'bedrooms', 'bathrooms'], 'float'))
        all_changed.append(df_huurwoningen)
    if df_huurstunt is not None:
        df_huurstunt = preprocess_huurstunt(df_huurstunt, save=save)
        df_huurstunt = df_huurstunt.astype(
            dict.fromkeys(['rental_price', 'living_area', 'rooms', 'bedrooms', 'bathrooms'], 'float'))
        all_changed.append(df_huurstunt)
    if len(all_changed) == 0:
        return
    df_all = pd.concat(all_changed)
    df_all['postcode_beginning'] = df_all.postcode.str[0:4]
    df_all['price_pm'] = df_all['rental_price'] / df_all['living_area']
    df_all['energie_class'] = df_all['energielabel'].apply(lambda x: energyclasses[x]).replace(0, np.NaN)
    df_all_apartments = pd.read_csv('data/all_apartments_processed.csv')
    df_best_sorted = find_best_items(df_all, df_all_apartments, threshold_good_apartment=threshold_good_apartment)
    return df_best_sorted


if __name__ == '__main__':
    df = pipeline()
    print(df)
