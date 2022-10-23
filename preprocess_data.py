from collections import defaultdict

import numpy as np
import pandas as pd


def preprocess_funda(df=None, save=False):
    if df is None:
        df = pd.read_csv('data/apartment_data_funda.csv')
    columns_needed = ['postcode', 'Huurprijs', 'Wonen', 'Oppervlakte', 'Inhoud', 'Aantal kamers', 'Aantal badkamers',
                      'Energielabel', 'Voorlopig energielabel', 'Balkon/dakterras', 'Tuin', 'Achtertuin']
    for column in columns_needed:
        if column not in df:
            df[column] = None
    df.postcode = df.postcode.str.replace(' Utrecht', '')  # Remove "Utrecht" from postcode
    # df['rental_price'] = df.Huurprijs.str.extract(r'([0-9.]+)')  # only get the first price
    costs = df.Huurprijs.str.extractall(
        r'([0-9.,]+)')  # will return up to 2 matches, first is rental price, second servicecosts
    rental_price = costs[costs.index.get_level_values(1) == 0]
    # TODO find better way to set these?
    df.loc[df.index.isin(rental_price.index.get_level_values(0)), 'rental_price'] = rental_price.values
    df['rental_price'] = df['rental_price'].str.replace(r'[.,]', '')
    service_costs = costs[costs.index.get_level_values(1) == 1]
    df.loc[df.index.isin(service_costs.index.get_level_values(0)), 'service_costs'] = service_costs.values
    df['service_costs'] = df['service_costs'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['living_area'] = df.Wonen.str.replace(' m²', '')
    df['living_area'] = df['living_area'].where(df['living_area'] == df['living_area'],
                                                df['Oppervlakte'].str.replace(' m² wonen', ''))
    df['Inhoud'] = df.Inhoud.str.replace(' m²', '')
    df[['rooms', 'bedrooms']] = df['Aantal kamers'].str.split('(', expand=True)
    df['rooms'] = df['rooms'].str.extract(r'([0-9.,]+)')
    df['bedrooms'] = df['bedrooms'].str.replace(')', '').str.extract(r'([0-9.,]+)')
    df['badkamer'] = df['Aantal badkamers'].str.extract(r'([0-9.,]+)')
    if df['Aantal badkamers'].str.contains('en').any():
        df[['badkamer', 'toilet']] = df['Aantal badkamers'].str.split('en', expand=True)
        # count separate toilet as extra bathroom, pretty sure pararius does it as well
        df['bathrooms'] = df['badkamer'].str.extract(r'([0-9.,]+)').astype(float) + df['toilet'].str.extract(
            r'([0-9.,]+)').astype(float)
    else:
        df['bathrooms'] = df['badkamer']
    if df['Energielabel'].str.contains('\r').any():
        df[['energielabel', 'Energierating']] = df['Energielabel'].str.split('\r', expand=True)
        df['Energierating'] = df['Energierating'].str.strip()
    else:
        df['energielabel'] = df['Energielabel']
    # set NaN and None to the voorlopig energielabel
    df['energielabel'] = df['energielabel'].where(df['energielabel'] == df['energielabel'],
                                                  df['Voorlopig energielabel'])
    df['balcony'] = df['Balkon/dakterras'].isin(['Dakterras aanwezig', 'Balkon aanwezig'])
    df['garden'] = df['Tuin'].isin(['Zonneterras', 'Achtertuin'])
    df['garden_size'] = df['Achtertuin'].str.extract(r'([0-9.,]+)')
    if save:
        df.to_csv('preprocessed_all_data_funda.csv')
    keep_cols = ['address', 'postcode', 'rental_price', 'service_costs', 'living_area', 'rooms', 'bedrooms',
                 'bathrooms', 'balcony', 'garden', 'energielabel', 'url', 'garden_size']
    df = df.loc[:, keep_cols]
    if save:
        df.to_csv('preprocessed_funda.csv')
    return df


def preprocess_pararius(df=None, save=False):
    if df is None:
        df = pd.read_csv('data/apartment_data_pararius.csv')
    columns_needed = ['postcode', 'Rental price', 'Service costs', 'Living area', 'Number of rooms',
                      'Number of bedrooms', 'Number of bathrooms', 'Energy rating', 'Balcony', 'Garden', 'Deposit',
                      'Rental agreement', 'Income requirement', 'Maximum number of residents', 'Duration']
    for column in columns_needed:
        if column not in df:
            df[column] = None
    df[['postcode', 'region']] = df.postcode.str.split('(', expand=True)
    df['region'] = df['region'].str.replace(')', '')
    df['rental_price'] = df['Rental price'].str.extract(r'([0-9.,]+)')
    df['rental_price'] = df['rental_price'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['service_costs'] = df['Service costs'].str.extract(r'([0-9.,]+)')
    df['service_costs'] = df['service_costs'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['living_area'] = df['Living area'].str.replace(' m²', '')
    df['rooms'] = df['Number of rooms']
    df['bedrooms'] = df['Number of bedrooms']
    df['bathrooms'] = df['Number of bathrooms']
    df['energielabel'] = df['Energy rating']
    df['balcony'] = df.Balcony == 'Present'
    # df[['Garden', 'garden_size']] = df['Garden'].str.split(' \(', expand=True)
    df['garden'] = 'Present' in df['Garden']
    df['garden_size'] = df['Garden'].str.extract(r'([0-9.,]+)')
    df['Deposit'] = df['Deposit'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['Income requirement'] = df['Income requirement'].str.replace(r'[.,]', '')  # convert the comma value to .
    if save:
        df.to_csv('preprocessed_all_data_pararius.csv')
    keep_cols = ['address', 'postcode', 'rental_price', 'service_costs', 'living_area', 'rooms', 'bedrooms',
                 'bathrooms', 'balcony', 'garden', 'energielabel', 'url', 'garden_size',
                 # additional fields
                 'Rental agreement', 'Deposit', 'Income requirement', 'Maximum number of residents', 'Duration']

    df = df.loc[:, keep_cols]
    if save:
        df.to_csv('preprocessed_pararius.csv')
    return df


def preprocess_huurwoningen(df=None, save=False):
    if df is None:
        df = pd.read_csv('data/apartment_data_huurwoningen.csv')
    columns_needed = ['postcode', 'Huurprijs', 'Servicekosten', 'Woonoppervlakte', 'Aantal kamers',
                      'Aantal slaapkamers', 'Aantal badkamers', 'Balkon', 'Tuin', 'Borg', 'Looptijd']
    for column in columns_needed:
        if column not in df:
            df[column] = None
    df[['postcode', 'region']] = df.postcode.str.split('(', expand=True)
    df['region'] = df['region'].str.replace(')', '')
    df['rental_price'] = df['Huurprijs'].str.extract(r'([0-9.,]+)')
    df['rental_price'] = df['rental_price'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['service_costs'] = df['Servicekosten'].str.extract(r'([0-9.,]+)')
    df['service_costs'] = df['service_costs'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['living_area'] = df['Woonoppervlakte'].str.replace(' m²', '')
    df['rooms'] = df['Aantal kamers']
    df['bedrooms'] = df['Aantal slaapkamers']
    df['bathrooms'] = df['Aantal badkamers']
    df['energielabel'] = None
    df['balcony'] = df.Balkon == 'Aanwezig'
    # df[['Garden', 'garden_size']] = df['Tuin'].str.split(' \(', expand=True)
    df['garden_size'] = df['Tuin'].str.extract(r'([0-9.,]+)')
    df['garden'] = 'Aanwezig' in df['Tuin']
    df['Deposit'] = df['Borg'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['Duration'] = df['Looptijd']
    if save:
        df.to_csv('preprocessed_all_data_huurwoningen.csv')
    keep_cols = ['address', 'postcode', 'rental_price', 'service_costs', 'living_area', 'rooms', 'bedrooms',
                 'bathrooms', 'balcony', 'garden', 'energielabel', 'url', 'garden_size',
                 # additional fields
                 'Deposit', 'Duration']

    df = df.loc[:, keep_cols]
    if save:
        df.to_csv('preprocessed_huurwoningen.csv')
    return df


def preprocess_huurstunt(df=None, save=False):
    if df is None:
        df = pd.read_csv('data/apartment_data_huurstunt.csv')
    columns_needed = ['postcode', 'Wijk', 'Laatst bekende huurprijs:', 'Servicekosten:', 'Woning oppervlakte:',
                      'Aantal kamers:', 'Aantal slaapkamers:', 'Aantal badkamers:', 'Energielabel:',
                      'Voorlopig energielabel:', 'Aantal personen:', 'Balkon:', 'Tuin:', 'Waarborgsom:']
    for column in columns_needed:
        if column not in df:
            df[column] = None
    df[['postcode', 'utrecht']] = df.postcode.str.split('\n', 1, expand=True)
    df['region'] = df['Wijk:']
    df['rental_price'] = df['Laatst bekende huurprijs:'].str.extract(r'([0-9.,]+)')
    df['rental_price'] = df['rental_price'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['service_costs'] = df['Servicekosten:'].str.extract(r'([0-9.,]+)')
    df['service_costs'] = df['service_costs'].str.replace(r'[.,]', '')  # convert the comma value to .
    df['living_area'] = df['Woning oppervlakte:'].str.replace(' m2', '')
    df['rooms'] = df['Aantal kamers:'].str.extract(r'([0-9.,]+)')
    df['bedrooms'] = df['Aantal slaapkamers:']
    df['bathrooms'] = df['Aantal badkamers:']
    df['energielabel'] = df['Energielabel:'].where(df['Energielabel:'] == df['Energielabel:'],
                                                   df['Voorlopig energielabel:'])
    df['Maximum number of residents'] = df['Aantal personen:']
    df['balcony'] = df['Balkon:'] == df['Balkon:']  # any value is ok
    # df[['Garden', 'garden_size']] = df['Tuin:'].str.split(' \(', expand=True)
    df['garden_size'] = df['Tuin:'].str.extract(r'([0-9.,]+)')
    df['garden'] = df['Tuin:'] == df['Tuin:']  # any value is ok
    df['Deposit'] = df['Waarborgsom:'].str.extract(r'([0-9.,]+)').replace(r'[.,]', '')
    # df['Duration'] = df['Looptijd']
    if save:
        df.to_csv('preprocessed_all_data_huurstunt.csv')
    keep_cols = ['address', 'postcode', 'rental_price', 'service_costs', 'living_area', 'rooms', 'bedrooms',
                 'bathrooms', 'balcony', 'garden', 'energielabel', 'url', 'garden_size',
                 # additional fields
                 'Deposit']

    df = df.loc[:, keep_cols]
    df = df.loc[df['living_area'].astype(int) > 0]
    if save:
        df.to_csv('preprocessed_huurstunt.csv')
    return df


def merge_apartments(df_funda, df_pararius, df_huurwoningen, df_huurstunt, save=False):
    if df_funda is None:
        df_funda = pd.read_csv('data/preprocessed_funda.csv')
    if df_pararius is None:
        df_pararius = pd.read_csv('data/preprocessed_pararius.csv')
    if df_huurwoningen is None:
        df_huurwoningen = pd.read_csv('data/preprocessed_huurwoningen.csv')
    if df_huurstunt is None:
        df_huurstunt = pd.read_csv('data/preprocessed_huurstunt.csv')
    df_all = pd.concat([df_funda, df_pararius, df_huurwoningen, df_huurstunt])
    df_all = df_all.drop(columns=['Unnamed: 0'], axis=1)
    if save:
        df_all.to_csv('data/all_apartments.csv', index=False)
    return df_all


def get_average_data(df_data, save=False):
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
    if df_data is None:
        df_data = pd.read_csv('data/all_apartments.csv')
    df_data['postcode_beginning'] = df_data.postcode.str[0:4]
    df_data['price_pm'] = df_data['rental_price'] / df_data['living_area']
    df_data['energie_class'] = df_data['energielabel'].apply(lambda x: energyclasses[x]).replace(0, np.NaN)
    if save:
        df_data.to_csv('all_apartments_processed.csv')
    averages = df_data.groupby('postcode_beginning').aggregate(
        {**dict.fromkeys(['price_pm', 'living_area', 'bedrooms', 'rooms', 'energie_class'], 'mean'),
         'address': 'count'}).reset_index()
    averages = averages.rename(columns={'address': 'count'})
    averages['energie_class'] = averages['energie_class'].round().map(energyclasses_reversed)
    if save:
        averages.to_csv('data/avg_values.csv')
    return averages


def preprocess_all(save=False):
    df_funda = preprocess_funda(save=save)
    df_pararius = preprocess_pararius(save=save)
    df_huurwoningen = preprocess_huurwoningen(save=save)
    df_huurstunt = preprocess_huurstunt(save=save)
    df_all = merge_apartments(df_funda, df_pararius, df_huurwoningen, df_huurstunt, save=save)
    df_averages = get_average_data(df_all, save=save)
    return df_averages


if __name__ == '__main__':
    preprocess_all()
