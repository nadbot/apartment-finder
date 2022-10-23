import pandas as pd
import geopandas

if __name__ == '__main__':
    df = pd.read_csv('data/all_apartments.csv')
    df['postcode_beginning'] = df.postcode.str[0:4]
    df['price_pm'] = df['rental_price'] / df['living_area']
    # a = df.sort_values('postcode_beginning')[['rental_price', 'postcode_beginning']]
    average_prices_per_postcode = df.groupby('postcode_beginning')['price_pm'].mean()

    print(df)
