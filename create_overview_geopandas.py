from collections import defaultdict

import geopandas as gpd
# https://public.opendatasoft.com/explore/dataset/georef-netherlands-postcode-pc4/export/?location=8,52.1564,5.29337&basemap=jawg.light
import pandas as pd

# import folium
# import mapclassify
# import matplotlib
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

if __name__ == '__main__':
    df = gpd.read_file('georef-netherlands-postcode-pc4.geojson')
    averages = pd.read_csv('data/avg_values.csv')
    df['pc4_code'] = df.pc4_code.astype(int)
    # df_relevant = df.loc[df.pc4_code.isin(df_data.postcode_beginning)]
    # df_res = df_relevant.merge(df_data, how='inner', right_on='postcode_beginning', left_on='pc4_code')
    df_res = df.merge(averages, how='inner', left_on='pc4_code', right_on='postcode_beginning')
    # df_res.plot(column='rental_price', legend=True)

    for row in ['price_pm', 'living_area', 'bedrooms', 'rooms', 'energie_class', 'count']:
        d = df_res.explore(
            column=row,  # make choropleth based on "BoroName" column
            tooltip=[row, "pc4_code"],  # show "BoroName" value in tooltip (on hover)
            popup=True,  # show all values in popup (on click)
            tiles="CartoDB positron",  # use "CartoDB positron" tiles
            style_kwds=dict(color="black")  # use black outline
        )
        d.save(f'output/Overview_{row}.html')
    print('test')
