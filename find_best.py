import pandas as pd

def find_best_items(df, df_all_items, save=False, threshold_good_apartment=2):
    keyvalues = ['price_pm', 'living_area', 'bedrooms', 'rooms', 'energie_class']
    if not df.equals(df_all_items):
        df_all_items = pd.concat([df, df_all_items])
    averages = df_all_items.groupby('postcode_beginning').aggregate(
            s := {**dict.fromkeys(keyvalues, ['mean', 'std']),
             'address': 'count'})
    cols = [f'{a}_{i}' for a in s for i in s[a] if type(s[a]) == list]
    cols.append('count')
    averages.columns = cols
    averages = averages.reset_index()
    df_res = df.merge(averages, how='inner', on='postcode_beginning')
    # number of stddevs that the current offer differs from the average of that region
    for keyvalue in keyvalues:
        df_res[f'{keyvalue}_stddev_ratio'] = (df_res[keyvalue] - df_res[f'{keyvalue}_mean'])/df_res[f'{keyvalue}_std']
    mask_price_pm = df_res.price_pm_stddev_ratio < -1 * threshold_good_apartment
    mask_living_area = df_res.living_area_stddev_ratio > threshold_good_apartment
    mask_bedrooms = df_res.bedrooms_stddev_ratio > threshold_good_apartment
    mask_rooms = df_res.rooms_stddev_ratio > threshold_good_apartment
    mask_energy = df_res.energie_class_stddev_ratio < -1 * threshold_good_apartment
    df_best = df_res.loc[mask_price_pm | mask_living_area | mask_bedrooms | mask_rooms | mask_energy]
    # sum them all to get the combined ranking
    # TODO maybe have a weight for each?
    df_best['ranking'] = df_best.living_area_stddev_ratio - df_best.price_pm_stddev_ratio - df_best.energie_class_stddev_ratio + df_best.rooms_stddev_ratio
    # df_best_sorted = df_best.sort_values(by=['living_area_stddev_ratio', 'price_pm_stddev_ratio',
    #                                      'energie_class_stddev_ratio', 'bedrooms_stddev_ratio', 'rooms_stddev_ratio'],
    #                                      ascending=[False, True, True, False, False])  # best values should be first
    df_best_sorted = df_best.sort_values(by='ranking', ascending=False)
    if save:
        df_best_sorted.to_csv('best_results.csv')
    return df_best_sorted

if __name__ == '__main__':
    df = pd.read_csv('data/all_apartments_processed.csv')
    find_best_items(df, df)

