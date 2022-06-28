import concurrent.futures as futures
from ratestbed.site.ratb import get_product_price
from ratestbed.site.ratb import get_ratb_data
import os
import pandas as pd


def run_multi_proces(df):
    futures_list = []
    for i in list(range(len(df))):
        sr = df.iloc[i]
        with futures.ProcessPoolExecutor() as executor:
            future = executor.submit(get_product_price, sr, i)
            futures_list.append(future)

    result = futures.wait(futures_list)
    price_list = []
    for future in result.done:
        try:
            r = future.result()
            price_list.append(r)
        except:
            return None

    return price_list


def main():
    if os.path.isfile('ratestbed_info.xlsx'):
        df = pd.read_csv('./ratestbed_info.xlsx')
    else:
        df = get_ratb_data()
        df.to_excel('/Users/user/dataknows/robo-advisor-testbed/ratestbed_info.xlsx')

    price_list = run_multi_proces(df)

    return price_list


if __name__ == '__main__':
    main()
