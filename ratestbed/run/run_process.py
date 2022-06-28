import concurrent.futures as futures
from ratestbed.site.ratb import get_product_price
from ratestbed.site.ratb import get_ratb_data
from ratestbed.uploader.upload import upload_ratestbed_price
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
    # price_list = []
    for future in result.done:
        try:
            r = future.result()
            upload_ratestbed_price(r.to_dict())
            # price_list.append(r)
        except:
            return None


def main():
    if os.path.isfile('ratestbed_info.xlsx'):
        df = pd.read_excel('./ratestbed_info.xlsx', dtype=str, index_col=0)
    else:
        df = get_ratb_data()
        df.to_excel('/Users/richg/dataknows/robo-advisor-testbed/ratestbed_info.xlsx')

    run_multi_proces(df)


if __name__ == '__main__':
    main()
