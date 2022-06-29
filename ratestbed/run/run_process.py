import concurrent.futures as futures
from ratestbed.site.ratb import get_ratb_data
from ratestbed.site.ratb import get_product_price
from ratestbed.site.ratb import get_product_info
from ratestbed.site.ratb import data_cleansing
from ratestbed.uploader.upload import upload_ratestbed_price
from ratestbed.uploader.upload import upload_ratestbed_info
import os
import pandas as pd


def batch_product_daily(df):
    futures_list = []
    for i in list(range(len(df))):
        sr = df.iloc[i]
        with futures.ProcessPoolExecutor() as executor:
            future = executor.submit(get_product_price, sr)
            print(f'{sr} submitted')
            futures_list.append(future)

    result = futures.wait(futures_list)
    for future in result.done:
        try:
            r = future.result()
            upload_ratestbed_price(r.to_dict())
            print(f'{r.to_dict()} uploaded')
        except Exception as e:
            raise e


def batch_product_info(df):
    futures_list = []
    for i in list(range(len(df))):
        sr = df.iloc[i]
        with futures.ProcessPoolExecutor() as executor:
            future = executor.submit(get_product_info, sr)
            print(f'{sr} submitted')
            futures_list.append(future)

    result = futures.wait(futures_list)
    for future in result.done:
        try:
            r = future.result()
            sr = data_cleansing(r)
            upload_ratestbed_info(sr.to_dict())
            print(f'{sr.to_dict()} uploaded')
        except Exception as e:
            raise e


def main():
    if os.path.isfile('ratestbed_info.xlsx'):
        df = pd.read_excel('ratestbed_info.xlsx', dtype=str, index_col=0)
    else:
        df = get_ratb_data()
        df.to_excel('ratestbed_info.xlsx')

    print('start product daily process')
    batch_product_daily(df)
    print('start product info process')
    batch_product_info(df)


if __name__ == '__main__':
    main()
