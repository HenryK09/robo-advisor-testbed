import concurrent.futures as futures
from ratestbed.site.ratb import get_ratb_data
from ratestbed.site.ratb import get_product_price
from ratestbed.site.ratb import get_product_info
from ratestbed.site.ratb import data_cleansing
from ratestbed.uploader.upload import upload_ratestbed_price
from ratestbed.uploader.upload import upload_ratestbed_info
import os
import pandas as pd
import traceback


def batch_product_daily(df):
    futures_list = []
    with futures.ProcessPoolExecutor() as executor:
        for i in list(range(len(df))):
            sr = df.iloc[i]
            future = executor.submit(get_product_price, sr)
            print(f'{i} submitted')
            futures_list.append(future)

    print('start uploading')
    result = futures.wait(futures_list)
    for n, future in enumerate(result.done):
        try:
            r = future.result()
            upload_ratestbed_price(r.to_dict(orient='records'))
            print(f'{n} uploaded')
        except Exception as e:
            print(f'{e}')
            pass


def batch_product_info(df):
    futures_list = []
    with futures.ProcessPoolExecutor() as executor:
        for i in list(range(len(df))):
            sr = df.iloc[i]
            future = executor.submit(get_product_info, sr)
            print(f'{i} submitted')
            futures_list.append(future)

    print('start uploading')
    result = futures.wait(futures_list)
    for n, future in enumerate(result.done):
        try:
            r = future.result()
            sr = data_cleansing(r)
            upload_ratestbed_info(sr.to_dict())
            print(f'{n} uploaded')
        except Exception as e:
            print(f'{e}')
            traceback.print_exc()
            pass


def main():
    if os.path.isfile('ratestbed_info.xlsx'):
        df = pd.read_excel('ratestbed_info.xlsx', dtype=str, index_col=0)
    else:
        df = get_ratb_data()
        df.to_excel('ratestbed_info.xlsx')

    print('start product daily process')
    batch_product_daily(df)
    print('start product info process')
    # batch_product_info(df)


if __name__ == '__main__':
    main()
