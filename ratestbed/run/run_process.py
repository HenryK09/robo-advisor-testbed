import concurrent.futures as futures
from ratestbed.site.ratb import get_ratb_data
from ratestbed.site.ratb import get_product_price
from ratestbed.site.ratb import get_product_info
from ratestbed.site.ratb import data_cleansing
from ratestbed.uploader.upload import upload_ratestbed_price
from ratestbed.uploader.upload import upload_ratestbed_info
from ratestbed.uploader.upload import get_conn
import pandas as pd
import traceback

WINDOWS_PATH = 'C:/Users/richg/dataknows/robo-advisor-testbed'


def fetch_db_algo():
    query = "select distinct algo_id from ratestbed.product_daily"
    return get_conn('local').fetch(query).data


def batch_product_daily(df, start):
    futures_list = []
    with futures.ProcessPoolExecutor() as executor:
        for i in list(range(len(df))):
            sr = df.iloc[i]
            future = executor.submit(get_product_price, sr, start)
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
    db_algo_list = fetch_db_algo()

    if len(db_algo_list) == 375:
        df = pd.read_excel(f'{WINDOWS_PATH}/ratestbed_info.xlsx', dtype=str, index_col=0)
        start = (pd.Timestamp.today() - pd.offsets.Week()).strftime('%Y-%m-%d')
    else:
        df = get_ratb_data()
        df.to_excel(f'{WINDOWS_PATH}/ratestbed_info.xlsx')
        start = '2000-01-01'

    print('start product daily process')
    batch_product_daily(df, start)
    print('start product info process')
    batch_product_info(df)


if __name__ == '__main__':
    main()
