import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import json


def get_ratb_data():
    url = 'https://www.ratestbed.kr:7443/portal/pblntf/listProgrsInfo2.do?menuNo=200238'
    res = requests.get(url)
    html = bs(res.text, features='html.parser')
    # ts = soup.tagStack

    tr_list = html.select_one('span > td')

    acdl = html.find_all('a', {'class': 'detail_link'})
    tag_list = []
    for tag in acdl:
        url_cd_dict = json.loads(tag.attrs['data-params'])
        algo_nm_dict = {
            'group': tag.contents[1].contents[1].text,
            'algorithm_name': tag.contents[1].contents[3].text,
            'account_name': tag.contents[3].text.strip().replace('[', '').replace(']', '')
        }
        tag_dict = url_cd_dict | algo_nm_dict
        tag_list.append(tag_dict)

    df = pd.DataFrame(tag_list)
    df = df.drop(columns='method')

    # st = html.find_all(['span', 'td', 'div'])
    #
    # ra_list = []
    # for s in st:
    #     ra_list.append(s.get_text())

    return df.astype(str)


def get_product_price(sr):
    today = pd.Timestamp.today().strftime('%Y-%m-%d')
    url = f'https://www.ratestbed.kr:7443/portal/pblntf/chartsRatereturn.json?acnutSn={sr["acnutSn"]}&invtTyCd={sr["invtTyCd"]}&sdate=2000-01-01&edate={today}&hbrdAssetsAt={sr["hbrdAssetsAt"]}&odrSn={sr["odrSn"]}&targetSe=C'
    res = requests.get(url)
    j = res.json()
    df = pd.json_normalize(j['chartsRatereturnAcnut'])
    df = df.rename(columns={
        'basicDate': 'base_dt',
        'standardPrice': 'std_pr',
        'acnutNm': 'account_name'
    })
    df['product_name'] = sr['algorithm_name']
    df['group'] = sr['group']

    df = df[['base_dt', 'product_name', 'account_name', 'std_pr', 'group']]

    return df.where(pd.notnull(df), None)
