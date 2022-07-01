import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import json


def get_ratb_data():
    url = 'https://www.ratestbed.kr:7443/portal/pblntf/listProgrsInfo2.do?menuNo=200238'
    res = requests.get(url)
    html = bs(res.text, features='html.parser')

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

    return df.astype(str)


def get_product_price(sr):
    today = pd.Timestamp.today().strftime('%Y-%m-%d')
    url = f'https://www.ratestbed.kr:7443/portal/pblntf/chartsRatereturn.json?acnutSn={sr["acnutSn"]}&invtTyCd={sr["invtTyCd"]}&sdate=2000-01-01&edate={today}&hbrdAssetsAt={sr["hbrdAssetsAt"]}&odrSn={sr["odrSn"]}&targetSe=C'
    try:
        res = requests.get(url)
    except:
        return None
    j = res.json()
    df = pd.json_normalize(j['chartsRatereturnAcnut'])
    df = df.rename(columns={
        'basicDate': 'base_dt',
        'standardPrice': 'std_pr',
        'acnutNm': 'account_name'
    })
    df['group'] = sr['group']

    df['base_dt'] = pd.to_datetime(df['base_dt'])

    df['id'] = 'RA'+sr['acnutSn']+sr['invtTyCd']+sr['odrSn']+sr['hbrdAssetsAt']
    df = df[['id', 'base_dt', 'std_pr']]

    return df.where(pd.notnull(df), None)


def get_product_info(sr):
    url = f'https://www.ratestbed.kr:7443/portal/pblntf/viewAcnutDetailComop.do?menuNo=200006&acnutSn={sr["acnutSn"]}&invtTyCd={sr["invtTyCd"]}&odrSn={sr["odrSn"]}&hbrdAssetsAt={sr["hbrdAssetsAt"]}'
    res = requests.post(url)
    html = bs(res.text, features='html.parser')
    th = html.find_all(['th', 'td'])
    info_dict = {}
    for i in list(range(len(th[0:20]))):
        if i % 2 == 0:
            key = th[i].text
            value = th[i + 1].text.strip()
            indv_dict = {key: value}
            info_dict = info_dict | indv_dict

    info_dict = info_dict | {'id': 'RA'+sr['acnutSn']+sr['invtTyCd']+sr['odrSn']+sr['hbrdAssetsAt']}

    return pd.Series(info_dict)


def data_cleansing(sr):
    sr[-1] = sr[-1].replace('\r', '').replace('\n', '').replace('\t', '').replace(
        '※ 참여자가 제시한 내용을 기술하였으며, 자세한 내용은 알고리즘 설명서를 확인하시기 바랍니다.', '').rstrip()
    sr[-2] = float(sr[-2].replace(',', ''))
    sr[-3] = int(sr[-3].replace(',', ''))
    sr[4] = sr[4].replace('\r', '').replace('\n', '').replace('\t', '')
    sr[4] = pd.to_datetime(sr[4].split('(')[0].replace('년 ', '/').replace('월 ', '/').replace('일', ''))
    sr[3] = pd.to_datetime(sr[3].replace('년 ', '/').replace('월 ', '/').replace('일', ''))
    sr[2] = sr[2].replace('회차', '')

    sr = sr.rename(index={
        '업체명': 'company_name',
        '알고리즘명': 'product_name',
        '통과차수': 'pass_num',
        '운용개시일': 'mng_start_dt',
        '공시개시일': 'disclosure_start_dt',
        '계좌유형': 'account_type',
        '계좌명': 'account_name',
        '운용자금(원)': 'mng_funds',
        '기준가': 'std_pr',
        '알고리즘 소개': 'description'
    })

    sr = sr.drop(index='std_pr')

    return sr.where(pd.notnull(sr), None)