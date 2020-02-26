import requests
import numpy as np
import datetime
import re
from bs4 import BeautifulSoup

from multiprocessing import Pool

import time
import pandas as pd

import re
import math


def getPage(page, ID, date1, date2):
    """Download one web page

        Params
        ------
        page - page number
        Id - id of item in bank balance sheet (unique within the whole russian bank industry)
        date1 - date to parse from
        date2 - date to parse to
        """

    mainpage = 'http://www.banki.ru/banks/ratings/?PROPERTY_ID='+ \
               ID +\
               '&search[type]=name&sort_param=rating&sort_order=ASC&REGION_ID=0&date1=' + date1 +\
               '&date2=' + date2 +\
               '&IS_SHOW_GROUP=0&PAGEN_1=' + page

    html = pd.read_html(io=mainpage, keep_default_na=False, na_values='н/д', decimal=',')
    soup = pd.DataFrame(html[2])

    return soup


def Page_Deep(data1, data2, ID):
    """Search inside one page"""

    k = 0
    i = 1
    while i < 20:
        if k == 0:
            data = getPage(str(i),ID,data1,data2)
            if data.iloc[:, 3].count() == 0:
                i = i + 1
                continue
            else:
                k = 1
                i = i + 1
        cur_page_data = getPage(str(i),ID,data1,data2)
        if cur_page_data.iloc[:,3].count() == 0:
            i = i + 1
            continue
        data = pd.concat([data, cur_page_data])
        if len(cur_page_data) < 50:
            break
        i = i + 1

    return data


def One_ID_List_Date(ID, dats):
    """Downloads all tables dedicated to one feature (one item from bank balance sheet)

    Params
    ------
    ID - id of item in bank balance sheet (unique within the whole russian bank industry)
    dates - dates to pares
    """

    all_data = []

    for idate in range(len(dats)-1):
        try:
            current_data = Page_Deep(dats[(idate+1)], dats[idate], ID)
            ddf = pd.DataFrame(current_data)
            ddf.columns = ['неважно', 'Лицензия_Банк', dats[(idate+1)], dats[idate], 'неважно', 'неважно']
            df = pd.DataFrame()
            df['Лицензия_Банк']= ddf ['Лицензия_Банк']
            df[dats[(idate+1)]]=ddf[dats[(idate+1)]]
            df[dats[idate]] = ddf[dats[idate]]
            all_data.append(df)
        except Exception:
            all_data.append('Пропуск в данных')

    return all_data


def get_ID(ID, dats):
    """Downloads one feature (one item from bank balance sheet) as one dataframe

    Params
    ------
    ID - id of item in bank balance sheet (unique within the whole russian bank industry)
    dates - dates to pares
    """

    k = 0
    for date in range(len(dats)-1):
        try:
            if k == 0:
                df = Page_Deep(dats[(date + 1)],dats[date],ID)
                if len(df) == 0:
                    df = pd.DataFrame()
                    continue
                else:
                    k = 1
                    df['дата'] = dats[(date + 1)]
                    df.columns = ['drop1','Лицензия_банк','ИД_' + ID,'drop2','drop3', 'drop4', 'дата']
                    df.drop(['drop1','drop2','drop3', 'drop4'], axis=1, inplace=True)
            df_0 = Page_Deep(dats[(date + 1)],dats[date],ID)
            if len(df_0) == 0:
                df_0 = pd.DataFrame()
                continue
            df_0['дата'] = dats[(date + 1)]
            df_0.columns = ['drop1','Лицензия_банк','ИД_' + ID,'drop2','drop3', 'drop4', 'дата']
            df_0.drop(['drop1','drop2','drop3','drop4'], axis=1, inplace=True)
            df = pd.concat([df, df_0])

        except Exception:
            continue
    return df


def parse_reporting_info(ID_list, dats, path):
    """Downloads ALL features (all item from bank balance sheet) as one dataframe

    Params
    ------
    IDs - list of ids of item in bank balance sheet (unique within the whole russian bank industry)
    dates - dates to pares
    path - where to save results
    """

    for ID in ID_list:
        start_time = time.time()

        data_id_401 = get_ID(ID, dats)
        data_id_401.to_csv(path + ID + '.csv', sep=',', header=True, index=False)

        print("--- %s minutes ---" % np.round((time.time() - start_time) / 60, 0))


########## Bank defaults parser ##########

def Bank_parser(number):
    bad_page = 'http://www.banki.ru/banks/memory/?by=PROPERTY_date&order=desc&PAGEN_1=' + str(number)
    response = requests.get(bad_page)
    html = response.content
    soup = BeautifulSoup(html, "lxml")
    return (soup)


def get_cause(url, mainpage='http://www.banki.ru'):
    page = mainpage + url
    response = requests.get(page)
    html = response.content
    soup = BeautifulSoup(html, "lxml")
    cause = soup.findAll('dd', {'class': "margin-bottom-zero"})
    return cause


def parse_closed_banks(path, mainpage='http://www.banki.ru'):
    bankrot = {'Банк': [], 'Лицензия': [], 'хреф': [], 'статус': [], 'ликвдата': [], 'причина': []}
    for i in range(1, 53):
        bank_page = Bank_parser(i, mainpage)
        curvec = bank_page.findAll("tbody")[2].findAll('td')

        bank = [curvec[1 + i * 6].findAll('a')[0].text for i in range(int(len(curvec) / 6))]
        lits = [curvec[2 + i * 6].text for i in range(int(len(curvec) / 6))]
        href = [re.split('="|">', str(curvec[1 + i * 6].findAll('a')[0]))[1] for i in range(int(len(curvec) / 6))]
        status = [curvec[3 + i * 6].text for i in range(int(len(curvec) / 6))]
        likvid_data = [curvec[4 + i * 6].text for i in range(int(len(curvec) / 6))]

        cause = [get_cause(item) for item in href]  # убрал [0].text из-за ошибки
        bankrot['Банк'].extend(bank)
        bankrot['Лицензия'].extend(lits)
        bankrot['хреф'].extend(href)
        bankrot['статус'].extend(status)
        bankrot['ликвдата'].extend(likvid_data)
        bankrot['причина'].extend(cause)

    closed_banks = pd.DataFrame(bankrot)
    closed_banks.drop(['Банк', 'причина', 'статус', 'хреф'], inplace=True, axis=1)
    closed_banks.to_csv(path + 'closed_banks.csv', sep=',', header=True, index=False)