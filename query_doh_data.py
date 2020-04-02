#!/usr/bin/python3
"""
Fetches JSON data from https://services5.arcgis.com/mnYJ21GiFTR97WFg
and saves it as an .xlsx excel file
"""
import time
import json
import requests
import pandas as pd
import threading
from queue import Queue
from datetime import datetime

q = Queue()
df_lock = threading.Lock()
print_lock = threading.Lock()
dfs = []


def threader():
    """"thread management"""
    while True:
        item = q.get()
        if item is None:
            break
        df = get_df(item)

        with print_lock:
            print(df)

        with df_lock:
            dfs.append(df)

        q.task_done()


def get_json_data(url: str):
    response = requests.get(url)
    data = json.loads(response.text)
    data = json.dumps([y["attributes"] for y in data["features"]])
    return data


def get_df(url: str):
    '''Fetch JSON from API and return pandas DataFrame'''
    try:
        data = get_json_data(url)
        if data == '[]':
            df = None
            raise ValueError 
        df = pd.DataFrame(pd.read_json(data))
    except:
        df = None
    finally:
        return df


def query_range(start: int = 1, end: int = 1_000_000) -> None:
    ''' Use API to query data from FID <start> to <end>'''
    threads = 64
    for _ in range(threads):
        thread = threading.Thread(target=threader)
        thread.daemon = True
        thread.start()

    for fid in range(start, end+1):
        url = f"https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services/PH_masterlist/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&objectIds={fid}&outFields=*&outSR=102100&cacheHint=true"
        q.put(url)
    q.join()
    df = pd.concat(dfs)
    return df


def query_all() -> None:
    ''' Use API to query all data
        Current bug, can only return 2000 data points
        could be the rate limit of the API'''
    url = "https://bit.ly/ph_doh_covid19_json"
    df = get_df(url)
    return df


def confirmed_cases() -> int:
    cases = int(json.loads(get_json_data('https://services5.arcgis.com/mnYJ21GiFTR97WFg/arcgis/rest/services/slide_fig/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22confirmed%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&cacheHint=true'))[0]['value'])
    return cases

def main() -> None:
    '''Main function'''
    t1 = datetime.now()

    df1 = query_all()
    print(df1)

    a = df1['FID'].max() + 1
    b = confirmed_cases()

    df2 = query_range(a, b)
    print(df2)
    df3 = pd.concat((df1, df2))

    t2 = datetime.now()

    print(f'\nFinished in {t2-t1} seconds.\n')
    fname = f"covid19_ph1-ph{b}_{t2.strftime('%Y%m%d%H%M%S')}.xlsx"
    print(f"Saving data to {fname}")
    df3.to_excel(fname)


if __name__ == '__main__':
    main()
