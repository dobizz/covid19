#!/usr/bin/python3
"""Fetches JSON data from https://services5.arcgis.com/mnYJ21GiFTR97WFg and saves it as an .xlsx excel file"""
import json
import requests
import pandas as pd

def main():
    '''Main function'''
    url = "https://bit.ly/ph_doh_covid19_json"
    response = requests.get(url)
    data = json.loads(response.text)
    data = json.dumps([y["attributes"] for y in data["features"]])
    df = pd.DataFrame(pd.read_json(data))
    df.to_excel(f"covid19_ph1-ph{len(df)}.xlsx")

if __name__ == '__main__':
    main()
    