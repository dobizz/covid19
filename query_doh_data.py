#!/usr/bin/python3

import requests
import json
import pandas as pd

def main():
    url = "https://bit.ly/ph_doh_covid19_json"
    response = requests.get(url)
    data = json.loads(response.text)
    x = json.dumps([y["attributes"] for y in data["features"]])
    df = pd.DataFrame(pd.read_json(x))
    df.to_excel(f"covid19_ph1-ph{len(df)}.xlsx")

if __name__ == '__main__':
    main()