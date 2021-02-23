import requests
import datetime
import re
import json

class TimeSeries():
    def __init__(self, code, index, values):
        self.code = code
        self.index = index
        self.values = values

class SSBData():
    url_start = r'https://data.ssb.no/api/v0/en/table/'

    def __init__(self, table_id, query_patch):
        self.table_id = table_id
        self.meta = self._get_api_params(query_patch)
        self.content = self._get_data_from_api(self.url_start + table_id, self.meta)

    def _get_api_params(self, patch):
        with open(patch, 'r', encoding='UTF-8') as j_f:
            return json.load(j_f)

    def _get_data_from_api(self, url, params):
        raw_data = requests.post(url, json=params)
        return json.loads(raw_data.content)

    def _parse_date(self, date, freq):
        repl = {'quarter': {'1': 1,
                            '2': 4, 
                            '3': 7, 
                            '4': 10}}
        d_temp = date.split('K')
        return datetime.date(int(d_temp[0]), repl[freq][d_temp[1]], 1)

    def get_index(self):
        frequency = self.content['dimension']['Tid']['label']
        dates = self.content['dimension']['Tid']['category']['index']
        return {v: self._parse_date(k, frequency) for k, v in dates.items()}

    def iter_values(self):
        vals = self.meta['value']
        for val in vals:
            yield val



if __name__ == '__main__':
    d = SSBData('09171', 'tab_09171.json')
    print(d.table_id)
    print(d.get_index())
 