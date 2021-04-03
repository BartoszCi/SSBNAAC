import itertools
import requests
import datetime
import re
import json
from collections import namedtuple

class SSBData():
    """Pull data from ssb.no and parse to user friendly format"""

    url_start = r'https://data.ssb.no/api/v0/en/table/'

    def __init__(self, table_id, query_patch):
        self.table_id = table_id
        self._query_json = self._get_api_params(query_patch)
        self._content = self._get_data_from_api(self.url_start + table_id, self._query_json)

    @property
    def content(self):
        return self._content

    @property
    def shape(self):
        out = {'Category ' + str(i): v for i, v in enumerate(self._content['size'][:-1])}
        out['Index: '] = self._content['size'][-1]
        return out

    @property
    def categories(self):
        catego = self._content['id'][::]
        catego.remove('Tid')
        return catego 

    @property
    def columns(self):
        out = {}
        for cate in self.categories:
            out[cate] = {}
            for k, v in self._content['dimension'][cate]['category']['label'].items():
                out[cate][k] = v
        return out

    def _get_api_params(self, patch):
        with open(patch, 'r', encoding='UTF-8') as j_f:
            return json.load(j_f)

    def _get_data_from_api(self, url, params):
        raw_data = requests.post(url, json=params)
        return json.loads(raw_data.content)

    def _parse_date(self, date, freq):
        repl = {
                'quarter': {
                            '1': 1,
                            '2': 4, 
                            '3': 7, 
                            '4': 10
                            }
                }

        d_temp = date.split('K')
        return datetime.date(int(d_temp[0]), repl[freq][d_temp[1]], 1)

    @property
    def index(self):
        frequency = self._content['dimension']['Tid']['label']
        dates = self._content['dimension']['Tid']['category']['index']
        return {v: self._parse_date(k, frequency) for k, v in dates.items()}

    def iter_columns(self):
        meta = self.columns
        categories = [[val for val in meta[cat]] for cat in meta]
        yield from itertools.product(*categories, repeat=1)

    def iter_values(self):
        vals = self._content['value']
        yield from vals

    def serialize(self):
        idx = self.index
        columns = self.iter_columns()
        vals = self.iter_values()
        return {col_name: [next(vals) for _ in idx] for col_name in columns}

    def iter_records(self):
        columns = self.iter_columns()
        vals = self.iter_values()
        Rec = namedtuple('Record', ('index', 'column', 'value'))
        
        for col_name in columns:
            for _, idx_val in self.index.items():
                yield Rec(idx_val, col_name, next(vals))

