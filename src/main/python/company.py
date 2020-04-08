import json
from const import *
import requests

session = requests.Session()

def search():
    f = open('../../../tmp/company_search.json')
    json_ = json.load(f)
    f.close()
    resp = session.post(COMPANY_SEARCH, data=json_)
    if resp.status_code != 200:
        print("status_code: %d" % resp.status_code)
        return

    json_ = resp.json()
    with open('../../../tmp/company_search_result.json') as f:
        json.dump(json_, f)
    print('search completed. please refer to file tmp/company_search_result.json')

if __name__ == '__main__':
    search()