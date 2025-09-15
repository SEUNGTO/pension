import pandas as pd
import os
import requests
import requests
import io
import zipfile
import xmltodict
import pandas as pd

# 코드 출처 : https://wikidocs.net/112853

url = "https://opendart.fss.or.kr/api/corpCode.xml"
api_key = os.getenv('DART_API_KEY')

params = {
    "crtfc_key": api_key
}
resp = requests.get(url, params=params)

f = io.BytesIO(resp.content)
zfile = zipfile.ZipFile(f)
zfile.namelist()
xml = zfile.read("CORPCODE.xml").decode("utf-8")
dict_data = xmltodict.parse(xml)
data = dict_data['result']['list']
df = pd.DataFrame(data)
os.makedirs('data', exist_ok=True)
df.to_csv('data/corp_code.csv', sep = "\t", index = False)