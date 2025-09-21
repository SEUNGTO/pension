#%%
import os
import pandas as pd
import FinanceDataReader as fdr
from tqdm import tqdm

os.makedirs('price', exist_ok=True)

codelist = pd.read_csv('data/corp_code.csv', sep = "\t")
codelist = codelist.dropna()

start = '2016-01-01'
end = '2025-06-30'

for code in tqdm(codelist['stock_code'][3482:]) :
    try : 
        buffer = fdr.DataReader(code, start=start, end = end)
        
        if not buffer.empty :
            
            buffer.to_csv(f'price/{code}.csv')
    except :
        print(f"ERROR | {code}")
        continue