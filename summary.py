import os
import pdb
import pandas as pd
from tqdm import tqdm

#%%
file_list = os.listdir('summary')
file_list = [f for f in file_list if "nobuffer" in f]
nobuffer = pd.DataFrame()
for file in tqdm(file_list) :

    date, y, _= file.split("_")
    
    tmp = pd.read_csv(f"summary/{file}")
    tmp['날짜'] = date
    tmp['기준'] = y
    
    nobuffer = pd.concat([nobuffer, tmp])

nobuffer = nobuffer.pivot(index = ['날짜', '그룹'], columns = '기준', values='성과')
nobuffer.to_excel('summary_nobuffer.xlsx')

# %%
file_list = os.listdir('summary')
file_list = [f for f in file_list if "buffer1M" in f]
buffer1M = pd.DataFrame()
for file in tqdm(file_list) :

    date, y, _= file.split("_")
    
    tmp = pd.read_csv(f"summary/{file}")
    tmp['날짜'] = date
    tmp['기준'] = y
    
    buffer1M = pd.concat([buffer1M, tmp])

buffer1M = buffer1M.pivot(index = ['날짜', '그룹'], columns = '기준', values='성과')
buffer1M.to_excel('summary_buffer1M.xlsx')