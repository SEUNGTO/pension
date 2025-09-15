import os
import re
import pdb
import requests
import pandas as pd
import logging

logging.basicConfig(filename="errors.txt",
                    level=logging.ERROR,
                    format="%(asctime)s [%(levelname)s] %(message)s")



def fetch_financial_report(corp_code, bsns_year, reprt_code) :

    """
    [ reprt_code ] 
    1분기보고서 : 11013
    반기보고서 : 11012
    3분기보고서 : 11014
    사업보고서 : 11011
    """

    url = 'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json'
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'), 
        'corp_code' : corp_code, 
        'bsns_year' : bsns_year, 
        'reprt_code' : reprt_code,
    }
    response = requests.get(url, params=params)
    item = response.json()
    
    if item['status'] == '000' :

        return pd.DataFrame(response.json()['list'])

    else :

        raise Exception(item['status'])
    

def make_clean_data(item) :

    # 필요한 데이터만 가져오기    
    cols = ['stock_code', 'thstrm_dt', 'fs_div', 'sj_nm', 'account_nm', 'thstrm_amount', ]
    data = item[cols]

    # 1) 기준일자 추출
    date = data.loc[data['sj_nm'] == '재무상태표', 'thstrm_dt'].unique()[0]
    date = re.sub(r'\D+', '', date)
    date = pd.to_datetime(date)
    data.loc[:, 'thstrm_dt'] = date

    # 2) 재무제표 선택 (연결재무제표가 있는 경우 연결재무제표, 없으면 개별재무제표)
    fs_div = 'CFS'
    fs_div_list = data['fs_div'].unique()
    if 'CFS' not in fs_div_list :
        fs_div = 'OFS'
    con1 = data['fs_div'] == fs_div
    data = data[con1]
    
    # 3) 수익성 지표에 해당하는 계정 선택
    acnt_list = ['매출액', '영업이익', '법인세차감전 순이익', '당기순이익', '자산총계', '자본총계']
    con2 = data['account_nm'].isin(acnt_list)
    data = data[con2]

    # 4) 데이터 속성 변경
    data['thstrm_amount'] = data['thstrm_amount'].apply(lambda x : re.sub(r"\D+", '', x))
    data['thstrm_amount'] = data['thstrm_amount'].astype(float)

    # 5) 불필요한 데이터 삭제
    data.drop(['sj_nm', 'fs_div'], axis = 1, inplace = True)

    # 5) 컬럼명 변경
    new_col_name = {
        'stock_code' : '종목코드',
        'thstrm_dt' : '날짜',
        'account_nm' : '계정명',
        'thstrm_amount' : '금액',
    }

    data.rename(columns = new_col_name, inplace = True)

    return data


if __name__ == '__main__' : 

    CORPS = pd.read_csv('data/corp_code.csv', dtype = str, sep = "\t")
    CORPS = CORPS.dropna().reset_index(drop = True)
    CORP_CODE_LIST = CORPS['corp_code']
    

    YEAR_LIST = range(2015, 2025)
    REPRT_LIST = ['11013', '11012', '11014', '11011']
    
    result = pd.DataFrame()
    
    i = 0
    batch_size = 20
    corp_iter = int(len(CORP_CODE_LIST)/batch_size)
    total_iter = corp_iter * len(YEAR_LIST) * len(REPRT_LIST)
    print("total_iter :", total_iter)

    for YEAR in YEAR_LIST :
        
        for REPRT in REPRT_LIST : 
        
            for s in range(corp_iter) :

                try : 
                    corp = CORP_CODE_LIST[s * batch_size : (s+1) * batch_size]
                    CORP_CODE = ",".join(corp)
                    item = fetch_financial_report(CORP_CODE, YEAR, REPRT)
                    item = make_clean_data(item)
                    item['보고서코드'] = REPRT

                    result = pd.concat([result, item])

                except : 
                    msg = f"ERROR : {YEAR} | {REPRT} | {CORP_CODE}"
                    logging.error(msg)
                    continue
                
                finally :
                    i += 1
                    progress = (i/total_iter) * 100
                    print(f"{i}번째 완료 | 진행률 : {progress:5.2f}% | 현재 데이터 : {len(result)}" , end = "\r")

    # 데이터 저장    
    os.makedirs("data", exist_ok=True)
    result.to_csv('data/result.csv', sep = "\t", index = False)
    pivot_data = result.pivot_table(index=['종목코드', '날짜', '보고서코드'], columns='계정명', values='금액')
    pivot_data.to_csv('data/pivot_data.csv', sep = "\t")
    