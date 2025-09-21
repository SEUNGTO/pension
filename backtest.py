import os
from tqdm import tqdm
import pandas as pd
import FinanceDataReader as fdr
from pandas.tseries.offsets import MonthBegin, MonthEnd

# +-----------------------+
# | 1. 데이터 전처리        |
# +-----------------------+

# 재무제표 데이터 불러오기
fs = pd.read_csv('data/pivot_data.csv', sep = "\t", dtype = str).reset_index(drop=True)

# Risk free rate (국고채3년물 수익률)
# 데이터 출처 : 한국은행 (https://snapshot.bok.or.kr/dashboard/A2)
riskfree = pd.read_csv('data/riskfree.csv')
riskfree['날짜'] = pd.to_datetime(riskfree['날짜'])
riskfree = riskfree.set_index('날짜')

# 데이터타입 변경
fs['날짜'] = pd.to_datetime(fs['날짜'])
fs.loc[:, '당기순이익':] = fs.loc[:, '당기순이익':].astype(float)

# 결측치 제거
fs.dropna(inplace=True)

# 편의상 결산월이 12월인 경우만 남김
con1 = (fs['날짜'].dt.month==3) & (fs['보고서코드'] == '11013') # 1분기 보고서 코드 : 11013
con2 = (fs['날짜'].dt.month==6) & (fs['보고서코드'] == '11012') # 반기 보고서 코드 : 11012
con3 = (fs['날짜'].dt.month==9) & (fs['보고서코드'] == '11014') # 3분기 보고서 코드 : 11014
con4 = (fs['날짜'].dt.month==12) & (fs['보고서코드'] == '11011') # 사업 보고서 코드 : 11011
fs = fs[con1 | con2 | con3 | con4]

# 매출액이 0인 경우 (Dart 오류일수도 있고, 금융기업일수도 있음) 제외
fs = fs[fs['매출액']!= 0]

# 지표 생성
# Y1 : 순이익률 (당기순이익 / 매출액)
# Y2 : 영업이익률 (영업이익 / 매출액)
# Y3 : ROE (당기순이익 / 자본총계) * 편의상 기말자본총계 사용하지만, 시간이 된다면 기초자본총계 사용 필요
# Y4 : ROA (당기순이익 / 자산총계)
# Y5 : 법인세차감전이익률 (법인세차감전순이익/매출액)
# Y6 : ROE2 (법인세차감전순이익/자본총계)
# Y7 : ROA2 (법인세차감전순이익/자산총계)

fs.loc[:, 'Y1'] = fs['당기순이익'] / fs['매출액']
fs.loc[:, 'Y2'] = fs['영업이익'] / fs['매출액']
fs.loc[:, 'Y3'] = fs['당기순이익'] / fs['자본총계']
fs.loc[:, 'Y4'] = fs['당기순이익'] / fs['자산총계']
fs.loc[:, 'Y5'] = fs['법인세차감전 순이익'] / fs['매출액']
fs.loc[:, 'Y6'] = fs['법인세차감전 순이익'] / fs['자본총계']
fs.loc[:, 'Y7'] = fs['법인세차감전 순이익'] / fs['자산총계']


# +-----------------------+
# | 2. 전략 백테스팅        |
# +-----------------------+
"""
    [ 백테스트 방법 ]
(1) 리밸런싱 주기 : 분기 1회 / 반기 1회 / 연 1회
(2) 매수/매도 : 다음 분기 초에 매수 > 분기 말에 매도
 e.g. 3월 31일 보고서 기준인 경우 : 4월 1일 종가에 매수, 6월 30일 종가에 매도
(3) 기타 : 발표 시점을 고려하지 않은 경우(buffer = 0)와 고려한 경우(buffer = 1) 모두 테스트
"""

# 성과를 살펴볼 5개 그룹(G5가 가장 좋음)
group = ['G1', 'G2', 'G3', 'G4', 'G5']

# Quality 기준 리스트 및 날짜 리스트 생성
y_list = ['Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7']
date_list = fs['날짜'].sort_values().unique()  # 리밸런싱 주기를 늘리려면 사용할 date 빈도를 수정


# +-----------------------+
# | 3. 백테스트 시작        |
# +-----------------------+

# 백테스팅 설정
test_setting = {
    'quarterly' : {
        'holding_period' : 3,
        'rebalance_month' : [3, 6, 9, 12]
    },
    'semiannually' : {
        'holding_period' : 6,
        'rebalance_month' : [6, 12]
    },
    'annually' : {
        'holding_period' : 12,
        'rebalance_month' : [12]
    },
}

for test_period, setting in test_setting.items() :

    holding_period = setting['holding_period']
    rebalance_month = setting['rebalance_month']
    date_list = [d for d in date_list if d.month in rebalance_month]


    for buffer in [0, 1] : 

        for date in date_list :

            for y in y_list :
                
                print(f"BUFFER : {buffer} | {test_period} |{date.strftime('%Y-%m-%d')} | {y}               ")

                tmp = fs.loc[fs['날짜'] == date, ['종목코드', '날짜', y]].copy()
                tmp['그룹'] = pd.qcut(tmp[y], len(group), labels=group)
                tmp['기간수익률'] = None
                tmp['표준편차'] = None

                
                buy_date = date + MonthBegin(buffer + 0)
                sell_date = date + MonthEnd(buffer + holding_period)

                i = 0
                for code in tmp['종목코드'] :
                    
                    i += 1
                    # 가격 정보가 없는 경우 다음으로 
                    nm = f'price/{code}.csv'
                    if not os.path.exists(nm) :
                        continue

                    p = pd.read_csv(nm)
                    p['Date'] = pd.to_datetime(p['Date'])
                    p = p.set_index('Date').loc[buy_date:sell_date]['Change']
                    
                    
                    if p.std() > 0.0 :
                        # 가격은 있으나, 거래정지 등의 사유로 표준편차가 0인 경우는 제외
                        tmp.loc[tmp['종목코드'] == code, '기간수익률'] = p.sum()   # 전일 대비 등락률의 합 = 기간수익률
                        tmp.loc[tmp['종목코드'] == code, '표준편차'] = p.std()     # 전일 대비 등락률의 표준편차
                    
                    
                    print(f" - {i}번째 작업 중 | 진행률 : {(i / len(tmp['종목코드'])) * 100:5.2f}%", end = "\r")

                # 성과 데이터 생성
                rf = riskfree.loc[buy_date:sell_date].mean().values[0]
                tmp['성과'] = (tmp['기간수익률'] - rf) / tmp['표준편차']
                
                os.makedirs(f'{test_period}_summary', exist_ok=True)
                summary = tmp.dropna().groupby('그룹', observed=False)[['성과']].mean()
                summary.to_csv(f"{test_period}_summary/{date.strftime('%Y-%m-%d')}_{y}_buffer{buffer}M.csv")

                # 분석용 데이터 저장
                os.makedirs(f'{test_period}_result', exist_ok=True)
                tmp.to_csv(f"{test_period}_result/{date.strftime('%Y-%m-%d')}_{y}_buffer{buffer}M.csv", index = False)
        

        # +-----------------------+
        # | 4. 결과 요약           |
        # +-----------------------+
        file_list = os.listdir(f'{test_period}_summary')
        file_list = [f for f in file_list if f"buffer{buffer}M" in f]
        summary = pd.DataFrame()
        for file in tqdm(file_list) :

            date, y, _= file.split("_")
            
            tmp = pd.read_csv(f"{test_period}_summary/{file}")
            tmp['날짜'] = date
            tmp['기준'] = y
            
            summary = pd.concat([summary, tmp])

        summary = summary.pivot(index = ['날짜', '그룹'], columns = '기준', values='성과')
        summary.to_excel(f'{test_period}_summary_buffer{buffer}M.xlsx')