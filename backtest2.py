import os
import pdb
import pandas as pd
from tqdm import tqdm
from pandas.tseries.offsets import MonthBegin, MonthEnd


# +-----------------------+
# | 1. 데이터 전처리        |
# +-----------------------+

# 1. 상장되지 않은 기업은 제외
# 2. 보유기간동안 거래정지된 기업 제외
#  - 단, 거래정지 여부는 공시로 판단하지 않고, 가격변동으로 봄 (표준편차 0)
# 3. 금융회사(은행, 보험, 증권사 등) 제외
# 4. 팩터 중에 일부라도 데이터가 없는 기업 제외 (e.g. 영업이익을 보고하지 않는 경우)
# 5. 결산월이 12월이 아닌 기업은 제외
# 6. 기타 분석이 불가능한 경우 제외
#  - 매출액이 0원인 경우


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
        'period' : {
            12 : 2,
            9 : 4,
            6 : 3,
            3 : 3
        },
        'rebalance_month' : [3, 6, 9, 12]
    },
    'semiannually' : {
        'period' : {
            12 : 5,
            6 : 7,
        },
        'rebalance_month' : [6, 12]
    },
    'annually' : {
        'period' : {
            12 : 12,
        },
        'rebalance_month' : [12]
    },
}

for rebalance, setting in test_setting.items() :

    # rebalnace_period = setting['rebalnace_period']
    rebalance_month = setting['rebalance_month']
    date_list = [d for d in date_list if d.month in rebalance_month]


    pf_return = pd.DataFrame()

    for date in date_list :
        
        
        tmp = fs.loc[fs['날짜'] == date, ['종목코드', '날짜'] + y_list].copy()
        period = test_setting[rebalance]['period'][date.month]

        buffer = 0
        if date.month == 12 :
            buffer = 3
        elif date.month == 9 :
            buffer = 2
        else :
            buffer = 2
        # 종목의 가격 데이터 불러오기
        first_date = date + MonthBegin(buffer) + pd.Timedelta(days=14)
        last_date = date + MonthBegin(buffer + period) + pd.Timedelta(days=14-1)
        

        print(f"리밸런싱 주기 : {rebalance} | 기준일자 : {date.strftime('%Y-%m-%d')} | 매수일자 : {first_date.strftime('%Y-%m-%d')} | 매도일자 : {last_date.strftime('%Y-%m-%d')}       ")

        # 불러와야 할 종목 리스트 추리기 (팩터별 상위 20%)            
        stock_list = []
        for y in y_list :
            grouping = tmp[['종목코드', '날짜', y]].copy()
            grouping['그룹'] = pd.qcut(grouping[y], len(group), labels=group)
            
            stock_list += grouping.loc[grouping['그룹'] == 'G5', '종목코드'].to_list()

        stock_list = list(set(stock_list)) # 불러와야 할 종목 

        i = 0
        stock_price = pd.DataFrame()
        for code in stock_list :
            
            i += 1
            # 가격 정보가 없는 경우 다음으로 
            nm = f'price/{code}.csv'
            if not os.path.exists(nm) :
                continue

            p = pd.read_csv(nm)
            p = p[['Date', 'Change']]
            p.columns = ['Date', code]
            p['Date'] = pd.to_datetime(p['Date'])
            p = p.set_index('Date').loc[first_date:last_date][code]
            
            
            if p.std() > 0.0 :
                stock_price = pd.concat([stock_price, p], axis = 1)
            
            print(f"[가격데이터] {i:4.0f}번째 작업 중 | 진행률 : {(i / len(stock_list)) * 100:5.2f}%       ", end = "\r")

        # 포트폴리오 일별 수익률 구하기
        price = pd.DataFrame()
        for y in y_list :

            result = tmp[['종목코드', y]].copy()
            result['그룹'] = pd.qcut(result[y], len(group), labels=group)
            result = result[result['그룹'] == 'G5']
            
            col = [c for c in result['종목코드'] if c in stock_price.columns]
            result = stock_price[col].mean(axis = 1)
            result = pd.DataFrame(result, columns = [y])
            
            price = pd.concat([price, result], axis = 1)

        # 리밸런싱 기간 동안의 데이터 합치기
        pf_return = pd.concat([pf_return, price])
    
    pf_return.index = pd.to_datetime(pf_return.index)
    pf_return.to_excel(f'250923_return_{rebalance}_rebalance.xlsx')