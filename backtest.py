import os
import pandas as pd
import FinanceDataReader as fdr
from pandas.tseries.offsets import MonthBegin, MonthEnd

# +-----------------------+
# | 1. 데이터 전처리        |
# +-----------------------+

# 재무제표 데이터 불러오기
fs = pd.read_csv('data/pivot_data.csv', sep = "\t", dtype = str).reset_index(drop=True)

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
fs.loc[:, 'Y1'] = fs['당기순이익'] / fs['매출액']
fs.loc[:, 'Y2'] = fs['영업이익'] / fs['매출액']
fs.loc[:, 'Y3'] = fs['당기순이익'] / fs['자본총계']
fs.loc[:, 'Y4'] = fs['당기순이익'] / fs['자산총계']


# +-----------------------+
# | 2. 전략 백테스팅        |
# +-----------------------+
"""
    [ 백테스트 방법 ]
(1) 리밸런싱 주기 : 분기 1회
(2) 매수/매도 : 다음 분기 초에 매수 > 분기 말에 매도
 e.g. 3월 31일 보고서 기준인 경우 : 4월 1일 종가에 매수, 6월 30일 종가에 매도
(3) 기타 : 발표 시점은 고려하지 않음
 - 현실적으로는 기준일로부터 1달 정도 buffer를 두어야 함
 - 지금은 감안하지 않고 추후에 보완
"""

# 성과를 살펴볼 5개 그룹(G5가 가장 좋음)
group = ['G1', 'G2', 'G3', 'G4', 'G5']

# Quality 기준 리스트 및 날짜 리스트 생성
y_list = ['Y1', 'Y2', 'Y3', 'Y4']
date_list = fs['날짜'].sort_values().unique()  # 리밸런싱 주기를 늘리려면 사용할 date 빈도를 수정

# 백테스팅 구간
for date in date_list :

    for y in y_list :
        
        print(f"{date.strftime('%Y-%m-%d')} | {y} 백테스팅...")

        tmp = fs.loc[fs['날짜'] == date, ['종목코드', '날짜', y]].copy()
        tmp['그룹'] = pd.qcut(tmp[y], len(group), labels=group)
        tmp['기간수익률'] = None
        tmp['표준편차'] = None

        # 기준일자에 바로 실적을 알 수 있다고 가정
        # 현실성있게 하려면 1달 정도 buffer 필요
        buffer = 0
        buy_date = date + MonthBegin(buffer + 0)
        sell_date = date + MonthEnd(buffer + 3)

        i = 0
        for code in tmp['종목코드'] :
            r = fdr.DataReader(code, buy_date, sell_date)['Change'] # Change : 전일 대비 등락률
            tmp.loc[tmp['종목코드'] == code, '기간수익률'] = r.sum()   # 전일 대비 등락률의 합 = 기간수익률
            tmp.loc[tmp['종목코드'] == code, '표준편차'] = r.std()     # 전일 대비 등락률의 표준편차
            
            i += 1
            print(f"{i}번째 작업 중 | 진행률 : {(i / len(tmp['종목코드'])) * 100:5.2f}%", end = "\r")

        # 성과 데이터 생성
        tmp['성과'] = tmp['기간수익률'] / tmp['표준편차']
        
        # 결과
        print()
        print("백테스팅 결과")
        print(tmp.dropna().groupby('그룹', observed=False)[['성과']].mean())
        print()

        # 데이터 저장
        os.makedirs('result', exist_ok=True)
        tmp.to_csv(f"result/{date.strftime('%Y-%m-%d')}_{y}.csv", index = False)