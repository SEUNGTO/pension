import FinanceDataReader as fdr
import pandas as pd
import pdb


start_date = '2016-01-01'
end_date = '2025-06-30'

kospi = fdr.DataReader('KS11', start_date, end_date)[['Change']]
kosdaq = fdr.DataReader('KQ11', start_date, end_date)[['Change']]
kospi200 = fdr.DataReader('KS200', start_date, end_date)[['Change']]

riskfree = pd.read_csv('data/interest.csv', encoding='euc-kr')
con1 = start_date <= riskfree['날짜']
con2 = riskfree['날짜'] <= end_date
riskfree = riskfree.loc[con1 & con2, ['날짜', '국고채(3년)']]
riskfree['날짜'] = pd.to_datetime(riskfree['날짜'])
riskfree['국고채(3년)'] = riskfree['국고채(3년)'] / 100
riskfree.columns = ['Date', 'riskfree']
riskfree = riskfree.set_index('Date')

benchmark = pd.concat([kospi, kosdaq, kospi200, riskfree], axis = 1)
benchmark.columns = ['kospi', 'kosdaq', 'kospi200', 'riskfree']

benchmark.to_excel('benchmark.xlsx')