import pdb
import pandas as pd
import numpy as np

def convert_yearly_to_daily(x) :
    return (1+x) ** (1/252) -1


def beta(returns, riskfree, marketex):
    riskfree = convert_yearly_to_daily(riskfree)
    exreturns = returns - riskfree
    return np.cov(exreturns.dropna(),marketex.dropna())[0][1] / np.var(marketex.dropna())

def alpha(returns, riskfree, marketex):
    riskfree = convert_yearly_to_daily(riskfree)
    exreturns = returns - riskfree
    alpha=np.mean(exreturns)-beta(returns, riskfree, marketex)*np.mean(marketex)
    return alpha

def sharpe_ratio(returns, crf):
    cum_returns = (1+returns).prod()
    period = len(returns)
    annual_returns = cum_returns ** (252/period) -1
    vol = returns.std()
    annual_vol = vol * (252 ** 0.5)
    
    daily_crf = convert_yearly_to_daily(crf)
    cum_crf = (1+daily_crf).prod()
    period = len(daily_crf)
    annual_crf = cum_crf ** (252/period) - 1
    
    return (annual_returns - annual_crf) / annual_vol

def treynor_ratio(returns, crf, market):

    cum_returns = (1+returns).prod()
    period = len(returns)
    annual_returns = cum_returns ** (252/period) -1
    
    daily_crf = convert_yearly_to_daily(crf)
    cum_crf = (1+daily_crf).prod()
    period = len(daily_crf)
    annual_crf = cum_crf ** (252/period) - 1

    
    return (annual_returns-annual_crf) / beta(returns, crf, market)

def information_ratio(returns, benchmark):
    diff = returns - benchmark
    return np.mean(diff) / diff.std()

# def sortino_ratio(returns,crf,MAR): #MAR represents the minimum acceptable return
#     er = np.mean(returns)
#     std_neg = returns[returns<MAR].std() #returns is an array, not a dataframe
#     return (er-crf)/std_neg

file_list = [
    '250923_return_annually_rebalance.xlsx',
    '250923_return_semiannually_rebalance.xlsx',
    '250923_return_quarterly_rebalance.xlsx',
]

file = file_list[0]
period = file.split("_")[2]

pf = pd.read_excel(file)
pf = pf.rename(columns = {'Unnamed: 0' : 'Date'})
pf = pf.set_index('Date')

benchmark = pd.read_excel('benchmark.xlsx')
benchmark = benchmark.set_index('Date')
benchmark = benchmark.loc['2016-03-15':]

pdb.set_trace()