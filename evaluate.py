import pdb
import pandas as pd
import numpy as np

def convert_yearly_to_daily(x) :
    return (1+x) ** (1/252) -1

def annual_returns(data, ret) :
    returns = data[ret]
    cum_returns = (1+returns).prod()
    period = len(returns)
    ann_returns = cum_returns ** (252/period) -1
    return ann_returns

def beta(data, ret, rf, bm):
    returns = data[ret]
    riskfree = data[rf]
    marketex = data[bm] - riskfree

    riskfree = convert_yearly_to_daily(riskfree)
    exreturns = returns - riskfree

    return np.cov(exreturns.dropna(),marketex.dropna())[0][1] / np.var(marketex.dropna())

def alpha(data, ret, rf, bm):
    returns = data[ret]
    riskfree = data[rf]
    marketex = data[bm] - riskfree

    riskfree = convert_yearly_to_daily(riskfree)
    exreturns = returns - riskfree
    alpha=np.mean(exreturns)-beta(data, ret, rf, bm)*np.mean(marketex)
    return alpha

def sharpe_ratio(data, ret, rf):
    returns = data[ret]
    crf = data[rf]
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

def treynor_ratio(data, ret, rf, bm):

    returns = data[ret]
    crf = data[rf]

    cum_returns = (1+returns).prod()
    period = len(returns)
    annual_returns = cum_returns ** (252/period) -1
    
    daily_crf = convert_yearly_to_daily(crf)
    cum_crf = (1+daily_crf).prod()
    period = len(daily_crf)
    annual_crf = cum_crf ** (252/period) - 1
    
    return (annual_returns-annual_crf) / beta(data, ret, rf, bm)

def information_ratio(data, ret, bm):

    returns = data[ret]
    benchmark = data[bm]
    diff = returns - benchmark
    return np.mean(diff) / diff.std()

def max_drawdown(data, ret):
    returns = data[ret]
    cum_returns = (1+returns).cumprod()
    peak = cum_returns.expanding(min_periods=1).max()
    drawdown = (cum_returns - peak) / peak
    max_drawdown = drawdown.min()
    return max_drawdown


if __name__ == '__main__' : 

    file_list = [

        '250923_return_annually_rebalance.xlsx',
        '250923_return_semiannually_rebalance.xlsx',
        '250923_return_quarterly_rebalance.xlsx',

    ]

    for file in file_list : 
        period = file.split("_")[2]

        pf = pd.read_excel(file)
        pf = pf.rename(columns = {'Unnamed: 0' : 'Date'})
        pf = pf.set_index('Date')

        bm = pd.read_excel('benchmark.xlsx')
        bm = bm.set_index('Date')
        bm = bm.loc['2016-03-15':]

        data = pf.join(bm)

        y_list = ['Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7']
        bm_list = ['kospi', 'kosdaq', 'kospi200']


        summary = pd.DataFrame()

        # BM 성과
        for bm_idx in bm_list : 
            bm_eval = {
                'annual' : annual_returns(data, bm_idx),
                'alpha' : None,
                'beta' : None,
                'sharpe_ratio' : sharpe_ratio(data, bm_idx, 'riskfree'),
                'treynor_ratio' : None,
                'information_ratio' : None,
                'max_drawdown' : max_drawdown(data, bm_idx),
            }
            tmp = pd.DataFrame(bm_eval, index = [bm_idx])
            summary = pd.concat([summary, tmp])

        
        # BM 대비 포트폴리오 성과 (Y1~Y7)
        for bm_idx in bm_list : 
            for y in y_list : 
                pf_eval = {
                    'annual' : annual_returns(data, y),
                    'alpha' : alpha(data, y, 'riskfree', bm_idx),
                    'beta' : beta(data, y, 'riskfree', bm_idx),
                    'sharpe_ratio' : sharpe_ratio(data, y, 'riskfree'),
                    'treynor_ratio' : treynor_ratio(data, y, 'riskfree', bm_idx),
                    'information_ratio' : information_ratio(data, y, bm_idx),
                    'max_drawdown' : max_drawdown(data, y),
                }
                buffer = pd.DataFrame(pf_eval, index = [f"{y}(vs. {bm_idx})"])
                summary = pd.concat([summary, buffer])
        
        # 결과 저장
        summary.to_excel(f"summary_{period}.xlsx")