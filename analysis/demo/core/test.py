import logging
import pandas as pd
from datetime import datetime
from dataupdate import getTsCode
from feature import *
logging.basicConfig(filename='/vms/sdb/lhd/code/paper/stock/analysis/demo/core/dataAnalysis.log', filemode='a', level=logging.INFO,
                    format='[%(levelname)s][%(asctime)s][%(name)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("dataAnalysis.log")

# TODO 锤子线和上吊线
def hammerAndHanging(df,period=3):
    # 找出伞形线的位置
    df['HammerOrHanging'] = is_hammer_or_hanging_line(df)
    umbrella_positions = df[df['HammerOrHanging']].index
    # print(f"伞形线位置为:{df[df['HammerOrHanging']]['trade_date']}")

    result = []
    df['trend'] = trend(df)

    for idx in umbrella_positions:
        if idx < period:
            # result.append('数据不足，无法判断')
            continue
        prev_trends = df.loc[idx - period +1 :idx, 'trend']
        if all(prev_trends == 1):  # 上吊线
            result.append(f"code:{ts_code},{df['trade_date'].iloc[idx]}上吊线卖出信号")
        elif all(prev_trends == -1):  # 锤子线
            result.append(f"code:{ts_code},{df['trade_date'].iloc[idx]}锤子线买入信号")
    print(result)


ts_codes = getTsCode()
# ts_codes = ['002975.SZ']
for ts_code in ts_codes:
    print(ts_code)
    df = pd.read_csv(f'/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts_code}_{datetime.now().strftime("%Y%m%d")}.csv')
    df['trade_date'] = df['trade_date'].astype(str)
    df = df.sort_values(by='trade_date').reset_index(drop=True)

    hammerAndHanging(df)



