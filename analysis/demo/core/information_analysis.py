import logging
import pandas as pd
from datetime import datetime
from dataupdate import getTsCode
from feature import *
import queue
import argparse
from returnTodb import *
import json
logging.basicConfig(filename='/vms/sdb/lhd/code/paper/stock/analysis/demo/core/information_analysis.log', filemode='a',
                    level=logging.INFO,
                    format='[%(levelname)s][%(asctime)s][%(name)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("information_analysis.log")

parser = argparse.ArgumentParser(description='STOCK Information Analysis')
parser.add_argument('--op', type=int, required=False, default=0, help='operator to use')
parser.add_argument('--uuid', type=str, required=False, default='abc', help='uuid')
parser.add_argument('--hammerAndHangingPeriod', type=int, required=False, default=3, help='趋势周期')
parser.add_argument('--engulfingPatternThreshold', type=float, required=False, default=0.5, help='刺透到第一根线的比重')
parser.add_argument('--engulfingPatternPeriod', type=int, required=False, default=3, help='趋势周期')
parser.add_argument('--starPatternShadowThreshold', type=float, required=False, default=0.6, help='实体占k线比例')
parser.add_argument('--starPatternFirstChangeThreshold', type=float, required=False, default=0.03, help='第一根线的实体波动')
parser.add_argument('--starPatternSecondChangeThreshold', type=float, required=False, default=0.01, help='第二根线的实体波动')
parser.add_argument('--starPatternThirdThreshold', type=float, required=False, default=0.5, help='第三根线深入第一根线的比重')
parser.add_argument('--starPatternPeriod', type=int, required=False, default=1, help='趋势周期')

parser.add_argument('--invertedHammerPatternBodyThreshold', type=float, required=False, default=0.1, help='实体占k线比重')
parser.add_argument('--invertedHammerPatternUpperShadowThreshold', type=float, required=False, default=0.6, help='上影线占k线比重')
parser.add_argument('--invertedHammerPatternDownShadowThreshold', type=float, required=False, default=0.1, help='下影线占k线比重')
parser.add_argument('--invertedHammerPatternPeriod', type=int, required=False, default=3, help='趋势周期')



args = parser.parse_args()


with open("information.json", 'r', encoding='utf-8') as file:
    information = json.load(file)

datas = queue.Queue()
# TODO 锤子线和上吊线
def hammerAndHanging(df, uuid,ts_code,period=3):
    # 找出伞形线的位置
    df['HammerOrHanging'] = is_hammer_or_hanging_line(df)
    umbrella_positions = df[df['HammerOrHanging']].index
    # print(f"伞形线位置为:{df[df['HammerOrHanging']]['trade_date']}")

    df['trend'] = trend(df)

    for idx in umbrella_positions:
        if idx < period:
            # result.append('数据不足，无法判断')
            continue
        prev_trends = df.loc[idx - period + 1:idx, 'trend']
        if all(prev_trends == 1):  # 上吊线
            data = (uuid,ts_code,df['trade_date'].iloc[idx],-1,"hammerAndHanging",information['reversal']['s1'])
            datas.put(data)

        elif all(prev_trends == -1):  # 锤子线
            data = (uuid, ts_code, df['trade_date'].iloc[idx],1,"hammerAndHanging",information['reversal']['b1'])
            datas.put(data)
# TODO 吞没形态
def engulfingPattern(df, uuid,ts_code,threshold=0.5,period=3):
    # 找出吞没形态的点
    df['engulfing']=is_engulfing_pattern(df,threshold=threshold)
    positions = df[df['engulfing']!=0].index
    # print(f"伞形线位置为:{df[df['HammerOrHanging']]['trade_date']}")

    df['trend'] = trend(df)
    for idx in positions:
        if idx < period:
            continue
        prev_trends = df.loc[idx - period:idx-1, 'trend']
        if all(prev_trends == 1) and df['engulfing'].iloc[idx] == -1:  # 看跌吞没形态
            data = (uuid, ts_code, df['trade_date'].iloc[idx], -1, "engulfingPattern",information['reversal']['s2'])
            datas.put(data)

        elif all(prev_trends == -1) and df['engulfing'].iloc[idx] == 1:  # 看涨吞没形态
            data = (uuid, ts_code, df['trade_date'].iloc[idx], 1, "engulfingPattern",information['reversal']['b2'])
            datas.put(data)

# TODO 星线(黄昏星和启明星)
def starPattern(df,uuid,ts_code,shadow_threshold=0.6,first_change_threshold=0.03,second_change_threshold=0.01,third_threshold=0.5,period=3):
    """
    判断 DataFrame 中每一天是否出现启明星形态
    :param df: 包含 'Open', 'High', 'Low', 'Close' 列的 DataFrame
    shadow_threshold : 实体站k线的比例
    first_change_threshold:下跌多少才算长阴线
    second_change_threshold：波动多少才算小实体
    third_threshold：深入到第一根阳线的比例
    :return: 包含判断结果的布尔型 Series
    """
    df['starLine'] = is_star_line(df,shadow_threshold,first_change_threshold,second_change_threshold,third_threshold)
    positions = df[df['engulfing'] != 0].index
    df['trend'] = trend(df)
    for idx in positions:
        if idx < period:
            continue
        prev_trends = df.loc[idx - period:idx - 1, 'trend']
        if all(prev_trends == 1) & df['starLine'].iloc[idx] == -1:  # 黄昏星
            data = (uuid, ts_code, df['trade_date'].iloc[idx], -1, "starPattern", information['reversal']['s3'])
            datas.put(data)

        elif all(prev_trends == -1) & df['starLine'].iloc[idx] == 1:  # 启明星
            data = (uuid, ts_code, df['trade_date'].iloc[idx], 1, "starPattern", information['reversal']['b3'])
            datas.put(data)

# TODO 倒锤子线
def invertedHammerPattern(df,uuid,ts_code,body_threshold=0.1, upper_shadow_threshold=0.6 , down_shadow_threshold=0.1,period=3):
    df['invertHammer'] = is_inverted_hammer_line(df,body_threshold,upper_shadow_threshold,down_shadow_threshold)
    positions = df[df['invertHammer']].index
    df['trend'] = trend(df)
    for idx in positions:
        if idx < period:
            continue
        prev_trends = df.loc[idx - period +1 :idx, 'trend']
        if all(prev_trends == 1):  # 流星线
            data = (uuid, ts_code, df['trade_date'].iloc[idx], -1, "invertedHammerPattern",information['reversal']['s4'])
            datas.put(data)

        elif all(prev_trends == -1):  # 倒锤子线
            data = (uuid, ts_code, df['trade_date'].iloc[idx], 1, "invertedHammerPattern",information['reversal']['b4'])
            datas.put(data)


ts_codes = getTsCode()
# ts_codes = ['002975.SZ']
for ts_code in ts_codes:
    print(ts_code)
    df = pd.read_csv(
        f'/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts_code}_{datetime.now().strftime("%Y%m%d")}.csv')
    df['trade_date'] = df['trade_date'].astype(str)
    df = df.sort_values(by='trade_date').reset_index(drop=True)

    hammerAndHanging(df,args.uuid,ts_code,args.hammerAndHangingPeriod)
    engulfingPattern(df,args.uuid,ts_code,args.engulfingPatternThreshold,args.engulfingPatternPeriod)
    starPattern(df,args.uuid,ts_code,args.starPatternShadowThreshold,args.starPatternFirstChangeThreshold,
                args.starPatternSecondChangeThreshold,args.starPatternThirdThreshold,args.starPatternPeriod)
    invertedHammerPattern(df, args.uuid, ts_code,args.invertedHammerPatternBodyThreshold, args.invertedHammerPatternUpperShadowThreshold,
                          args.invertedHammerPatternDownShadowThreshold, args.invertedHammerPatternPeriod)

    insert_data(datas, 0)
    while not datas.empty(): # 清空对列
        datas.get()



