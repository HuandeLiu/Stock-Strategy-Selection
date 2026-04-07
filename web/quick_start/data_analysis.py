from dataupdate import *
import argparse
from returnTodb import *
import concurrent.futures
import time
import threading
from tqdm import tqdm
import sys
from functools import partial
import os
import queue
from dataupdate import writer_local
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

parser = argparse.ArgumentParser(description='STOCK Data Analysis')
parser.add_argument('--op', type=int, required=True, default=1, help='operator to use')
parser.add_argument('--uuid', type=str, required=True, default='abc', help='uuid')
parser.add_argument('--history', type=int, required=False, default=100, help='history')
parser.add_argument('--patience', type=float, required=False, default=0.001, help='patience')
parser.add_argument('--ts_codes', type=str, required=False, default='000001.SZ',help='股票代码列表')
parser.add_argument('--happen', type=int, required=False, default=30, help='happen')
parser.add_argument('--trend_flag', type=bool, required=False, default=False, help='trend_flag')

parser.add_argument('--min_days', type=int, required=False, default=5, help='min_days')
parser.add_argument('--var_threshold', type=float, required=False, default=1.0, help='var_threshold')
parser.add_argument('--change_threshold', type=float, required=False, default=0.01, help='change_threshold')

parser.add_argument('--min_windows', type=int, required=False, default=3, help='min_windows')
parser.add_argument('--period', type=int, required=False, default=3, help='period')


args = parser.parse_args()

# writer_local()

# python data_analysis.py --uuid 12345--history 100 --happen 30 --patience 0.001 --trend_flag false
import logging
logging.basicConfig(filename='/vms/sdb/lhd/code/paper/stock/analysis/demo/core/dataAnalysis.log', filemode='a', level=logging.INFO,
                    format='[%(levelname)s][%(asctime)s][%(name)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("dataAnalysis")

""" TODO 均线分析
    需求：
    1、股价从高点依次往后回踩5、10、20、30、60日均线、且踩完60日均线之后的三天都是上涨趋势
    参数：
    1、history = 100 从历史的100个节点开始查看
    2、happen = 30 ，最后一个回踩点发生在最近happen天
    3、patience = 0.001, 低点在均线上方0.1个点也算回踩
    4、trend_flag = False 是否定义回踩后有三天连续的红线
    
"""

# datas =[]
datas = queue.Queue()
def average_analysis(df,op,uuid,history=100,happen=30,patience=0.001,trend_flag=False):
    # logger.info("正在进行股票代码 %s 的均线分析，参数为 history: %s , happen: %s, patience: %s , trend_flag: %s ",
    #             df['ts_code'].iloc[0],history,happen,patience,trend_flag)
    if(len(df)<history):
        return
    df = df.iloc[-1 * history:].reset_index(drop=True)
    # print(df)
    # def find_start_index(df,start = 0):
    #     # 遍历数据，找到五条均线都小于股价的起始节点
    #     start_indexs = []
    #     for i in range(start,len(df)):
    #         if (df['MA5'][i] < df['low'][i] and
    #                 df['MA10'][i] < df['low'][i] and
    #                 df['MA20'][i] < df['low'][i] and
    #                 df['MA30'][i] < df['low'][i] and
    #                 df['MA60'][i] < df['low'][i]):
    #             start_indexs.append(i)
    #     return start_indexs

    def find_start_index(df):
        # 遍历数据，找到五条均线都小于股价的起始节点
        condition = ((df['MA5'] < df['low']) &(df['MA10'] < df['low']) &
                (df['MA20'] < df['low']) &(df['MA30'] < df['low']) &
                (df['MA60'] < df['low']))
        start_indexs = df[condition].index
        # print(start_indexs)
        # if(len(start_indexs)<2):
        #     return start_indexs
        # else:
        #     filtered_indexs = []
        #     for i in range(1, len(start_indexs)):
        #         if start_indexs[i] - start_indexs[i - 1] > 5:
        #             filtered_indexs.append(start_indexs[i-1])
        #     filtered_indexs.append(start_indexs[-1])
        #
        #     return filtered_indexs
        return start_indexs


    # start_time = time.time()
    # start_indexs = df[df['average_start_index']].index
    start_indexs = find_start_index(df)
    # print(len(start_indexs))
    # end_time = time.time()
    # print("len {},time:{}".format(len(start_indexs),(end_time - start_time)))

    # start_time1 = time.time()
    success = []
    # while( start_index != -1 and start_index < len(df)):
    for start_index in start_indexs:
        relevant_data = df[start_index:]
        # print("start_index", start_index)
        # start_index = -1
        if(len(relevant_data)< 3):
            break
        ma_columns = ['MA5', 'MA10', 'MA20', 'MA30', 'MA60']
        pullback_dates = []
        last_date_index = start_index
        for ma_column in ma_columns:
            for j in range(last_date_index, len(relevant_data) - 3): # -3是因为后面还要有三天的红线
                if relevant_data['low'].iloc[j] <= relevant_data[ma_column].iloc[j] * (1 + patience): # 回踩
                    if ma_column == 'MA60':
                        if trend_flag:
                            if (relevant_data['close'].iloc[j] <= relevant_data['close'].iloc[j+1]
                                and relevant_data['close'].iloc[j+1] <= relevant_data['close'].iloc[j+2]
                                and relevant_data['close'].iloc[j+2] <= relevant_data['close'].iloc[j+3]
                            ):# 三天红线
                                pullback_dates.append(relevant_data['trade_date'].iloc[j])
                                if (len(set(pullback_dates)) != len(pullback_dates)):  # 判断是否在同一天回踩
                                    pullback_dates = []
                                if (len(pullback_dates) == len(ma_columns)):
                                    success.append(pullback_dates)
                            # else:
                            #     print(relevant_data['trade_date'].iloc[j])
                            #     print(relevant_data['close'].iloc[j], relevant_data['close'].iloc[j + 1],
                            #           relevant_data['close'].iloc[j + 2])
                            #     print("{}不满足回踩后连续三天红线".format(df['ts_code'].iloc[0]))
                        else:
                            pullback_dates.append(relevant_data['trade_date'].iloc[j])
                            if (len(set(pullback_dates)) != len(pullback_dates)): # 判断是否在同一天回踩
                                pullback_dates = []
                            if(len(pullback_dates) == len(ma_columns)):
                                success.append(pullback_dates)
                                # last_date_index = j
                                # start_index = find_start_index(df, last_date_index + 1)

                    else:
                        pullback_dates.append(relevant_data['trade_date'].iloc[j])
                    last_date_index = j
                    break

    # end_time1 = time.time()
    # print("len {},time:{}".format(len(start_indexs),(end_time1 - start_time1)))


    if(len(success) > 0):
        # print(success,"==================")
        success.sort(key=lambda x: x[0]) # 排序
        # print(success,"********************")
        current_date = datetime.now()
        specific_date = datetime.strptime(success[-1][-1], "%Y%m%d")
        date_difference = current_date - specific_date
        if(date_difference.days < happen):
            success[-1].insert(0, df['ts_code'].iloc[0])
            # lines = ",".join(success[-1])
            data = (uuid,datetime.now().strftime("%Y%m%d"), history, happen, patience, trend_flag,
                    success[-1][0],success[-1][1],success[-1][2],success[-1][3],success[-1][4],success[-1][5])
            datas.put(data)
            # insert_data(data,op) # 将数据插入数据库
            logger.info(data)
            logger.info("均值分析成功")
            # with open('/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/average_analysis{}_history{}_happen{}_patience{}_trend_flag{}.csv'.format(datetime.now().strftime("%Y%m%d")), 'a') as file:
            #     file.write(lines)
            #     file.write("\n")
            # print(",".join(success[-1]))

def low_point_analysis(df):
    def change_litter(df1):
        for i in range(len(df1)-1):
            change1 = abs((df1['close'].iloc[i] - df1['open'].iloc[i]) / df1['open'].iloc[i])
            change2 = abs((df1['close'].iloc[i+1] - df1['open'].iloc[i+1]) / df1['open'].iloc[i+1])
            change = abs((df1['open'].iloc[i] - df1['open'].iloc[i+1]) / df1['open'].iloc[i]) # 前后两天开盘价差距比较小
            if ((change1 < 0.01) and (change2 < 0.01) and change < 0.002):  # 阈值设为0.01，即1% ,
                # print("代码 {} 在时间 {} 连续两天波动不大".format(df1['ts_code'].iloc[i],df1['trade_date'].iloc[i]))
                return i+1 # 返回波动不大的那一天
        return -1
    # def down(df1):
    #     idx = 0
    #     change = abs((df1['close'].iloc[idx] - df1['open'].iloc[idx]) / df1['open'].iloc[idx]) # 波动
    #     while((df1['close'].iloc[idx] < df1['open'].iloc[idx]) or change < 0.01): # 下跌 or 上升不大
    #         idx = idx + 1
    #         change = abs((df1['close'].iloc[idx] - df1['open'].iloc[idx]) / df1['open'].iloc[idx])  # 波动
    #
    #     return idx # 跌了多少天

    def down(df1):
        idx = 0

        while(idx+1 < len(df1) and df1['open'].iloc[idx] > df1['open'].iloc[idx+1] ): # 下跌

            idx = idx + 1
            # print("代码 {} 在时间 {} 正在下跌，跌了 {} 天,前后两天的开盘价为{}，{}".
            #       format(df1['ts_code'].iloc[idx], df1['trade_date'].iloc[idx],idx,
            #              df1['open'].iloc[idx],df1['open'].iloc[idx+1]))

        return idx # 跌了多少天

    def up(df1):
        flag = False
        if(len(df1) > 2):
            up1 = (df1['close'].iloc[1] > df1['open'].iloc[1])
            up2 = (df1['close'].iloc[2] > df1['open'].iloc[2])
            # print("up1 {} , up2 {}".format(up1,up2))
            if (up1 and up2):
                flag = True
                print("股票代码{}，在时间{}匹配成功".format(df1['ts_code'].iloc[0],df1['trade_date'].iloc[0]))
        return flag

    start_index = 0
    start_index += change_litter(df)
    while(start_index != -1): # 找得到
        relevant_data = df[start_index:]
        if(len(relevant_data) > 2):
            update_index = down(relevant_data)
            update_data = relevant_data[update_index:]
            if(update_index > 3 ):
                up(update_data)
                # print(update_data['ts_code'].iloc[0])

        if(change_litter(df[start_index:]) == -1):
            break
        else:
            start_index += change_litter(df[start_index:])
            # print("start_index:",start_index)

def box_analysis1(df):
    # df.to_csv("box.csv",index=False)
    # start_index = 0
    # ma = df['MA5'].iloc[0]
    # while(1):
    #     count = 0
    #     end_date = df['trade_date'].iloc[0]
    #     for i in range(0,len(df)):
    #         if(df['MA5'].iloc[i] < ma * 1.1 & df['MA5'].iloc[i] > ma * 0.9):
    #             count += 1
    #             end_date = df['trade_date'].iloc[i]
    #         else:
    #             break
    #     if(count < 20):
    #         start_index += 1
    if(len(df) < 1):
        # print("==============")
        return
    if(df['ts_code'].iloc[0].startswith(('00','60'))) :
        thod = 0.05
    elif(df['ts_code'].iloc[0].startswith(('30','68'))):
        thod = 0.1
    else:
        thod = 0.15
    # 转换日期格式
    # df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

    # 定义空列表，用于存储符合条件的时间段
    time_ranges = []

    # 计算滚动窗口的最大最小值
    rolling_max = df['MA5'].rolling(window=30).max()
    rolling_min = df['MA5'].rolling(window=30).min()

    # 计算波动振幅
    amplitude = (rolling_max - rolling_min) / rolling_min

    # 筛选出振幅不超过10%的行
    filtered_indices = amplitude[amplitude <= thod].index

    # 找出连续的时间段
    if len(filtered_indices) > 0:
        start_index = filtered_indices[0]
        end_index = filtered_indices[0]
        for i in range(1, len(filtered_indices)):
            if filtered_indices[i] - end_index == 1:
                end_index = filtered_indices[i]
            else:
                if((end_index - start_index) > 30):
                    time_ranges.append((df['trade_date'][start_index], df['trade_date'][end_index]))
                start_index = filtered_indices[i]
                end_index = filtered_indices[i]
        if ((end_index - start_index) > 30):
            time_ranges.append((df['trade_date'][start_index], df['trade_date'][end_index]))

    # 输出结果
    if time_ranges:
        # print('存在五日均线的波动振幅不超过10%的时间段：')
        # for start, end in time_ranges:
        current_date = datetime.now()
        specific_date = datetime.strptime(time_ranges[-1][1], "%Y%m%d")
        date_difference = current_date - specific_date
        if(date_difference < 30):
            print(f'股票{df["ts_code"].iloc[0]}从 {time_ranges[-1][0]} 到 {time_ranges[-1][1]},存在五日均线的波动振幅不超过15%的时间段')
    else:
        pass
        # print('不存在五日均线的波动振幅不超过10%的时间段。')


    pass

def box_analysis(df):
    def box_flag(df):
        if (df['ts_code'].iloc[0].startswith(('00', '60'))):
            thod = 0.07
        elif (df['ts_code'].iloc[0].startswith(('30', '68'))):
            thod = 0.15
        else:
            thod = 0.2
            # 定义空列表，用于存储符合条件的时间段
        time_ranges = []
        # 计算滚动窗口的最大最小值
        rolling_max = df['MA5'].rolling(window=30).max()
        rolling_min = df['MA5'].rolling(window=30).min()
        # 计算波动振幅
        amplitude = (rolling_max - rolling_min) / rolling_min
        # 筛选出振幅不超过10%的行
        filtered_indices = amplitude[amplitude <= thod].index
        # 找出连续的时间段
        if len(filtered_indices) > 0:

            start_index = filtered_indices[0]
            end_index = filtered_indices[0]
            for i in range(1, len(filtered_indices)):
                if filtered_indices[i] - end_index == 1:
                    end_index = filtered_indices[i]
                else:
                    # if ((end_index - start_index) > 30):
                    # time_ranges.append((df['trade_date'][start_index], df['trade_date'][end_index]))
                    start_index = filtered_indices[i]
                    end_index = filtered_indices[i]
            time_ranges.append((df['trade_date'][start_index], df['trade_date'][end_index]))
            # print(time_ranges)
            return end_index

            # if time_ranges:
            #     if ((int(time_ranges[-1][1]) - int(time_ranges[-1][0]) != 0)):
            #         # print(f'股票{df["ts_code"].iloc[0]}从 {time_ranges[-1][0]} 开始到 {time_ranges[-1][1]} 的历史N天一直处于箱型状态')
            #         return end_index
        return -1


    if (len(df) < 1):
        return
    end_index = box_flag(df)
    # print(end_index,len(df))
    if((end_index!= -1) & ((len(df) - end_index) >=3)): # 箱体结束后放量
        # up1 = (df['close'].iloc[end_index + 1] > df['open'].iloc[end_index + 1])
        # up2 = (df['close'].iloc[end_index + 2] > df['open'].iloc[end_index + 2])
        up1 = True
        up2 = True
        # print(up1,up2)
        if (up1 and up2 ):
            current_date = datetime.now()
            specific_date = datetime.strptime(df['trade_date'][end_index], "%Y%m%d")
            date_difference = current_date - specific_date
            # print(date_difference.days)
            if(date_difference.days < 30):
                print(f'股票{df["ts_code"].iloc[0]}在 {df["trade_date"][end_index]} 结束箱体，并连续两天上涨,并且发生的时间节点在最近5天')



""" TODO 
    枯燥行情
    1、价格波动较低
    2、未做：交易量少（如何定义）
    参数：
    1、min_days = 3 最小连续三天以上的日内波动小，日间方差小
    2、change_threshold = 0.03 日内波动的阈值（公式：|open-close|/(open+close)/2  ）
    3、var_threshold = 0.0005 日间方差阈值（连续多天内的 （open+close）/2 的方差）
    4、history 当前时间之间的history天内有过这样的行情
    5、patience = 1 可以容忍有patience天方差偏离以上标准
    解释：留下来的都是不愿意卖的，通过枯燥行情减少关注度
"""
def Boring_market_trends(df,op,uuid,history = 10,min_days = 4,var_threshold = 0.0005,
                         change_threshold = 0.02,patience = 1):
    if(len(df)<history):
        logger.error("数据长度不够")
        logger.info(df)
        return
    # 1、找出波动率低的索引日期
    df['open_close_diff'] = (df['close'] - df['open']).abs() / ((df['close'] + df['open']) / 2)
    # print(df)
    indices = df['open_close_diff'] < change_threshold
    # print(indices)
    flag = False
    start_date = ""
    end_date = ""
    # 查找的是最近一次发生的时间
    for i in range(len(df), len(df) - 1 - history, -1):
        count = 0
        while(True):
            if all(indices[i - min_days - count : i ]): # 最少连续 min_days 的时间波动率低
                selected_data = df.iloc[i - min_days - count: i]
                # print(selected_data)
                selected_data_var = ((selected_data['close'] + selected_data['open'])/2).var()
                # print("方差为：",selected_data_var)
                if(selected_data_var < var_threshold):
                    flag = True
                    count += 1
                    start_date = selected_data['trade_date'].iloc[0]
                    end_date = selected_data['trade_date'].iloc[-1]
                    return_data = selected_data
                    return_var = selected_data_var
                if(patience <= 0):
                    break
                patience -=1
                count += 1
            else:
                break
        if(flag):
            break

    if(flag):
        # current_date = datetime.now()
        # date_difference = current_date - end_date
        # if (date_difference.days < happen):
        if (1):
            # logger.info(f"股票代码{df['ts_code'].iloc[0]},在{start_date}到{end_date}时间内存在涨跌幅小，波动率低")

            # lines = ','.join([df['ts_code'].iloc[0], start_date, end_date])
            # with open('/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/Boring_market_trends{}history{}_mindays{}_varthreshold{}_changethreshold{}_patience{}.csv'
            #                   .format(datetime.now().strftime("%Y%m%d"),history,min_days,var_threshold,change_threshold,patience), 'a') as file:
            #     file.write(lines)
            #     file.write("\n")

            data = (uuid, history,min_days,var_threshold,change_threshold,patience,df['ts_code'].iloc[0], start_date, end_date)
            logger.info(f"枯燥行情{data}")
            # insert_data(data, op)
            datas.put(data)

        # print(return_data)
        # print("方差为：", return_var)

""" TODO 
    跳空低开，次日收复跳空缺口，三个判断条件
    1、第一天下跌：                      close[i] < open[i]
    2、第二天开盘价和最高点小于前一天最低价， open[i+1] < low[i] and high[i+1] < low[i]
    3、第三天收盘价大于第一天最低价         close[i+2] > low[i]
    参数：
    history 当前时间之间的history天内有过这样的行情
    解释：
    
"""
def find_gap_down_recovery_stocks(df,op,uuid,history = 10):
    df =df.iloc[len(df) - history:].reset_index(drop=True)
    # 检查是否存在跳空低开且次日收复缺口的情况
    flag = False
    start_date = ""
    end_date = ""
    if(len(df)<history):
        return
    for i in range(len(df)-3,0,-1):
        flag1 = df['close'].iloc[i] < df['open'].iloc[i]
        flag2 = (df['open'].iloc[i+1] < df['low'].iloc[i]) & (df['high'].iloc[i+1] < df['low'].iloc[i])
        flag3 = df['close'].iloc[i+2] > df['low'].iloc[i]
        # 判断是否跳空低开
        if flag1 & flag2 & flag3:
            flag = True
            start_date = df['trade_date'].iloc[i]
            end_date = df['trade_date'].iloc[i+2]
            break

    if(flag):
        # logger.info(f"股票代码{df['ts_code'].iloc[0]},在{start_date}到{end_date}时间内跳空低开，然后回补缺口")
        # lines = ','.join([df['ts_code'].iloc[0],start_date,end_date])
        # with open('/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/find_gap_down_recovery_stocks{}_history{}.csv'
        #                   .format(datetime.now().strftime("%Y%m%d"),history), 'a') as file:
        #     file.write(lines)
        #     file.write("\n")
        data = (uuid,history,df['ts_code'].iloc[0],start_date,end_date)
        # insert_data(data, op)
        datas.put(data)


""" TODO
    成交量3~5天红盘，之后3到5天下跌、上涨和下跌之间的成交量相差不超过25%，上涨的量大于下跌的量，不一定阶梯
    今收 > 昨收，成交量就是红的
    参数：
    1、var_threshold = 1 方差：用于控制量与量之间不会相差太大
    2、history = 100 使用历史多少天的数据进行分析
    3、min_windows = 3 最少会有连续的三天上涨之后接着连续的三天下跌
    4、period = 3 历史的history天至少出现过三次以上行情
"""
def trading_volume_trend(df, op, uuid, var_threshold = 1.0, history = 100, min_windows = 3, period = 3):
    # df = df[['ts_code','trade_date','pre_close','close','vol']]
    df = df.iloc[len(df) - history:].reset_index(drop=True)
    if(len(df) < history):
        return
    # 1、区分出红绿成交量
    indicesRed = df['open'] < df['close'] # 为True则为红，为False则为绿
    indicesGreen = df['open'] > df['close'] # 为True则为红，为False则为绿
    # print(indices)
    # 2、找出连续成交量为红与连续成交量为绿的点，同时方差在一定阈值内
    red_index = []
    green_index = []
    for i in range(0,len(df) - min_windows):
        # count_red = 0
        # count_green = 0
        flag_red = False
        flag_green = False
        if(all(indicesRed.iloc[i : i + min_windows])):
            selected_red = df.iloc[i : i + min_windows]
            # print("红盘",selected_red)
            selected_red_var = (selected_red['vol']/selected_red['vol'].mean()).var()
            # print("红盘方差",selected_red_var)
            if(selected_red_var < var_threshold):
                flag_red = True
                # count_red += 1
                if (len(red_index) > 0 and (i < red_index[-1][1])):
                    red_index.pop()
                red_index.append([i, i + min_windows])
                # print("追加红盘：\n",df.iloc[i : i + min_windows])

        elif(all(indicesGreen.iloc[i : i + min_windows])):
            selected_green = df.iloc[i: i + min_windows]
            # print("绿盘",selected_green)
            selected_green_var = (selected_green['vol'] / selected_green['vol'].mean()).var()
            # print("绿盘方差",selected_green_var)
            if (selected_green_var < var_threshold):
                flag_green = True
                # count_green += 1
                # if ((len(green_index) > 0) and ((i > green_index[-1][0]) and i < green_index[-1][1])):
                #     continue
                if (len(green_index) > 0 and (i < green_index[-1][1])):
                    green_index.pop()
                green_index.append([i, i + min_windows])
                # print("追加绿盘：\n", df.iloc[i: i + min_windows])


    # 3、找出两个索引节点能够连上的点，并判断两个集合的成交量哪个更大些
    success = []
    # print(red_index)
    # print(green_index)
    for red in red_index:
        for green in green_index:
            # 判断红区间和绿区间是否相连
            # print(red[1],green[0])
            if red[1] == green[0]:
                # print("success")
                # 计算红区间的总成交量
                red_volume = df.iloc[red[0]:red[1]]['vol'].sum()
                # 计算绿区间的总成交量
                green_volume = df.iloc[green[0]:green[1]]['vol'].sum()
                # print(red_volume,green_volume)
                # 判断哪个区间的成交量更大
                if red_volume >= green_volume:
                    success.append([df['ts_code'].iloc[0],
                                   df['trade_date'].iloc[red[0]],df['trade_date'].iloc[red[1]-1],
                                   df['trade_date'].iloc[green[0]],df['trade_date'].iloc[green[1]-1]])

    if(len(success) >= period):
        # headers = ['ts_code','red_start','red_end','green_start','green_end']
        # data = pd.DataFrame(success,columns=headers)
        # data.to_csv('/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/trading_volume_trend{}_varthreshold{}_history{}_minwindows{}_period{}.csv'.
        #             format(datetime.now().strftime("%Y%m%d"),var_threshold,history,min_windows,period),mode = 'a',header=False,index=False)
        # print(success)
        # print(",".join(success))
        for s in success:
            data = (uuid,history, var_threshold,min_windows,period, s[0],s[1],s[2])
            datas.put(data)
            logger.info(f"成交量连续{data}")
            # insert_data(data, op)


def testmuti():
    ts_codes = getTsCode()
    # 分析数据
    print("正在加载数据...")
    df = loadData("")
    print("加载数据完成")
    for ts_code in ts_codes:
        try:
            df_single = df[df['ts_code'] == ts_code].sort_values(by='trade_date').reset_index(drop=True)
            # find_gap_down_recovery_stocks(df_single)
            # Boring_market_trends(df_single)
            # trading_volume_trend(df_single)
            # print(df_single)
            average_analysis(df_single,args.uuid,args.history,args.happen,args.patience,args.trend_flag)
            # print("正在分析代码{}".format(ts_code))
            # low_point_analysis(df_single)

            # box_analysis(df_single)
        except Exception as e:
            print(f"分析代码 {ts_code} 时出现异常: {e}，继续分析下一个代码...")

# threadname = []

# def analyze_single_code(ts_code,df):
def analyze_single_code(df_single):

    # current_thread = threading.current_thread()
    # threadname.append(current_thread.name)
    # if(len(threadname)%100==0):
    #     print(f"当前线程名称: {current_thread.name}")
    try:
        # 加载最近100个交易日的数据
        # import time
        # logger.info("正在分析处理股票代码：{}".format(args.ts_code))
        start_time = time.time()
        # logger.info("正在分析处理股票代码：{}".format(ts_code))
        # df = loadData(ts_code)
        # df_single = df[df['ts_code'] == ts_code].sort_values(by='trade_date').reset_index(drop=True)
        # df_single = df[df['ts_code'] == ts_code]
        if args.op == 1:
            average_analysis(df_single, args.op, args.uuid, args.history, args.happen, args.patience, args.trend_flag)
        elif args.op == 2:
            Boring_market_trends(df_single, args.op, args.uuid, args.history,args.min_days, args.var_threshold, args.change_threshold, args.patience)
        elif args.op == 3:
            find_gap_down_recovery_stocks(df_single, args.op, args.uuid, args.history)
        elif args.op == 4:
            trading_volume_trend(df_single, args.op, args.uuid, args.var_threshold, args.history, args.min_windows, args.period)
        end_time = time.time()  # 记录结束时间
        # print(f'代码执行时间：{end_time - start_time}秒')

    except Exception as e:
        # logger.error(f"分析代码 {ts_code} 时出现异常: {e}，继续分析下一个代码...")
        logger.error(f"分析代码时出现异常: {e}，继续分析下一个代码...")
        # print(f"分析代码 {ts_code} 时出现异常: {e}，继续分析下一个代码...")

def duoxiancheng(df,max_workers=32):
    ts_codes = getTsCode()
    # 分析数据
    # print("正在加载数据...")
    # df = loadData("")
    # print("加载数据完成")
    df =df.sort_values(by='trade_date').reset_index(drop=True)
    # condition = ((df['MA5'] < df['low']) & (df['MA10'] < df['low']) &
    #              (df['MA20'] < df['low']) & (df['MA30'] < df['low']) &
    #              (df['MA60'] < df['low']))
    # df['average_start_index'] = condition

    analyze_single_code_with_param = partial(analyze_single_code, df=df)

    # print("数据处理完成")

    # 使用线程池进行多线程分析
    # with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # executor.map(analyze_single_code_with_param, ts_codes)
        # print("开始进行多线程分析")
        list(tqdm(executor.map(analyze_single_code_with_param, ts_codes), total=len(ts_codes), desc="处理进度"))



def testsingle(ts_code = '002281.SZ'):
    # 分析数据
    # ts_code = '603366.SH'
    print("正在加载数据...")
    df = loadData(args.ts_code)
    print("加载数据完成")

    # 加载最近100个交易日的数据
    df_single = df[df['ts_code'] == ts_code].sort_values(by='trade_date').reset_index(drop=True)# .iloc[-100:, :].reset_index(drop=True)
    print(df_single)

    # Boring_market_trends(df_single)
    # trading_volume_trend(df_single)

    average_analysis(df_single,args.history,args.happen,args.patience,args.trend_flag)
    # print("正在分析代码{}".format(ts_code))
    # low_point_analysis(df_single)

    # box_analysis(df_single)

# if __name__ == '__main__':
    #  TODO python -u data_analysis.py --history 100 --happen 30 --patience 0.001

# testsingle('000001.SZ')
# testsingle('603366.SH')
# testsingle('839493.BJ')
# testsingle('300766.SZ')
# testsingle('600824.SH')
# testsingle('002281.SZ')
# testmuti()


# TODO 多线程的代码
# file_path = "./data/stock_price_{}.csv".format(datetime.now().strftime("%Y%m%d"))
# logger.info("正在读取文件")
# if os.path.exists(file_path):
#     df = pd.read_csv(file_path)
#     df['trade_date'] = df['trade_date'].astype(str)
#     logger.info(f'{file_path} 文件存在')
# else:
#     df = loadData("")
#     df.to_csv(file_path,index=False)
#
# # df = loadData("")
# logger.info("数据读取完成")
# # print("数据读取完成")
# start_time = time.time()
# duoxiancheng(df , max_workers=16)
# end_time = time.time()  # 记录结束时间
# logger.info(f'代码执行时间：{end_time - start_time}秒')
# # print(f'代码执行时间：{end_time - start_time}秒')
# insert_data(datas,args.op)

# file_path = f"/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_000001.SZ_{datetime.now().strftime('%Y%m%d')}.csv"
# if not os.path.exists(file_path):
#     logger.info("数据加载中")
#     dfall = loadData("")
#     logger.info("数据加载完成")
#     print("数据加载完成")
#     if 'ts_code' in dfall.columns:
#         # 按 'ts_code' 列分组
#         grouped = dfall.groupby('ts_code')
#
#         # 遍历每个分组
#         for ts_code, group in grouped:
#             # 生成文件名
#             file_name = f'/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts_code}_{datetime.now().strftime("%Y%m%d")}.csv'
#             # 将分组数据保存为 CSV 文件
#             group.to_csv(file_name, index=False)
#             logger.info(f"代码{ts_code}写入完成")
#             print(f"代码{ts_code}写入完成")
# logger.info((args.ts_codes)[:-1].split(","))
for ts_code in (args.ts_codes)[:-1].split(","):
    # logger.info(f"当前处理的代码是:{ts_code}")
    # df = pd.read_csv(f'/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts_code}_20250315.csv')
    try:
        df = pd.read_csv(f'/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts_code}_{datetime.now().strftime("%Y%m%d")}.csv')
        df['trade_date'] = df['trade_date'].astype(str)
        df = df.sort_values(by='trade_date').reset_index(drop=True)
        analyze_single_code(df)
    except Exception as e:
        logger.error(f'解析文件/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts_code}_{datetime.now().strftime("%Y%m%d")}.csv出现异常')
insert_data(datas,args.op)


