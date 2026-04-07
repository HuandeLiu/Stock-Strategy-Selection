import pandas as pd

# TODO 股票走势斜率
def trend(df,up_threshold=0.01,down_threshold=-0.01):
    """
    。使用五日均线的斜率判断,斜率计算公式为 diff(1)/pre_ma
    up_threshold:超过该值为上涨趋势，标记为1
    down_threshold:低于该值为下跌趋势，标记为-1
    否则为横盘趋势，标记为0
    :param df:
    :return: 每一行值的标记结果
    """
    # 计算五日均线的一阶差分
    df['MA5_diff'] = df['MA5'].diff(1)
    # 取前一日的5日均线
    df['pre_ma'] = df['MA5'].shift(1)
    # 计算斜率
    df['slope'] = df['MA5_diff'] / df['pre_ma']

    # 根据斜率进行标记
    conditions = [
        df['slope'] > up_threshold,
        df['slope'] < down_threshold
    ]
    choices = [1, -1]
    df['trend_label'] = pd.Series(0, index=df.index)
    df['trend_label'] = df['trend_label'].mask(conditions[0], 1)
    df['trend_label'] = df['trend_label'].mask(conditions[1], -1)

    return df['trend_label']

# TODO 纺锤线：暗示行情转折,实体足够小
def is_spinning_top(df, body_threshold=0.01, shadow_threshold=0.3):
    """
    判断单根K线是否为纺锤线
    :param df: DataFrame的一行数据，包含开盘价、最高价、最低价、收盘价
    :param body_threshold: 实体长度占K线波动范围的最大比例，用于判断实体是否足够小
    :param shadow_threshold: 上下影线长度占K线波动范围的最小比例，用于判断上下影线是否足够长
    :return: True或False，表示是否为纺锤线
    """
    open_price = df['open']
    close_price = df['close']
    high_price = df['high']
    low_price = df['low']

    # 计算K线波动范围
    range_price = high_price - low_price
    # 计算实体长度
    body = abs(open_price - close_price)
    # 计算上影线长度
    upper_shadow = high_price - max(open_price, close_price)
    # 计算下影线长度
    lower_shadow = min(open_price, close_price) - low_price

    # 判断是否为纺锤线 1、实体占比足够小；2、上影线和下影线占比足够大
    # return (body / range_price <= body_threshold) and \
    #     (upper_shadow / range_price >= shadow_threshold) and \
    #     (lower_shadow / range_price >= shadow_threshold)
    return body/((open_price + close_price)/2)<body_threshold
    # df['is_spinning_top'] = df.apply(is_spinning_top, axis=1)

# TODO 伞形线
def is_hammer_or_hanging_line(df, body_threshold=0.3, lower_shadow_threshold=0.6, upper_shadow_threshold=0.1):
    # lower_shadow_threshold = body_threshold * 2 # 下影线长度应该大于实体的两倍
    # upper_shadow_threshold = body_threshold / 2 # 上影线长度应该没有或者很小
    """
    判断单根 K 线是否为伞形线（锤子线或上吊线）
    :param df: DataFrame 的一行数据，包含开盘价、最高价、最低价、收盘价
    :param body_threshold: 实体长度占 K 线波动范围的最大比例，用于判断实体是否足够小
    :param lower_shadow_threshold: 下影线长度占 K 线波动范围的最小比例，大于实体的两倍，0用于判断下影线是否足够长
    :return: True 或 False，表示是否为伞形线
    """
    open_price = df['open']
    close_price = df['close']
    high_price = df['high']
    low_price = df['low']

    # 计算 K 线波动范围
    range_price = high_price - low_price


    # 计算实体长度
    body = abs(open_price - close_price)
    # 计算上影线长度
    upper_shadow = high_price - pd.concat([open_price, close_price], axis=1).max(axis=1)
    # 计算下影线长度
    lower_shadow = pd.concat([open_price, close_price], axis=1).min(axis=1) - low_price

    # 判断是否为伞形线;1、实体足够小；2、上影线短、下影线长；3、实体长度不为0
    return (body / range_price <= body_threshold) & \
           (lower_shadow / range_price >= lower_shadow_threshold) & \
           (upper_shadow / range_price < upper_shadow_threshold) & \
           (body>0)
    # df['is_hammer_or_hanging_man'] = df.apply(is_hammer_or_hanging_man, axis=1)

# TODO 吞没形态
def is_engulfing_pattern (df,threshold=0.5):
    """
    判断 DataFrame 中每一天是否出现看涨吞没形态
    :param df: 包含 'Open', 'High', 'Low', 'Close' 列的 DataFrame
    threshold 为0.5 是刺透或者乌云盖顶状态，为1 是吞没状态
    :return: 1是看涨吞没形态，-1是看跌吞没形态，默认是0
    """
    # 初始化结果 Series，默认值为 False
    result = pd.Series([0] * len(df), index=df.index)

    for i in range(1, len(df)):

        prev_open = df['open'].iloc[i - 1]
        prev_close = df['close'].iloc[i - 1]
        is_prev_bearish = prev_open > prev_close # 前一天是阴线
        is_prev_bullish = prev_open < prev_close # 前一天是阳线

        current_open = df['open'].iloc[i]
        current_close = df['close'].iloc[i]

        is_current_bullish = current_open < current_close # 当天是阳线
        is_current_bearish = current_open > current_close # 当天是阴线

        # 阳线实体完全吞没阴线实体
        is_bullish_engulfing = (current_close > (prev_open * threshold + prev_close * (1 - threshold))) and (current_open < prev_close)
        # 阴线实体完全吞没阳线实体
        is_bearish_engulfing = (current_open > prev_close) and (current_close < (prev_open * threshold + prev_close * (1 - threshold)))

        # 判断是否为看涨吞没形态
        if is_prev_bearish and is_current_bullish and is_bullish_engulfing:
            result.iloc[i] = 1
        # 判断是否为看跌吞没形态
        if is_prev_bullish and is_current_bearish and is_bearish_engulfing:
            result.iloc[i] = -1

    return result

# TODO 星形线
def is_star_candle(df, body_threshold=0.3):
    """
    判断 DataFrame 中每一天是否为星蜡烛线
    :param df: 包含 'Open', 'High', 'Low', 'Close' 列的 DataFrame
    :param body_threshold: 实体长度占 K 线波动范围的最大比例，用于判断实体是否足够小
    :return: 包含判断结果的布尔型 Series
    """
    # 计算 K 线波动范围
    df['range'] = df['high'] - df['low']
    # 计算实体长度
    df['body'] = abs(df['open'] - df['close'])
    # 计算实体长度占 K 线波动范围的比例
    df['body_ratio'] = df['body'] / df['range']

    # 判断是否为星蜡烛线
    result = df['body_ratio'] <= body_threshold
    return result

# TODO 启明星和黄昏星
def is_star_line(df,shadow_threshold=0.6,first_change_threshold=0.03,second_change_threshold=0.01,third_threshold=0.5):
    """
    判断 DataFrame 中每一天是否出现启明星形态
    :param df: 包含 'Open', 'High', 'Low', 'Close' 列的 DataFrame
    shadow_threshold : 实体站k线的比例
    first_change_threshold:下跌多少才算长阴线
    second_change_threshold：波动多少才算小实体
    third_threshold：深入到第一根阳线的比例
    :return: 包含判断结果的布尔型 Series
    """
    result = pd.Series([0] * len(df), index=df.index)
    for i in range(2, len(df)):
        # TODO 启明星
        # 第一根 K 线：长阴线:波动超过阈值、上下影线短
        first_open = df['open'].iloc[i - 2]
        first_close = df['close'].iloc[i - 2]
        is_first_bearish = (first_open > first_close and # 阴线
                            abs(first_open - first_close) > (df['high'].iloc[i - 2] - df['low'].iloc[i - 2]) * shadow_threshold and # 影线小
                            abs(first_open - first_close) / ((first_open + first_close) / 2) > first_change_threshold) # 实体够长

        # 第二根 K 线：小实体 K 线，与第一根阴线有跳空缺口
        second_open = df['open'].iloc[i - 1]
        second_close = df['close'].iloc[i - 1]
        is_second_small = abs(second_open - second_close) / ((second_open + second_close)/2) < second_change_threshold
        is_gap_down = second_high < first_low if (second_high := max(second_open, second_close)) and (first_low := df['close'].iloc[i - 2]) else False

        # 第三根 K 线：长阳线，收盘价深入第一根阴线实体内部
        third_open = df['open'].iloc[i]
        third_close = df['close'].iloc[i]
        is_third_bullish = (third_open < third_close and # 阳线
                            abs(third_open - third_close) > (df['high'].iloc[i] - df['low'].iloc[i]) * shadow_threshold and # 影线小
                            third_close > (first_open * third_threshold + first_close * (1-third_threshold)) ) # 深入第一根阴线内部

        if is_first_bearish and is_second_small and is_gap_down and is_third_bullish:
            result.iloc[i] = 1
            # data = (uuid, ts_code, df['trade_date'].iloc[i], 1, "starPattern", information['reversal']['b3'])
            # datas.put(data)

        # TODO 黄昏星
        # 第一根 K 线：长阳线:波动超过阈值、上下影线短
        is_first_bullish = (first_open < first_close and  # 阳线
                            abs(first_open - first_close) > (df['high'].iloc[i - 2] - df['low'].iloc[i - 2]) * shadow_threshold and  # 影线小
                            abs(first_open - first_close) / ((first_open + first_close) / 2) > first_change_threshold)  # 实体够长

        # 第二根 K 线：小实体 K 线，与第一根阳线有向上跳空缺口
        is_second_small = abs(second_open - second_close) / ((second_open + second_close) / 2) < second_change_threshold
        is_gap_up = second_low > first_high if (second_low := min(second_open, second_close)) and (first_high := df['high'].iloc[i - 2]) else False

        # 第三根 K 线：长阴线，收盘价深入第一根阳线实体内部
        third_open = df['open'].iloc[i]
        third_close = df['close'].iloc[i]
        is_third_bearish = (third_open > third_close and  # 阴线
                            abs(third_open - third_close) > (df['high'].iloc[i] - df['low'].iloc[i]) * shadow_threshold and  # 影线小
                            third_close < (first_open * (1 - third_threshold) + first_close * third_threshold))  # 深入第一根阳线内部

        if is_first_bullish and is_second_small and is_gap_up and is_third_bearish:
            result.iloc[i] = -1
            # data = (uuid, ts_code, df['trade_date'].iloc[i], -1, "starPattern", information['reversal']['s3'])
            # datas.put(data)
    return result

# TODO 倒锤子线
def is_inverted_hammer_line(df, body_threshold=0.1, upper_shadow_threshold=0.6 , down_shadow_threshold=0.1):
    """
    判断 DataFrame 中每一天是否为流星线
    :param df: 包含 'open', 'high', 'low', 'close' 列的 DataFrame
    :param body_threshold: 实体长度占 K 线波动范围的最大比例，用于判断实体是否足够小
    :param upper_shadow_threshold: 上影线长度占 K 线波动范围的最小比例，用于判断上影线是否足够长
    :return: 包含判断结果的布尔型 Series
    """
    open_price = df['open']
    close_price = df['close']
    high_price = df['high']
    low_price = df['low']
    range_price = high_price - low_price

    body = abs(open_price - close_price)
    upper_shadow = high_price - pd.concat([open_price, close_price], axis=1).max(axis=1)
    lower_shadow = pd.concat([open_price, close_price], axis=1).min(axis=1) - low_price

    # 判断是否为流星线
    is_small_body = body / range_price <= body_threshold
    is_long_upper_shadow = upper_shadow / range_price >= upper_shadow_threshold
    is_short_lower_shadow = lower_shadow / range_price < down_shadow_threshold

    return is_small_body & is_long_upper_shadow & is_short_lower_shadow
