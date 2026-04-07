import pandas as pd
import tushare as ts
import pymysql
import os
from datetime import datetime
# 初始化pro接口
pro = ts.pro_api('b0bfc5263992ca8de7765a398980a64e15e82dc7dad7f40dcf660b36')

# 创建一个游标对象
def getconnect():
    connection = pymysql.connect(
        host='10.147.20.202',  # 数据库主机地址
        # host='localhost',    # 数据库主机地址
        user='root',  # 数据库用户名
        password='123456',  # 数据库密码
        database='stock',  # 数据库名称
        charset='utf8mb4',  # 字符集
        cursorclass=pymysql.cursors.DictCursor  # 返回字典形式的结果
    )
    return connection
def getTsCode():
    # 拉取数据
    # TODO 获取股票的基本信息
    df = pro.stock_basic(**{
        "ts_code": "",
        "name": "",
        "exchange": "",
        "market": "",
        "is_hs": "",
        "list_status": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "cnspell",
        "market",
        "list_date",
        "act_name",
        "act_ent_type"
    ])
    # df.to_csv('ts_code.csv', index=False)

    # df =pd.read_csv('ts_code.csv')
    return df['ts_code']

# 按照股票代码和日期获取数据，传入空值时为获取全量
def getDataAndWrite(ts_code , start_date="" , end_date=""):
    # 拉取数据
    df = pro.daily(**{
        "ts_code": "{}".format(ts_code),
        "trade_date": "",
        "start_date": "{}".format(start_date),
        "end_date": "{}".format(end_date),
        "offset": "",
        "limit": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    df = df.iloc[::-1] # 逆序
    # df['MA5'] = df['close'].rolling(window=5).mean().round(4)
    # df['MA10'] = df['close'].rolling(window=10).mean().round(4)
    # df['MA20'] = df['close'].rolling(window=20).mean().round(4)
    # df['MA30'] = df['close'].rolling(window=30).mean().round(4)
    # df['MA60'] = df['close'].rolling(window=60).mean().round(4)
    # df = df.iloc[60:, :]
    # df = df.fillna(0.0, inplace=True)

    # print(df)

    connection = getconnect()
    try:
        # 创建一个游标对象
        with connection.cursor() as cursor:
            # 定义批量插入的 SQL 语句
            # sql = ("INSERT INTO `stock_prices` (`ts_code`, `trade_date`, `open`,`high`, `low`, "
            #        "`close`,`pre_close`, `changed`, `pct_chg`, `vol`, `amount`,`MA5`,`MA10`,`MA20`,`MA30`,`MA60`)"
            #        " VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s)")

            sql = ("INSERT INTO `stock_prices` (`ts_code`, `trade_date`, `open`,`high`, `low`, "
                   "`close`,`pre_close`, `changed`, `pct_chg`, `vol`, `amount`)"
                   " VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)")

            # 准备批量插入的数据
            data_list = [tuple(row) for row in df.itertuples(index=False)]
            # 执行批量插入操作
            cursor.executemany(sql, data_list)
            # 提交事务
            connection.commit()
            # 打印插入的行数
            print(f"Inserted {cursor.rowcount} rows.")
    except Exception as e:
        print("异常股票代码：",ts_code)
        # 回滚事务
        connection.rollback()
        print(f"An error occurred: {e}")
    finally:
        connection.close()

def getDataForDay(start_date="20240224" , end_date="20250224"):
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')
    # 将日期对象转换为字符串格式，这里使用 %Y%m%d 格式
    date_strings = [date.strftime('%Y%m%d') for date in date_range]
    for day in date_strings:
        print("正在查找日期：",day)
        df = pro.daily(trade_date=day)
        connection = getconnect()
        try:
            # 创建一个游标对象
            with connection.cursor() as cursor:
                # 定义批量插入的 SQL 语句
                # sql = ("INSERT INTO `stock_prices` (`ts_code`, `trade_date`, `open`,`high`, `low`, "
                #        "`close`,`pre_close`, `changed`, `pct_chg`, `vol`, `amount`,`MA5`,`MA10`,`MA20`,`MA30`,`MA60`)"
                #        " VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s)")

                sql = ("INSERT INTO `stock_prices` (`ts_code`, `trade_date`, `open`,`high`, `low`, "
                       "`close`,`pre_close`, `changed`, `pct_chg`, `vol`, `amount`)"
                       " VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)")

                # 准备批量插入的数据
                data_list = [tuple(row) for row in df.itertuples(index=False)]
                # 执行批量插入操作
                cursor.executemany(sql, data_list)
                # 提交事务
                connection.commit()
                # 打印插入的行数
                print(f"Inserted {cursor.rowcount} rows.")
        except Exception as e:
            print("异常日期：",day)
            # 回滚事务
            connection.rollback()
            print(f"An error occurred: {e}")
        finally:
            connection.close()

# UPDATE stock_prices sp JOIN (
#                 SELECT trade_date,
#                     AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW ) AS MA5,
#                     AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS MA10,
#                     AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) AS MA20,
#                     AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS MA30,
#                     AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW ) AS MA60,
#                     AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW ) AS MA90,
#                     AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW ) AS MA120
#                 FROM stock_prices WHERE ts_code = %s) AS subquery ON sp.trade_date = subquery.trade_date
#                 SET sp.MA5 = subquery.MA5, sp.MA10 = subquery.MA10,
#                 sp.MA20 = subquery.MA20, sp.MA30 = subquery.MA30,
#                 sp.MA60 = subquery.MA60,sp.MA90 = subquery.MA90,sp.MA120 = subquery.MA120 WHERE ts_code = %s;
def calculateMa(ts_code):
    connection = getconnect()
    try:
        # 创建一个游标对象
        with connection.cursor() as cursor:
            # 定义批量插入的 SQL 语句
            # sql = ("INSERT INTO `stock_prices` (`ts_code`, `trade_date`, `open`,`high`, `low`, "
            #        "`close`,`pre_close`, `changed`, `pct_chg`, `vol`, `amount`,`MA5`,`MA10`,`MA20`,`MA30`,`MA60`)"
            #        " VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s)")

            # sql = """UPDATE stock_prices sp JOIN (
            #     SELECT trade_date,
            #         AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW ) AS MA5,
            #         AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS MA10,
            #         AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) AS MA20,
            #         AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS MA30,
            #         AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW ) AS MA60,
            #         AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW ) AS MA90,
            #         AVG(close) OVER ( ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW ) AS MA120
            #     FROM stock_prices GROUP BY ts_code ) AS subquery ON sp.trade_date = subquery.trade_date
            #     SET sp.MA5 = subquery.MA5, sp.MA10 = subquery.MA10,
            #     sp.MA20 = subquery.MA20, sp.MA30 = subquery.MA30,
            #     sp.MA60 = subquery.MA60,sp.MA90 = subquery.MA90,sp.MA120 = subquery.MA120 WHERE ts_code = %s;"""
            # print(sql)

            sql = """ 
            UPDATE stock_prices sp JOIN(
                  SELECT trade_date,ts_code,
                  AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW ) AS MA5,
                  AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS MA10,
                  AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) AS MA20,
                  AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS MA30,
                  AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW ) AS MA60,
                  AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW ) AS MA90,
                  AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW ) AS MA120
                  FROM stock_prices) AS subquery ON sp.trade_date = subquery.trade_date AND sp.ts_code = subquery.ts_code
                  SET sp.MA5 = subquery.MA5, sp.MA10 = subquery.MA10,
                  sp.MA20 = subquery.MA20, sp.MA30 = subquery.MA30,
                  sp.MA60 = subquery.MA60,sp.MA90 = subquery.MA90,
                  sp.MA120 = subquery.MA120
                  ;"""

            # cursor.execute(sql,(ts_code,ts_code) )
            cursor.execute(sql,)
            # 提交事务
            connection.commit()
            # 打印插入的行数
            print(f"update {cursor.rowcount} rows.")
    except Exception as e:
        print("异常股票代码：",ts_code)
        # 回滚事务
        connection.rollback()
        print(f"An error occurred: {e}")
    finally:
        connection.close()


# 加载最近n天的数据
def loadData(ts_code):
    connection = getconnect()
    try:
        # 创建一个游标对象
        with connection.cursor() as cursor:
            # 定义批量插入的 SQL 语句
            # sql = ("SELECT `ts_code`,`trade_date`, `open`,`high`, `low`,`close`,`MA5`,`MA10`,`MA20`,`MA30`,`MA60` "
                   # "from `stock_prices` where ts_code='300638.SZ'")
            if ts_code:
                sql = ("SELECT * from `stock_prices` where ts_code = '{}'".format(ts_code))
            else:
                # sql = ("SELECT `ts_code`,`trade_date`, `open`,`high`, `low`,`close`,`MA5`,`MA10`,`MA20`,`MA30`,`MA60` "
                #        "from `stock_prices` ")
                sql = ("SELECT  * from `stock_prices`")
                       # "where ts_code like '3007%'")
            print(sql)
            # cursor.execute(sql,(ts_code,))
            cursor.execute(sql)
            results = cursor.fetchall()

            # 将结果转换为DataFrame
            df = pd.DataFrame(results)
            # df.to_csv("stock_prices_{}.csv".format(ts_code), index=False)
            return df
    except Exception as e:
        print("异常股票代码：", ts_code)
        print(f"An error occurred: {e}")
    finally:
        connection.close()

def writer_local():
    file_path = f"/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_000001.SZ_{datetime.now().strftime('%Y%m%d')}.csv"
    if not os.path.exists(file_path):
        dfall = loadData("")
        # logger.info("数据加载完成")
        print("数据加载完成")
        if 'ts_code' in dfall.columns:
            # 按 'ts_code' 列分组
            grouped = dfall.groupby('ts_code')

            # 遍历每个分组
            for ts_code, group in grouped:
                # 生成文件名
                file_name = f'/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts_code}_{datetime.now().strftime("%Y%m%d")}.csv'
                # 将分组数据保存为 CSV 文件
                group.to_csv(file_name, index=False)
                # logger.info(f"代码{ts_code}写入完成")
                print(f"代码{ts_code}写入完成")

# if __name__ == '__main__':
print("正在拉取股票代码")
ts_codes = getTsCode()
print("正在将股票数据写入数据库")
getDataForDay("20250314","20250316")
# TODO 将数据写入到数据库
print("正在更新均值")
calculateMa("abc")
# TODO写入到linux本地
print("文件写入本地")
writer_local()

#
# for ts_code in ts_codes:
#     print(ts_code)
#     calculateMa(ts_code) # 计算均值

# TODO 获取数据
# loadData("")



