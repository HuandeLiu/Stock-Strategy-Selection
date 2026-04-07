import pandas as pd
import tushare as ts
import pymysql
from datetime import datetime
import logging
import os
import shutil
logging.basicConfig(filename='/vms/sdb/lhd/code/paper/stock/analysis/demo/core/dataAnalysis.log', filemode='a', level=logging.INFO,
                    format='[%(levelname)s][%(asctime)s][%(name)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("getData")

# 初始化pro接口
pro = ts.pro_api('b0bfc5263992ca8de7765a398980a64e15e82dc7dad7f40dcf660b36')

# 创建一个游标对象
def getconnect():
    connection = pymysql.connect(
        # host='10.147.20.202',  # 数据库主机地址
        host='8.148.225.21',  # 数据库主机地址
        #host='10.147.20.212',  # 数据库主机地址
        port=3306,
        # host='localhost',    # 数据库主机地址
        user='root',  # 数据库用户名
        password='liuhuande123',  # 数据库密码
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
    df.to_csv('ts_code.csv', index=False)

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
            logger.error(f"Inserted {cursor.rowcount} rows.")
            # print(f"Inserted {cursor.rowcount} rows.")
    except Exception as e:
        # print("异常股票代码：",ts_code)
        logger.error(f"异常股票代码 {ts_code}")
        logger.error(e)
        # 回滚事务
        connection.rollback()
        # print(f"An error occurred: {e}")
    finally:
        connection.close()

def getDataForDay(start_date="20240224" , end_date="20250224"):
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')
    # 将日期对象转换为字符串格式，这里使用 %Y%m%d 格式
    date_strings = [date.strftime('%Y%m%d') for date in date_range]
    for day in date_strings:
        print("正在查找日期：",day)
        logger.info(f"正在查询日期{day}的股票数据")
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
                logger.info(f'Inserted {cursor.rowcount} rows.')
                # print(f"Inserted {cursor.rowcount} rows.")
        except Exception as e:
            logger.error(f'异常日期：{day}')
            # print("异常日期：",day)
            # 回滚事务
            connection.rollback()
            # print(f"An error occurred: {e}")
            logger.error(f'An error occurred: {e}')
        finally:
            connection.close()

def calculateMa():
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
            cursor.execute(sql, )
            # 提交事务
            connection.commit()
            # 打印插入的行数
            # print(f"update {cursor.rowcount} rows.")
            logger.info(f"update {cursor.rowcount} rows.")
    except Exception as e:
        # print("更新股票均值异常")
        logger.error("更新股票均值异常")
        logger.error(e)
        # 回滚事务
        connection.rollback()
        # print(f"An error occurred: {e}")
    finally:
        connection.close()


def calculateMaNew():
    def update_ma_values(df_ma):
        connection = getconnect()
        try:
            with connection.cursor() as cursor:
                # 分页处理（应对154,146行大数据）
                page_size = 5000
                for i in range(0, len(df_ma), page_size):
                    print("正在更新{}".format(i))
                    batch = df_ma.iloc[i:i + page_size]

                    # 生成动态SQL
                    update_sql = """
                           UPDATE stock_prices 
                           SET MA5 = %s, MA10 = %s, MA20 = %s,
                               MA30 = %s, MA60 = %s, MA90 = %s, MA120 = %s
                           WHERE ts_code = %s AND trade_date = %s
                       """
                    # 转换为参数元组列表
                    params = [(
                        row['MA5'], row['MA10'], row['MA20'],
                        row['MA30'], row['MA60'], row['MA90'], row['MA120'],
                        row['ts_code'], row['trade_date']
                    ) for _, row in batch.iterrows()]

                    cursor.executemany(update_sql, params)
                    connection.commit()
                    logger.info(f"已更新 {i + len(batch)} 条记录")
        finally:
            connection.close()

    df = loadData("")
    df_grouped = df.groupby('ts_code', group_keys=False)
    df_sorted = df_grouped.apply(lambda x: x.sort_values('trade_date'))
    df_sorted = df_sorted.reset_index(drop=True)
    windows = {
        'MA5': 5,
        'MA10': 10,
        'MA20': 20,
        'MA30': 30,
        'MA60': 60,
        'MA90': 90,
        'MA120': 120
    }

    for ma, window in windows.items():
        df_sorted[ma] = df_sorted.groupby('ts_code', group_keys=False)['close'] \
            .apply(lambda x: x.rolling(window=window, min_periods=1).mean()).round(2)
    df_ma = df_sorted[['MA5', 'MA10', 'MA20', 'MA30', 'MA60', 'MA90', 'MA120', 'ts_code', 'trade_date']]
    update_ma_values(df_ma)

# 加载最近n天的数据
def loadData(ts_code=""):
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
            # print(sql)
            # logger.info(sql)

            # cursor.execute(sql,(ts_code,))
            cursor.execute(sql)
            results = cursor.fetchall()

            # 将结果转换为DataFrame
            df = pd.DataFrame(results)
            # df.to_csv("stock_prices_{}.csv".format(ts_code), index=False)
            return df
    except Exception as e:
        # print("异常股票代码：", ts_code)
        # print(f"An error occurred: {e}")
        logger.error(f'股票{ts_code}数据加载异常')
        logger.error(e)
    finally:
        connection.close()

def writer_local_all():
    def delete_all_files_in_folder(folder_path):
        # 遍历文件夹中的所有内容
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            # 如果是文件，则删除
            if os.path.isfile(file_path):
                os.remove(file_path)
            # 如果是文件夹，则递归删除文件夹及其内容
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    file_path = f"/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_000001.SZ_{datetime.now().strftime('%Y%m%d')}.csv"
    file_dir = f"/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data"
    delete_all_files_in_folder(file_dir)
    # if not os.path.exists(file_path):
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

def writer_local(ts_codes):
    def delete_all_files_in_folder(folder_path):
        # 遍历文件夹中的所有内容
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            # 如果是文件，则删除
            if os.path.isfile(file_path):
                os.remove(file_path)
            # 如果是文件夹，则递归删除文件夹及其内容
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    file_path = f"/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_000001.SZ_{datetime.now().strftime('%Y%m%d')}.csv"
    file_dir = f"/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data"
    delete_all_files_in_folder(file_dir)
    # if not os.path.exists(file_path):
    for ts in ts_codes:
        dfall = loadData(ts)
        file_name = f'/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/stock_price_{ts}_{datetime.now().strftime("%Y%m%d")}.csv'
        # 将分组数据保存为 CSV 文件
        dfall.to_csv(file_name, index=False)
        # logger.info(f"代码{ts_code}写入完成")
        print(f"代码{ts}写入完成")



if __name__ == '__main__':
    logger.info("正在拉取股票代码")
    ts_codes = getTsCode()
    logger.info("正在拉取股票数据")
    #getDataForDay("20250419","20250423")
    getDataForDay(datetime.now().strftime("%Y%m%d"),datetime.now().strftime("%Y%m%d"))
    logger.info("正在计算股票均值。。。")
    # logger.info("正在计算股票均值。。。")
    calculateMaNew()
    logger.info("股票均值计算完成。。。")
    # print("股票均值计算完成。。。")
    logger.info("文件写入本地")
    writer_local(ts_codes)






    # # TODO 将数据写入到数据库
    # for ts_code in ts_codes:
    #     print(ts_code)
    #     calculateMa(ts_code) # 计算均值
    # calculateMa('002281.SZ')
    # TODO 获取数据
    # loadData("")

    # calculateMa("abc")