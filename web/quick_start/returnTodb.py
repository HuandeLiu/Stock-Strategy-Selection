from dataupdate import getconnect
import logging
logging.basicConfig(filename='/vms/sdb/lhd/code/paper/stock/analysis/demo/core/dataAnalysis.log', filemode='a', level=logging.INFO,
                    format='[%(levelname)s][%(asctime)s][%(name)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("getData")
def insert_data(datas,op):
    connection = getconnect()
    try:
        # 创建一个游标对象
        with connection.cursor() as cursor:
            # 定义批量插入的 SQL 语句
            if op == 1: # 均值分析
                sql = ("INSERT INTO `average_analysis` VALUES (%s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)")
            if op == 2: # 枯燥行情
                sql = ("INSERT INTO `boring_market_trends` VALUES (%s,%s, %s, %s,%s, %s, %s,%s, %s)")
            if op == 3: # 枯燥行情
                sql = ("INSERT INTO `find_gap_down_recovery_stocks` VALUES (%s,%s, %s, %s,%s)")
            if op == 4: # 枯燥行情
                sql = ("INSERT INTO `trading_volume_trend` VALUES (%s,%s, %s, %s,%s, %s, %s,%s)")
            data_list = []
            while not datas.empty():
                data_list.append(datas.get())

            # 执行批量插入操作
            cursor.executemany(sql, data_list)
            # cursor.execute(sql, datas)
            # 提交事务
            connection.commit()
            # 打印插入的行数
            # logger.info(f"insert data success,{datas}")
            # print(f"Inserted {cursor.rowcount} rows.")
    except Exception as e:
        # print("分析结果插入异常",data)
        logger.error(f"数据插入异常{e}")
        # 回滚事务
        connection.rollback()
    finally:
        connection.close()