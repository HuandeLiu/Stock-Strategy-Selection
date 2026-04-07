use stock;
drop table stock_prices;
create table stock_prices
(
    ts_code    varchar(255) null,
    trade_date varchar(255) null,
    open       float        null,
    high       float        null,
    low        float        null,
    close      float        null,
    pre_close  float        null,
    changed    float        null,
    pct_chg    float        null,
    vol        float        null,
    amount     float        null,
    MA5        float        null,
    MA10       float        null,
    MA20       float        null,
    MA30       float        null,
    MA60       float        null,
    MA90       float        null,
    MA120       float        null
);

CREATE INDEX idx_stock_prices_ts_code_trade_date ON stock_prices (ts_code, trade_date);

use stock;
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
      ;



SELECT * FROM information_schema.INNODB_TRX;



 select b.SID, b.USERNAME, b.SERIAL#, spid, paddr, sql_text, b.MACHINE
   from v$process a, v$session b, v$sqlarea c
  where a.ADDR = b.PADDR
    and b.SQL_HASH_VALUE = c.HASH_VALUE;



 show full processlist;

SHOW OPEN TABLES WHERE In_use > 0;  -- 查看被锁定的表

SELECT * FROM information_schema.INNODB_TRX;  -- 查看未提交事务

select a.*,b.* from v$locked_object a,dba_objects b where b.object_id = a.object_id


# 每5秒检测长事务
SELECT COUNT(*) FROM information_schema.INNODB_TRX

show databases;
update average_analysis set id ='111'

show index from stock_prices_test;