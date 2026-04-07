use stock;
ALTER TABLE `dbbaroverview` MODIFY COLUMN `symbol` VARCHAR(45) BINARY;

ALTER TABLE `dbtickoverview` MODIFY COLUMN `symbol` VARCHAR(45) BINARY;

ALTER TABLE `dbbardata` MODIFY COLUMN `symbol` VARCHAR(45) BINARY;

ALTER TABLE `dbtickdata` MODIFY COLUMN `symbol` VARCHAR(45) BINARY;

UPDATE  stock_prices
SELECT
    trade_date,
    close,
    AVG(close) OVER (
        ORDER BY trade_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS MA5,
    AVG(close) OVER (
        ORDER BY trade_date
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS MA10,
    AVG(close) OVER (
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS MA20,
    AVG(close) OVER (
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS MA30,
    AVG(close) OVER (
        ORDER BY trade_date
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS MA50
FROM
    stock_prices
ORDER BY
    trade_date;


UPDATE stock_prices sp
JOIN (
    SELECT
        trade_date,
        AVG(close) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS MA5,
        AVG(close) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS MA10,
        AVG(close) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS MA20,
        AVG(close) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS MA30,
        AVG(close) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS MA50
    FROM stock_prices
) AS subquery
ON sp.trade_date = subquery.trade_date
SET
    sp.MA5 = subquery.MA5,
    sp.MA10 = subquery.MA10,
    sp.MA20 = subquery.MA20,
    sp.MA30 = subquery.MA30,
    sp.MA60 = subquery.MA50;


RENAME TABLE stock_prices TO stock_prices_bak;
drop table stock_prices;

create database stock;
use stock;
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



SELECT trade_date,ts_code,
       AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW ) AS MA5,
       AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW ) AS MA10,
       AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) AS MA20,
       AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) AS MA30,
       AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW ) AS MA60,
       AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW ) AS MA90,
       AVG(close) OVER ( PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW ) AS MA120
       FROM stock_prices;

use stock;
SHOW INDEX FROM stock_prices;


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


use stock;
RENAME TABLE stock_prices TO stock_prices_bak;

