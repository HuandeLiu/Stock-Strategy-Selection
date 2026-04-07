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

-- 创建分析表，存储股票筛选后的统计结果
# drop table average_analysis;
CREATE TABLE average_analysis (
    id VARCHAR(255) NOT NULL,
    select_date    VARCHAR(20),
    history        INT DEFAULT 100,
    happen         INT DEFAULT 30 ,
    patience       Float DEFAULT 0.0,
    trend_flag     bool DEFAULT FALSE,
    ts_code        VARCHAR(20),
    MA5DATE        VARCHAR(20) ,
    MA10DATE       VARCHAR(20),
    MA20DATE       VARCHAR(20),
    MA30DATE       VARCHAR(20),
    MA60DATE       VARCHAR(20)
);

# drop table boring_market_trends;
CREATE table boring_market_trends(
    id VARCHAR(255) NOT NULL,
    history        INT DEFAULT 10,
    min_days        INT DEFAULT 4,
    var_threshold        float DEFAULT 0.0005,
    change_threshold        float DEFAULT 0.02,
    patience        INT DEFAULT 1,
    ts_code        VARCHAR(20),
    start_date     VARCHAR(20) ,
    end_date     VARCHAR(20)
);

# drop table find_gap_down_recovery_stocks;
create table find_gap_down_recovery_stocks(
    id VARCHAR(255) NOT NULL,
    history        INT DEFAULT 10,
    ts_code        VARCHAR(20),
    start_date     VARCHAR(20) ,
    end_date     VARCHAR(20)
);

# drop table trading_volume_trend;
create table trading_volume_trend(
    id VARCHAR(255) NOT NULL,
    history        INT DEFAULT 100,
    var_threshold        float DEFAULT 1.0,
    min_windows        INT DEFAULT 3,
    periods        INT DEFAULT 3,
    ts_code        VARCHAR(20),
    start_date     VARCHAR(20) ,
    end_date     VARCHAR(20)
);

drop table operator;
create table operator(
    id VARCHAR(255) NOT NULL,
    ts_code VARCHAR(20),
    happen_date VARCHAR(20),
    information int default 0, -- 为1是买入信号，为-1是卖出信号
    feature VARCHAR(255), -- 符合什么特征
    introduce text
)