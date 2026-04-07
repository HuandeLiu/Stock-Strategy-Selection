package cn.scnu.stock.web.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * (StockPrices)表实体类
 *
 * @author makejava
 * @since 2025-03-02 15:59:04
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("stock_prices")
//@ApiModel(value="StockPrices", description="")
//@SuppressWarnings("serial")
public class StockPrices extends Model<StockPrices> {

    private String ts_code;

    private String trade_date;

    private Float open;

    private Float high;

    private Float low;

    private Float close;

    private Float pre_close;

    private Float changed;

    private Float pct_chg;

    private Float vol;

    private Float amount;

    private Float ma5;

    private Float ma10;

    private Float ma20;

    private Float ma30;

    private Float ma60;

    private Float ma90;

    private Float ma120;




}

