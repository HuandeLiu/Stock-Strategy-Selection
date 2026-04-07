package cn.scnu.stock.web.service;

import com.baomidou.mybatisplus.extension.service.IService;
import cn.scnu.voice.web.entity.StockPrices;

import java.util.Map;

/**
 * (StockPrices)表服务接口
 *
 * @author makejava
 * @since 2025-03-02 15:59:04
 */
public interface StockPricesService extends IService<StockPrices> {

    Map<String, Object> selectStock(String tsCode);
}

