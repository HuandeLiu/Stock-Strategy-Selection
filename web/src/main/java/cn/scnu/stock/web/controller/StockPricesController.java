package cn.scnu.stock.web.controller;



import cn.scnu.voice.web.entity.StockPrices;
import cn.scnu.voice.web.service.StockPricesService;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Map;

/**
 * (StockPrices)表控制层
 *
 * @author makejava
 * @since 2025-03-02 15:58:58
 */
@RestController
@CrossOrigin
@RequestMapping("/stockPrices")
public class StockPricesController{
    /**
     * 服务对象
     */
    @Resource
    private StockPricesService stockPricesService;

    // 获取股票数据
    @PostMapping("/selectStock")
    public Map<String, Object> selectStock(@ApiParam(value = "ts_code",required = true) String ts_code) {
        return stockPricesService.selectStock(ts_code);
    }

}


