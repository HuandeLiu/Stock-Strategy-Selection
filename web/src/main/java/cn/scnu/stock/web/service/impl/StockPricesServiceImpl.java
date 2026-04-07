package cn.scnu.stock.web.service.impl;

import cn.scnu.voice.web.mapper.AverageAnalysisMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import cn.scnu.voice.web.mapper.StockPricesMapper;
import cn.scnu.voice.web.entity.StockPrices;
import cn.scnu.voice.web.service.StockPricesService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * (StockPrices)表服务实现类
 *
 * @author makejava
 * @since 2025-03-02 15:59:04
 */
@Service("stockPricesService")
public class StockPricesServiceImpl extends ServiceImpl<StockPricesMapper, StockPrices> implements StockPricesService {
    private static Logger logger = Logger.getLogger(StockPricesServiceImpl.class.getName());

    @Autowired
    private StockPricesMapper stockPricesMapper;

    @Override
    public Map<String, Object> selectStock(String tsCode) {
        logger.info("正在查询股票数据：" + tsCode);
        String msg = "";
        HashMap<String, Object> map = new HashMap<>();

//        String datePattern = "^\\d{8}$";
//        if (!Pattern.matches(datePattern, startDate)) {
//            logger.error("startDate 格式不正确，应为 YYYYMMDD 格式：" + startDate);
//            msg = "startDate 格式不正确，应为 YYYYMMDD 格式：" + startDate;
//        }
//        else {
        QueryWrapper<StockPrices> wrapper = new QueryWrapper<>();
        wrapper.eq("ts_code", tsCode);
//        wrapper.ge("trade_date", startDate);
        List<StockPrices> stockPrices = stockPricesMapper.selectList(wrapper);
        msg = "数据查询成功：" + tsCode;
        map.put("data", stockPrices);
//        }
        map.put("msg", msg);

        return map;
    }
}

