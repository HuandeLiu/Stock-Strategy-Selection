package cn.scnu.stock.web.service;

import com.baomidou.mybatisplus.extension.service.IService;
import cn.scnu.voice.web.entity.AverageAnalysis;

import java.util.Map;

/**
 * (AverageAnalysis)表服务接口
 *
 * @author makejava
 * @since 2025-03-02 13:19:49
 */
public interface AverageAnalysisService extends IService<AverageAnalysis> {

    Map<String, Object> dataAnalysis(Integer history, Integer happen, Float patience, Boolean trendFlag);

    Map<String, Object> getDataAnalysis(String uuid);
}

