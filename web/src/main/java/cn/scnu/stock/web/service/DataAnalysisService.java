package cn.scnu.stock.web.service;

import cn.scnu.voice.web.entity.AverageAnalysis;
import cn.scnu.voice.web.entity.Params;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.util.Map;

public interface DataAnalysisService extends IService<Params> {
    Map<String, String> dataAnalysis(Params params) throws IOException, ConfigurationException;

    Map<String, Object> getDataAnalysis(Integer op,String uuid);
}
