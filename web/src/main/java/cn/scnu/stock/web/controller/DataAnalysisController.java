package cn.scnu.stock.web.controller;


import cn.scnu.voice.web.entity.Params;
import cn.scnu.voice.web.service.AverageAnalysisService;
import cn.scnu.voice.web.service.DataAnalysisService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

@CrossOrigin
@RestController
@RequestMapping("/dataAnalysis")
public class DataAnalysisController {
    @Resource
    private DataAnalysisService dataAnalysisService;

    @ApiOperation(value = "数据分析")
    @PostMapping("/dataAnalysis")
    public Map<String, String> dataAnalysis(@ApiParam(value = "params",required = true)@RequestBody Params params
    ) throws ConfigurationException, IOException {
        return dataAnalysisService.dataAnalysis(params);
    }

    @ApiOperation(value = "获取到经过分析后的数据")
    @PostMapping("/getDataAnalysis")
    public Map<String, Object> getDataAnalysis(@ApiParam(value = "op",required = true) Integer op,
                                                @ApiParam(value = "uuid",required = true) String uuid) {
        return dataAnalysisService.getDataAnalysis(op,uuid);
    }
}
