package cn.scnu.stock.web.controller;



import cn.scnu.voice.web.entity.AverageAnalysis;
import cn.scnu.voice.web.service.AverageAnalysisService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Map;

/**
 * (AverageAnalysis)表控制层
 *
 * @author makejava
 * @since 2025-03-02 13:19:42
 */
@CrossOrigin
@RestController
@RequestMapping("/averageAnalysis")
@Api(value = "示例 API", description = "示例 API 接口描述")
public class AverageAnalysisController  {
    /**
     * 服务对象
     */
    @Resource
    private AverageAnalysisService averageAnalysisService;

    @ApiOperation(value = "数据分析")
    @PostMapping("/dataAnalysis")
    public Map<String, Object> dataAnalysis(@ApiParam(value = "history",required = true) Integer history,
                                            @ApiParam(value = "happen",required = true) Integer happen,
                                            @ApiParam(value = "patience",required = false) Float patience,
                                            @ApiParam(value = "trend_flag",required = false) Boolean trend_flag
                                            ) {
        return averageAnalysisService.dataAnalysis(history, happen, patience, trend_flag);
    }

    @ApiOperation(value = "获取到经过分析后的数据")
    @PostMapping("/getDataAnalysis")
    public Map<String, Object> getDataAnalysis(@ApiParam(value = "uuid",required = true) String uuid) {
        return averageAnalysisService.getDataAnalysis(uuid);
    }

}

