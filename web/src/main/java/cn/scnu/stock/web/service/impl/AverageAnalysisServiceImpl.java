package cn.scnu.stock.web.service.impl;

import cn.scnu.voice.web.mapper.PersonMapper;
import cn.scnu.voice.web.utils.JavaPythonIntegration;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import cn.scnu.voice.web.mapper.AverageAnalysisMapper;
import cn.scnu.voice.web.entity.AverageAnalysis;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import cn.scnu.voice.web.service.AverageAnalysisService;

import java.util.*;

/**
 * (AverageAnalysis)表服务实现类
 *
 * @author makejava
 * @since 2025-03-02 13:19:49
 */
@Service("averageAnalysisService")
public class AverageAnalysisServiceImpl extends ServiceImpl<AverageAnalysisMapper, AverageAnalysis> implements AverageAnalysisService {
    private static Logger logger = Logger.getLogger(AverageAnalysisServiceImpl.class.getName());

    @Autowired
    private AverageAnalysisMapper averageAnalysisMapper;

    @Override
    public Map<String, Object> dataAnalysis(Integer history, Integer happen, Float patience, Boolean trendFlag) {
        String uuid = UUID.randomUUID().toString();
        logger.info("正在进行均值分析，参数列表为: "+ uuid + " " +history + " " + happen + " " + patience + " " + trendFlag);
        HashMap<String, String> params = new HashMap<>();
        params.put("op", String.valueOf(1)); // 这个要跟python代码匹配上
        params.put("uuid", uuid);
        params.put("history", String.valueOf(history));
        params.put("happen", String.valueOf(happen));
        params.put("patience", String.valueOf(patience));
        params.put("trend_flag", String.valueOf(trendFlag));

//        Date currentDate = new Date();
//        // 定义日期格式
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        // 格式化日期
//        String formattedDate = sdf.format(currentDate);
//
//        String filePath = "/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data/"
//                +"average_analysis"+formattedDate+"_history"+history+"_happen"+happen+"_patience"+patience+"_trend_flag"+String.valueOf(trendFlag)+".csv";
//        File file = new File(filePath);
//        if (file.exists()) {
//            System.out.println("文件存在");
//        }else {
//            System.out.println("文件不存在");
//        }

        JavaPythonIntegration.exec("/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data_analysis.py",params);
        logger.info("数据分析完成....");

        HashMap<String, Object> map = new HashMap<>();
        map.put("uuid", uuid);
        map.put("history", String.valueOf(history));
        map.put("happen", String.valueOf(happen));
        map.put("patience", String.valueOf(patience));
        map.put("trendFlag", String.valueOf(trendFlag));
        map.put("msg","数据分析完成");
        return map;
    }

    @Override
    public Map<String, Object> getDataAnalysis(String uuid) {
        logger.info("正在查找uuid为："+uuid+"的数据...");
        QueryWrapper<AverageAnalysis> wrapper = new QueryWrapper<>();
        wrapper.eq("id", uuid);
        List<AverageAnalysis> data = averageAnalysisMapper.selectList(wrapper);

        HashMap<String, Object> map = new HashMap<>();
        map.put("uuid", uuid);
        map.put("data", data);
        return map;
    }
}

