package cn.scnu.stock.web.service.impl;

import cn.scnu.voice.web.entity.*;
import cn.scnu.voice.web.mapper.*;
import cn.scnu.voice.web.service.DataAnalysisService;
import cn.scnu.voice.web.utils.JavaPythonIntegration;
import cn.scnu.voice.web.utils.ReadDataFromFile;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.configuration2.INIConfiguration;
import org.apache.commons.configuration2.SubnodeConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service("dataAnalysisService")
public class DataAnalysisServiceImpl extends ServiceImpl<DataAnalysisServiceMapper, Params> implements DataAnalysisService {
    private static Logger logger = Logger.getLogger(AverageAnalysisServiceImpl.class.getName());

    @Autowired
    private AverageAnalysisMapper averageAnalysisMapper;

    @Autowired
    private BoringMarketTrendsMapper boringMarketTrendsMapper;

    @Autowired
    private FindGapDownRecoveryStocksMapper findGapDownRecoveryStocksMapper;

    @Autowired
    private TradingVolumeTrendMapper tradingVolumeTrendMapper;

    public String check(Params params){
        String uuid ="0000";
        Date currentDate = new Date();
        // 定义日期格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        // 格式化日期
        String formattedDate = sdf.format(currentDate);
        Integer op = params.getOp();
        if (op == 1){
            QueryWrapper<AverageAnalysis> wrapper = new QueryWrapper<>();
            wrapper.eq("history",params.getHistory());
            wrapper.eq("happen",params.getHappen());
            wrapper.eq("patience",params.getPatience());
            wrapper.eq("trend_flag",params.isTrend_flag());
            AverageAnalysis averageAnalysis = averageAnalysisMapper.selectOne(wrapper);
            if (averageAnalysis != null){
                uuid = averageAnalysis.getId();
            }
        }
        if (op == 2){
            QueryWrapper<BoringMarketTrends> wrapper = new QueryWrapper<>();
            wrapper.eq("history",params.getHistory());
            wrapper.eq("min_days",params.getMin_days());
            wrapper.eq("var_threshold",params.getVar_threshold());
            wrapper.eq("change_threshold",params.getChange_threshold());
            wrapper.eq("patience",params.getPatience());
            BoringMarketTrends boringMarketTrends = boringMarketTrendsMapper.selectOne(wrapper);
            if (boringMarketTrends != null){
                uuid = boringMarketTrends.getId();
            }
        }
        if (op == 3){
            QueryWrapper<FindGapDownRecoveryStocks> wrapper = new QueryWrapper<>();
            wrapper.eq("history",params.getHistory());
            FindGapDownRecoveryStocks findGapDownRecoveryStocks = findGapDownRecoveryStocksMapper.selectOne(wrapper);
            if (findGapDownRecoveryStocks != null){
                uuid = findGapDownRecoveryStocks.getId();
            }
        }
        if (op == 4){
            QueryWrapper<TradingVolumeTrend> wrapper = new QueryWrapper<>();
            wrapper.eq("history",params.getHistory());
            wrapper.eq("var_threshold",params.getHistory());
            wrapper.eq("min_windows",params.getMin_windows());
            wrapper.eq("period",params.getPeriod());
            TradingVolumeTrend tradingVolumeTrend = tradingVolumeTrendMapper.selectOne(wrapper);
            if (tradingVolumeTrend != null){
                uuid = tradingVolumeTrend.getId();
            }
        }
        return uuid;

    }

    @Override
    public Map<String, String> dataAnalysis(Params params) throws IOException, ConfigurationException {
        String uuid = UUID.randomUUID().toString();
        params.setUuid(uuid);
        Field[] fields = params.getClass().getDeclaredFields(); // 获取到参数和值
//        System.out.println(params);

        String[] ts_codes = ReadDataFromFile.read_csv();

        INIConfiguration config = new INIConfiguration();
        Path currentDir = Paths.get(System.getProperty("user.dir"));
        Path filePath = currentDir.resolve("conf.ini");
        config.read(new FileReader(filePath.toFile()));

        SubnodeConfiguration dbSection = config.getSection("application");
        int numThread = 16;
        try {
            numThread = dbSection.getInt("thread");
        }catch (Exception e){
            logger.error("dbSection.getInt thread is null");
        }
        logger.info("numThread:"+numThread);

//        int numThread = 16;
        HashMap<String, String>[] pp = new HashMap[numThread+1];
        for (int i = 0; i < pp.length; i++) {
            pp[i] = new HashMap<>();
        }
        int count = 0;
        String ts_code = "";
        for (int i = 0; i < ts_codes.length; i++) {
            ts_code = ts_code + ts_codes[i]+",";
            if((i%(ts_codes.length/numThread) == 0 && i !=0) || (i == ts_codes.length-1)){
                for (Field field : fields) {
                    field.setAccessible(true);  // 允许访问私有字段
                    try {
                        pp[count].put(field.getName(), field.get(params).toString());
//                    System.out.println(field.getName() + ": " + field.get(params));
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
                pp[count++].put("ts_codes", ts_code);
                ts_code = "";
//            logger.info("======="+pp[i].toString());
            }
        }
        System.out.println(pp[0].toString());

        // 创建一个固定大小的线程池，线程数量根据实际情况调整
        int cpuCores = Runtime.getRuntime().availableProcessors();
        logger.info("可用cpu数为:"+cpuCores);
        ExecutorService executor = Executors.newFixedThreadPool(numThread);
        for (int i = 0; i < count; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    JavaPythonIntegration.exec("/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data_analysis.py", pp[index]);
//                    logger.info("代码分析完成"+pp[index].get("ts_code"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        // 关闭线程池
        executor.shutdown();
        while (!executor.isTerminated()) {
            // 等待所有任务完成
        }

//        logger.info("正在进行均值分析，参数列表为: "+ uuid + " " +history + " " + happen + " " + patience + " " + trendFlag);

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
// TODO
//        JavaPythonIntegration.exec("/vms/sdb/lhd/code/paper/stock/analysis/demo/core/data_analysis.py",pp);
        logger.info("数据分析完成....");
//
//        HashMap<String, Object> map = new HashMap<>();
//        map.put("uuid", uuid);
//        map.put("history", String.valueOf(history));
//        map.put("happen", String.valueOf(happen));
//        map.put("patience", String.valueOf(patience));
//        map.put("trendFlag", String.valueOf(trendFlag));
//        map.put("msg","数据分析完成");
        return pp[0];
    }

    @Override
    public Map<String, Object> getDataAnalysis(Integer op,String uuid) {
        logger.info("正在查找op为"+op+",uuid为："+uuid+"的数据...");
        List<Object> data =new ArrayList<>();
        if (op == 1){
            QueryWrapper<AverageAnalysis> wrapper = new QueryWrapper<>();
            wrapper.eq("id", uuid);
            data = Collections.singletonList(averageAnalysisMapper.selectList(wrapper));
        }
        if (op == 2){
            QueryWrapper<BoringMarketTrends> wrapper = new QueryWrapper<>();
            wrapper.eq("id", uuid);
            data = Collections.singletonList(boringMarketTrendsMapper.selectList(wrapper));
        }
        if (op == 3){
            QueryWrapper<FindGapDownRecoveryStocks> wrapper = new QueryWrapper<>();
            wrapper.eq("id", uuid);
            data = Collections.singletonList(findGapDownRecoveryStocksMapper.selectList(wrapper));
        }
        if (op == 4){
            QueryWrapper<TradingVolumeTrend> wrapper = new QueryWrapper<>();
            wrapper.eq("id", uuid);
            data = Collections.singletonList(tradingVolumeTrendMapper.selectList(wrapper));
        }

        HashMap<String, Object> map = new HashMap<>();
        map.put("op", op);
        map.put("uuid", uuid);
        map.put("data", data);
        return map;
    }
}
