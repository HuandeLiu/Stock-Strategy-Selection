package cn.scnu.stock.web.utils;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadDataFromFile {
    private static Logger logger = Logger.getLogger(ReadDataFromFile.class.getClass());
    public static String[] read_csv() {
//        String filePath = "E:\\code\\python\\project\\voice\\web\\src\\main\\java\\ts_code.csv"; // 数据文件路径，根据实际情况修改
        String filePath = "ts_code.csv"; // 数据文件路径，根据实际情况修改
        List<String> tsCodes = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isHeader = true; // 用于判断是否是表头行
            while ((line = br.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue; // 跳过表头行
                }
                String[] parts = line.split(",");
                if (parts.length > 0) {
                    tsCodes.add(parts[0]);
//                    logger.info("股票代码"+parts[0]);
                }
            }

            // 将List转换为数组
            String[] tsCodeArray = tsCodes.toArray(new String[0]);

            // 打印数组内容，用于验证
//            for (String tsCode : tsCodeArray) {
//                logger.info("股票代码为"+tsCode.toString()+"  ");
//            }
            return tsCodes.toArray(new String[0]);
        } catch (IOException e) {
            e.printStackTrace();
            return new String[0];
        }


    }
}