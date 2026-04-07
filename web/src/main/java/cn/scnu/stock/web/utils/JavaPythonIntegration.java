package cn.scnu.stock.web.utils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
public  class JavaPythonIntegration {
    private static Logger logger = Logger.getLogger(JavaPythonIntegration.class.getClass());
    public static void exec(String pythonScriptPath, HashMap<String, String> params) {
        try {
            // 构建命令行调用 Python 脚本
//            String command = "conda activate pytorch && python " + pythonScriptPath + " ";
            String command = "python " + pythonScriptPath + " ";
            for (Map.Entry<String, String> param : params.entrySet()) {
                String key = param.getKey();
                String value = param.getValue();
                command = command + "--" + key + " " + value + " ";
            }
            logger.info("Executing Python : " + command);

//            System.out.println("Executing Python  " + command);
            // 执行命令
            Runtime runtime = Runtime.getRuntime();
//            Process process = runtime.exec(command);
            Process process = runtime.exec(command);
//            logger.info("Python Process info : " + process.info());
//            System.out.println(process.info());
//            System.out.println(process.toHandle());
            // 获取错误流
            InputStream errorStream = process.getErrorStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
            String line;
            while ((line = reader.readLine()) != null) {
                logger.error("Python Process ERROR : " + line);
//                System.out.println("Error: " + line);
            }

            // 读取输出
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line1;
            while ((line1 = reader1.readLine()) != null) {
                logger.info("Python Process output : " + line1);
//                System.out.println(line1);
            }

            // 等待进程结束
            int exitCode = process.waitFor();
//            logger.info("Python Process exitCode : " + exitCode);

//            ProcessBuilder processBuilder = new ProcessBuilder("conda activate pytorch & python", command);

            // 启动进程
//            Process process = processBuilder.start();

            // 获取 Python 脚本的输出
//            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//            String line;
//            while ((line = reader.readLine()) != null) {
//                System.out.println(line);
//            }


//            System.out.println("Python script executed with exit code: " + exitCode);
        } catch (IOException | InterruptedException e) {
            logger.error("Python Process Exception : " + e.getMessage());
//            System.out.println("出现了异常");
//            e.printStackTrace();
        }
    }
}
