package cn.scnu.stock.web.config;

//import org.ini4j.Ini;
import org.apache.commons.configuration2.INIConfiguration;
import org.apache.commons.configuration2.SubnodeConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
public class DbConfigReader {

    @Bean
    public DataSource dataSource() {
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        try {
            // 读取 JAR 包外部的 INI 文件
            INIConfiguration config = new INIConfiguration();
            Path currentDir = Paths.get(System.getProperty("user.dir"));
            // 连接当前工作目录和 conf.ini 文件路径
            Path filePath = currentDir.resolve("conf.ini");
            System.out.println("配置文件路径为："+filePath.toFile().getAbsolutePath());
            config.read(new FileReader(filePath.toFile()));

            SubnodeConfiguration dbSection = config.getSection("database");
            String url = dbSection.getString("url");
            String username = dbSection.getString("username");
            String password = dbSection.getString("password");
            String driverClassName = dbSection.getString("driverClassName");
//            System.out.println(url+username+password+driverClassName);

            // 创建数据源
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            dataSource.setDriverClassName(driverClassName);

            return dataSource;
        } catch (IOException | ConfigurationException e) {
            throw new RuntimeException("Failed to read database configuration from INI file", e);
        }
    }
}