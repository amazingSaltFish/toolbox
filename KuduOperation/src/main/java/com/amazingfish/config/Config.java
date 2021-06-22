package com.amazingfish.config;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class Config {
    private static final Logger log = LoggerFactory.getLogger(Config.class);

    private static final String SUFFIX = ".properties";


    /**
     * 加载配置
     *
     * @param env 启动环境
     * @return Properties类
     */
    public static Properties getProperties(String env) {
        Properties properties = new Properties();
        String proName = (env == null) ? "local" : env;
        try {
            properties.load(Config.class.getClassLoader().getResourceAsStream(proName + SUFFIX));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return properties;
    }
}
