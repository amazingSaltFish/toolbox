package com.amazingfish.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

/**
 * @Author: wmh
 * Create Time: 2021/6/19 11:20
 */
public class Configs {
    final static Config config = ConfigFactory.load();

    public final static String KUDU_MASTER = config.getString(EnvParams.KUDU_MASTER);
    public final static String ENV = config.getString(EnvParams.ENV);

}
