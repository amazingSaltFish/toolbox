package com.amazingfish.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @Author: wmh
 * Create Time: 2021/6/21 0:20
 */
public class Operate {
    private String url;
    private SparkSession sparkSession;
    private Properties properties;

    public Operate(SparkSession sparkSession, String url, Properties properties) {
        this.sparkSession = sparkSession;
        this.url = url;
        this.properties = properties;
    }

    public Dataset<Row> readJDBC(String tableName) {
       return sparkSession.read().jdbc(url, tableName, properties);
    }
}
