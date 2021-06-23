package com.amazingfish;

import com.amazingfish.config.Configs;
import com.amazingfish.operate.BaseKuduOperate;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: wmh
 * Create Time: 2021/6/18 23:38
 */
public class
 Application {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        if ("local".equalsIgnoreCase(Configs.ENV)) {
            sparkConf.setMaster("local");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        BaseKuduOperate operate = new BaseKuduOperate(sparkSession, Configs.KUDU_MASTER);
        operate.listTable().forEach(System.out::println);
        sparkSession.close();
    }
}
