package com.amazingsaltfish.spark;

import org.apache.spark.sql.SparkSession;

/**
 * @author: wmh
 * Create Time: 2021/6/26 21:56
 */
public class LocalSparkSession {
    public static SparkSession getLocalSparkSession() {
        return SparkSession.builder().master("local[*]").getOrCreate();
    }
}
