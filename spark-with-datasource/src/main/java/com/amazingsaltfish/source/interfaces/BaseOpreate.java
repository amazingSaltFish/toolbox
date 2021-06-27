package com.amazingsaltfish.source.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * 每个数据源都应该有读取数据和写入数据的方法
 * @author: wmh
 * Create Time: 2021/6/23 23:56
 */
public interface BaseOpreate {
    /**
     * 读取数据
     * @param path 读取路径
     * @return  数据集
     */
    Dataset<Row> loadData(String path);

    /**
     * 写入数据
     * @param dataset 待写入数据集
     * @param path 写入路径
     */
    void saveData(Dataset<Row> dataset,String path);
}
