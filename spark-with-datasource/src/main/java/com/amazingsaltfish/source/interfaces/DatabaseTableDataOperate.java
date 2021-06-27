package com.amazingsaltfish.source.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;

/**
 * @author: wmh
 * Create Time: 2021/6/24 0:03
 */
public interface DatabaseTableDataOperate {

    /**
     * 追加表数据
     * @param dataset 待追加数据集
     * @param tableName 表名
     */
    void appendData(Dataset<Row> dataset, String tableName);

    /**
     * 删除表数据
     * @param dataset 待删除数据集
     * @param tableName 表名
     */
    void deleteData(Dataset<Row> dataset, String tableName, HashMap<String,Object> params);
    /**
     * 覆写表
     * @param dataset   待写入数据集
     * @param tableName 表名
     */
    void overwriteData(Dataset<Row> dataset, String tableName);

    /**
     * 更新表
     * @param dataset 待更新的数据集
     * @param tableName 更新的表名
     */
    void updateData(Dataset<Row> dataset, String tableName, HashMap<String,Object> params);



}
