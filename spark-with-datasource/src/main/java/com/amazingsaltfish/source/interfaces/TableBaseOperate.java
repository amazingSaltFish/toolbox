package com.amazingsaltfish.source.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author: wmh
 * Create Time: 2021/6/26 22:32
 */
public interface TableBaseOperate{
    Dataset<Row> loadTable(String tableName);

    void saveTable(Dataset<Row> dataFrame, String tableName);
}
