package com.amazingsaltfish.source;

import com.amazingsaltfish.source.interfaces.BaseOpreate;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author: wmh
 * Create Time: 2021/6/24 18:27
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractBaseOperate implements BaseOpreate{

    protected SparkSession sparkSession;

    /**
     * 读取表
     * @param tableName 表名
     * @return 表数据
     */
    public Dataset<Row> loadTable(String tableName) {
        return loadData(tableName);
    }

    /**
     * 存储表
     * @param dataset 表数据
     * @param tableName 表名
     */
    public void saveTable(Dataset<Row> dataset, String tableName) {
        saveData(dataset,tableName);
    }
}
