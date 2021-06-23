package com.amazingfish.operate;

import lombok.NoArgsConstructor;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.kudu.spark.kudu.KuduWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: wmh
 * Create Time: 2021/6/23 13:30
 */
public class ComplexKuduOperate extends BaseKuduOperate {

    private final static Logger logger = LoggerFactory.getLogger(ComplexKuduOperate.class);

    /**
     * 构造kudu的函数
     *
     * @param sparkSession spark的活动会话
     * @param kuduMaster   kudu的主节点连接
     */
    public ComplexKuduOperate(SparkSession sparkSession, String kuduMaster) {
        super(sparkSession, kuduMaster);
    }


    /**
     * @param df                 复制数据集
     * @param tableName          表名
     * @param primaryKeyList     主键列
     * @param createTableOptions 表创建参数
     * @param writeOptions       表写入参数
     */
    public void copyKuduTable(Dataset<Row> df, String tableName, List<String> primaryKeyList, CreateTableOptions createTableOptions, KuduWriteOptions writeOptions) {
        if (ifExistKuduTable(tableName)) {
            logger.warn("table {} already exists!!! please check tableName", tableName);
        } else {
            logger.warn("start create kudu table: {}", tableName);
            createKuduTable(tableName, df.schema(), primaryKeyList, createTableOptions);
        }
        appendKuduTable(df, tableName, writeOptions);
    }


    /**
     * 创建默认副本的表
     *
     * @param tableName      目标表名
     * @param schema         表结构信息
     * @param primarykeyList 主键列表
     */
    public void createKuduTableDefaultThreeReplication(String tableName, StructType schema, List<String> primarykeyList) {
        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.setNumReplicas(3);
        createTableOptions.setRangePartitionColumns(primarykeyList);
        createKuduTable(tableName, schema, primarykeyList, createTableOptions);
        logger.info("kudu table: {} have been created!", tableName);
    }


    /**
     * truncate kudu表
     *
     * @param tableName   原表
     * @param kuduContext kudu context
     * @param kuduClient  kudu client
     */
    public void truncateKuduTable(String tableName, KuduContext kuduContext, KuduClient kuduClient) {

        String tempTableName = tableName + "_bak";
        Schema schema = null;

        try {
            schema = kuduClient.openTable(tableName).getSchema();
        } catch (KuduException e) {
            e.printStackTrace();
        }
        List<ColumnSchema> primaryKeyColumns = schema.getPrimaryKeyColumns();
        List<String> primaryKeys = new ArrayList<>();
        for (ColumnSchema column : primaryKeyColumns) {
            primaryKeys.add(column.getName());
        }

        kuduContext.createTable(
                tempTableName,
                schema,
                new CreateTableOptions().setNumReplicas(3).addHashPartitions(primaryKeys, 3)
        );
        super.deletekuduTable(tableName);
        super.renameTable(tempTableName, tableName, kuduClient);

    }

}
