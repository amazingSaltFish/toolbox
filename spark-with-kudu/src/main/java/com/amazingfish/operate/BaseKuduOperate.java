package com.amazingfish.operate;

import lombok.extern.slf4j.Slf4j;

import org.apache.kudu.client.AlterTableOptions;
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
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

import java.util.HashMap;
import java.util.List;

/**
 * @Author: wmh
 * Create Time: 2021/6/19 11:51
 * kudu表的相关操作
 */
@Slf4j
public class BaseKuduOperate {
    private final SparkSession sparkSession;
    private final KuduContext kuduContext;
    private final String kuduMaster;
    private final static Logger logger = LoggerFactory.getLogger(BaseKuduOperate.class);

    /**
     * 构造kudu的函数
     *
     * @param sparkSession spark的活动会话
     * @param kuduMaster   kudu的主节点连接
     */
    public BaseKuduOperate(SparkSession sparkSession, String kuduMaster) {
        this.sparkSession = sparkSession;
        this.kuduMaster = kuduMaster;
        this.kuduContext = new KuduContext(kuduMaster, sparkSession.sparkContext());
    }

    /**
     * 用来读取kudu表
     *
     * @param tableName 目标表名
     * @return 目标数据集
     */
    public Dataset<Row> getKuduTable(String tableName) {
        HashMap<String, String> kuduMap = new HashMap<>();
        kuduMap.put("kudu.master", kuduMaster);
        kuduMap.put("kudu.table", tableName);
        logger.info("kudu table: {} load data!", tableName);
        return sparkSession.read().format("kudu").options(kuduMap).load();
    }

    /**
     * 向kudu表中覆写数据
     *
     * @param df               待写入数据
     * @param tableName        目标表名
     * @param kuduWriteOptions 写入配置参数
     */
    public void appendKuduTable(Dataset<Row> df, String tableName, KuduWriteOptions kuduWriteOptions) {
        kuduContext.upsertRows(df, tableName, kuduWriteOptions);
        logger.info("kudu table:data insert into {}", tableName);
    }

    /**
     * 从kudu表中删除数据
     *
     * @param df               待删除数据集
     * @param tableName        目标表名
     * @param kuduWriteOptions kudu写入参数
     */
    public void deleteKuduTableData(Dataset<Row> df, String tableName, KuduWriteOptions kuduWriteOptions) {
        kuduContext.deleteRows(df, tableName, kuduWriteOptions);
        logger.info("kudu table:{} delete rows", tableName);
    }

    /**
     * 删除表
     *
     * @param tableName 目标表名
     */
    public void deletekuduTable(String tableName) {
        kuduContext.deleteTable(tableName);
        logger.info("kudu table: {} is deleted", tableName);
    }

    /**
     * 判别目标库中是否存在表
     *
     * @param tableName 目标表名
     * @return 表是否存在
     */
    public Boolean ifExistKuduTable(String tableName) {
        return kuduContext.tableExists(tableName);
    }

    /**
     * 更新kudutable
     *
     * @param df               待更新数据集
     * @param tableName        目标表名
     * @param kuduWriteOptions kudu的写入参数
     */
    public void updateKuduTable(Dataset<Row> df, String tableName, KuduWriteOptions kuduWriteOptions) {
        kuduContext.updateRows(df, tableName, kuduWriteOptions);
    }

    /**
     * 创建表
     *
     * @param tableName          目标表名
     * @param schema             表的结构
     * @param primarykeyList     主键列
     * @param createTableOptions 创建选项参数
     */
    public void createKuduTable(String tableName, StructType schema, List<String> primarykeyList, CreateTableOptions createTableOptions) {
        Buffer<String> primaryKeys = JavaConversions.asScalaBuffer(primarykeyList);
        kuduContext.createTable(tableName, schema, primaryKeys, createTableOptions);
    }


    public List<String> listTable() {
        List<String> tableList = null;
        try {
            tableList = kuduContext.syncClient().getTablesList().getTablesList();
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return tableList;
    }

    /**
     * 修改kudu的表名
     *
     * @param tableName 需要修改的表
     * @param newName 新表名
     * @param kuduClient kuduClient
     */
    public void renameTable(String tableName, String newName, KuduClient kuduClient) {
        AlterTableOptions alterTableOptions = new AlterTableOptions().renameTable(newName);
        try {
            kuduClient.alterTable(tableName, alterTableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

}
