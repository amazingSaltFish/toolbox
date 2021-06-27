package com.amazingsaltfish.source.interfaces;

import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * @author: wmh
 * Create Time: 2021/6/24 9:41
 */
public interface DatabaseTableOperate{
    /**
     * 创建表
     * @param tableName 表名
     * @param schema  表结构
     * @param params 其它参数
     */
    void createTable(String tableName, StructType schema, HashMap<String,Object> params);


    /**
     * 重命名表
     * @param oldTableName 旧表名
     * @param newTableName 新表名
     */
    void renameTableName(String oldTableName, String newTableName);

    /**
     * 删除表
     * @param tableName 表名
     */
    void deleteTable(String tableName);

    /**
     * 判断表是否存在
     * @param tableName 表名
     * @return if exist return true else false
     */
    Boolean isTableExist(String tableName);

    /**
     * 返回表列表
     * @return tablelist
     */
    List<String> listTables();

    /**
     * truncate Table
     * @param tableName 表名
     */
    void truncateTable(String tableName);

}
