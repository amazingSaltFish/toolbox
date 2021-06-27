package com.amazingsaltfish.source.interfaces;

import com.amazingsaltfish.model.Column;
import org.apache.spark.sql.types.StructType;

/**
 * @author: wmh
 * Create Time: 2021/6/24 18:29
 */
public interface TableColumnOperate {
    /**
     * 表添加列
     * @param tableName 表名
     * @param schema 待添加列
     *
     */
    void alterTableAddCol(String tableName, Column column);

    /**
     * 删除列
     * @param tableName 表名
     * @param schema 待删除列
     */
    void alterTableDropCol(String tableName, String column);


    void alterTableRenameCol(String tableName, String oldColName, String newColName);


    void alterTableColumnType(String tableName, String columnName, String oldType, String newType);
}
