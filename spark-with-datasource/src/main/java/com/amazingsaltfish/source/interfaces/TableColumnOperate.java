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
     * @param column 待添加列
     *
     */
    void alterTableAddCol(String tableName, Column column);

    /**
     * 删除列
     * @param tableName 表名
     * @param column 待删除列
     */
    void alterTableDropCol(String tableName, String column);

    /**
     * 修改列的名字
     * @param tableName 表名
     * @param oldColName 旧列名
     * @param newColName 新列名
     */
    void alterTableRenameCol(String tableName, String oldColName, String newColName);

    /**
     *  修改列的类型
     * @param tableName 表名
     * @param columnName 列名
     * @param oldType 旧类型
     * @param newType 新类型
     */

    void alterTableColumnType(String tableName, String columnName, String oldType, String newType);
}
