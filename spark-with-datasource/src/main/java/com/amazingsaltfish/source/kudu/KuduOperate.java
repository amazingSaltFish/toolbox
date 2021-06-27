package com.amazingsaltfish.source.kudu;

import com.amazingsaltfish.model.Column;
import com.amazingsaltfish.source.interfaces.BaseOpreate;
import com.amazingsaltfish.source.interfaces.DatabaseTableDataOperate;
import com.amazingsaltfish.source.interfaces.DatabaseTableOperate;
import com.amazingsaltfish.source.interfaces.TableColumnOperate;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;

/**
 * @author: wmh
 * Create Time: 2021/6/26 23:31
 */
public class KuduOperate extends KuduBaseOperate implements DatabaseTableOperate , TableColumnOperate, DatabaseTableDataOperate {
    private DatabaseTableDataOperate databaseTableDataOperate;
    private TableColumnOperate tableColumnOperate;
    private DatabaseTableOperate databaseTableOperate;

    public KuduOperate(SparkSession sparkSession, String kuduMaster) {
        this.tableColumnOperate = new KuduAlterColumnOperate(sparkSession, kuduMaster);
        this.databaseTableOperate = new KuduTableOperate(sparkSession, kuduMaster);
        this.databaseTableDataOperate = new KuduTableDataOperate(sparkSession, kuduMaster);
    }

    @Override
    public void createTable(String tableName, StructType schema, HashMap<String, Object> params) {
        databaseTableOperate.createTable(tableName, schema, params);
    }

    @Override
    public void renameTableName(String oldTableName, String newTableName) {
        databaseTableOperate.renameTableName(oldTableName, newTableName);
    }

    @Override
    public void deleteTable(String tableName) {
        databaseTableOperate.deleteTable(tableName);
    }

    @Override
    public Boolean isTableExist(String tableName) {
        return databaseTableOperate.isTableExist(tableName);
    }

    @Override
    public List<String> listTables() {
        return databaseTableOperate.listTables();
    }

    @Override
    public void truncateTable(String tableName) {
        databaseTableOperate.truncateTable(tableName);
    }

    @Override
    public void alterTableAddCol(String tableName, Column column) {
        tableColumnOperate.alterTableAddCol(tableName, column);
    }

    @Override
    public void alterTableDropCol(String tableName, String column) {
        tableColumnOperate.alterTableDropCol(tableName, column);
    }

    @Override
    public void alterTableRenameCol(String tableName, String oldColName, String newColName) {
        tableColumnOperate.alterTableRenameCol(tableName, oldColName, newColName);
    }

    @Override
    public void alterTableColumnType(String tableName, String columnName, String oldType, String newType) {
        tableColumnOperate.alterTableColumnType(tableName, columnName, oldType, newType);
    }

    @Override
    public void appendData(Dataset<Row> dataset, String tableName) {
        databaseTableDataOperate.appendData(dataset,tableName);
    }

    @Override
    public void deleteData(Dataset<Row> dataset, String tableName, HashMap<String, Object> params) {
        databaseTableDataOperate.deleteData(dataset,tableName,params);
    }

    @Override
    public void overwriteData(Dataset<Row> dataset, String tableName) {
        databaseTableDataOperate.overwriteData(dataset,tableName);
    }

    @Override
    public void updateData(Dataset<Row> dataset, String tableName, HashMap<String, Object> params) {
        databaseTableDataOperate.updateData(dataset, tableName, params);
    }
}
