package com.amazingsaltfish.source.kudu;

import com.amazingsaltfish.exception.MethodCanNotSupportException;
import com.amazingsaltfish.model.Column;
import com.amazingsaltfish.model.kudu.column.KuduColumn;
import com.amazingsaltfish.source.interfaces.TableColumnOperate;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author: wmh
 * Create Time: 2021/6/27 3:39
 */
public class KuduAlterColumnOperate extends KuduBaseOperate implements TableColumnOperate {
    private KuduContext kuduContext;

    public KuduAlterColumnOperate(SparkSession sparkSession, String kuduMaster) {
        super(sparkSession, kuduMaster);
        this.kuduContext = new KuduContext(kuduMaster, sparkSession.sparkContext());
    }


    @Override
    public void alterTableAddCol(String tableName, Column column) {
        KuduColumn kuduColumn = (KuduColumn) column;
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        alterTableOptions.addColumn(kuduColumn.getColumnSchema());
    }

    @Override
    public void alterTableDropCol(String tableName, String column) {
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        alterTableOptions.dropColumn(column);
        try {
            kuduContext.syncClient().alterTable(tableName, alterTableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void alterTableRenameCol(String tableName, String oldColName, String newColName) {
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        AlterTableOptions renameOptions = alterTableOptions.renameColumn(oldColName, newColName);
        try {
            kuduContext.syncClient().alterTable(tableName, renameOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void alterTableColumnType(String tableName, String columnName, String oldType, String newType) {
        throw new MethodCanNotSupportException(this.getClass().getName(), Thread.currentThread().getStackTrace()[1].getMethodName());
    }
}
