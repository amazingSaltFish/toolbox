package com.amazingsaltfish.source.kudu;

import com.amazingsaltfish.exception.MethodCanNotSupportException;
import com.amazingsaltfish.source.interfaces.DatabaseTableOperate;
import com.amazingsaltfish.utils.CastUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

import java.util.HashMap;
import java.util.List;


/**
 * @author: wmh
 * Create Time: 2021/6/26 23:12
 */
@Setter
@Getter
public class KuduTableOperate extends KuduBaseOperate implements DatabaseTableOperate {
    private KuduContext kuduContext;
    private final static String PRIMARY_KEY_INDEX = "primary.keys";
    private final static String CREATE_OPTIONS = "create.options";

    public KuduTableOperate(SparkSession sparkSession, String kuduMaster) {
        super(sparkSession,kuduMaster);
        this.kuduContext = new KuduContext(kuduMaster, sparkSession.sparkContext());
    }


    @Override
    public void createTable(String tableName, StructType schema, HashMap<String, Object> params) {
        List<String> primaryKeys = CastUtil.castList(params.get(PRIMARY_KEY_INDEX), String.class);
        CreateTableOptions createTableOptions = (CreateTableOptions) params.get(CREATE_OPTIONS);
        Buffer<String> primarykeyScalaList = JavaConversions.asScalaBuffer(primaryKeys);
        kuduContext.createTable(tableName, schema, primarykeyScalaList, createTableOptions);
    }

    @Override
    public void renameTableName(String oldTableName, String newTableName) {
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        alterTableOptions.renameTable(newTableName);
        try {
            kuduContext.syncClient().alterTable(oldTableName,alterTableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteTable(String tableName) {
        kuduContext.deleteTable(tableName);
    }

    @Override
    public Boolean isTableExist(String tableName) {
        return kuduContext.tableExists(tableName);
    }


    @Override
    public List<String> listTables() {
        try {
            return kuduContext.syncClient().getTablesList().getTablesList();
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void truncateTable(String tableName) {
        throw new MethodCanNotSupportException(this.getClass().getName(), Thread.currentThread().getStackTrace()[1].getMethodName());
    }
}
