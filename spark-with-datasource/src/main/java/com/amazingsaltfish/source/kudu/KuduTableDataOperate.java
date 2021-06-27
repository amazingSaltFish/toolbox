package com.amazingsaltfish.source.kudu;


import com.amazingsaltfish.exception.MethodCanNotSupportException;
import com.amazingsaltfish.source.interfaces.DatabaseTableDataOperate;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.kudu.spark.kudu.KuduWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.util.HashMap;

/**
 * @author: wmh
 * Create Time: 2021/6/27 2:28
 */
public class KuduTableDataOperate extends KuduBaseOperate implements DatabaseTableDataOperate {
    private KuduContext kuduContext;
    private final static String WRITE_OPTION = "kudu.option";

    public KuduTableDataOperate(SparkSession sparkSession, String kuduMaster) {
        super(sparkSession, kuduMaster);
        this.kuduContext = new KuduContext(kuduMaster, sparkSession.sparkContext());
    }

    @Override
    public void appendData(Dataset<Row> dataset, String tableName) {
        saveData(dataset, tableName);
    }

    @Override
    public void deleteData(Dataset<Row> dataset, String tableName, HashMap<String, Object> params) {
        KuduWriteOptions kuduWriteOptions = (KuduWriteOptions) params.get(WRITE_OPTION);
        kuduContext.deleteRows(dataset, tableName,kuduWriteOptions);
    }


    @Override
    public void overwriteData(Dataset<Row> dataset, String tableName) {
        throw new MethodCanNotSupportException(this.getClass().getName(), Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void updateData(Dataset<Row> dataset, String tableName, HashMap<String,Object> params) {
        KuduWriteOptions kuduWriteOptions = (KuduWriteOptions) params.get(WRITE_OPTION);
        kuduContext.updateRows(dataset, tableName,kuduWriteOptions);
    }
}
