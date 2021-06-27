package com.amazingsaltfish.source.kudu;

import com.amazingsaltfish.source.AbstractBaseOperate;
import com.amazingsaltfish.source.interfaces.BaseOpreate;
import com.amazingsaltfish.source.interfaces.TableBaseOperate;
import com.amazingsaltfish.spark.LocalSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * @author: wmh
 * Create Time: 2021/6/26 21:53
 */
public class KuduBaseOperateTest {
    private String kuduMaster = "spark1:7051,spark2:7051,spark3:7051";
    SparkSession sparkSession = LocalSparkSession.getLocalSparkSession();
    @Test
    public void loadData() {
        SparkSession sparkSession = LocalSparkSession.getLocalSparkSession();
        TableBaseOperate tableBaseOperate = KuduBaseOperate.Builder(sparkSession, kuduMaster);
        Dataset<Row> stu9 = tableBaseOperate.loadTable("stu9");
        stu9.show();
        ArrayList<Row> list = new ArrayList<Row>();
        list.add(RowFactory.create(1, "xiaoming", 10));
        Dataset<Row> dataFrame = sparkSession.createDataFrame(list, stu9.schema());
        tableBaseOperate.saveTable(dataFrame,"stu9");
        tableBaseOperate.loadTable("stu9").show();
        sparkSession.stop();
    }

    @Test
    public void saveData() {

    }
}