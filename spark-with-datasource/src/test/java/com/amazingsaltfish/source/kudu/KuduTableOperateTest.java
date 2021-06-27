package com.amazingsaltfish.source.kudu;

import com.amazingsaltfish.spark.LocalSparkSession;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author: wmh
 * Create Time: 2021/6/27 1:31
 */
public class KuduTableOperateTest {
    private SparkSession sparkSession;
    private String kuduMaster = "spark1:7051,spark2:7051,spark3:7051";
    @Before
    public void initial() {
        sparkSession = LocalSparkSession.getLocalSparkSession();
    }

    @Test
    public void createTable() {
    }

    @Test
    public void renameTableName() {
        KuduTableOperate kuduTableOperate = new KuduTableOperate(sparkSession, kuduMaster);
        kuduTableOperate.renameTableName("aaa","aaa");
    }

    @Test
    public void deleteTable() {
    }

    @Test
    public void isTableExist() {
    }

    @Test
    public void truncateTable() {
    }
}