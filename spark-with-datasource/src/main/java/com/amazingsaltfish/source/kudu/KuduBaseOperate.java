package com.amazingsaltfish.source.kudu;

import com.amazingsaltfish.source.AbstractBaseOperate;
import com.amazingsaltfish.source.interfaces.TableBaseOperate;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.HashMap;

/**
 * @author: wmh
 * Create Time: 2021/6/25 0:51
 * kudu 基本操作，加载和写入数据
 */
@Getter
@Setter
public class KuduBaseOperate extends AbstractBaseOperate implements TableBaseOperate {
    private final static String KUDU_IMPL_CLASS_NAME = "kudu";
    private String kuduMaster;

    public KuduBaseOperate() {
        super();
    }

    protected KuduBaseOperate(SparkSession sparkSession, String kuduMaster) {
        super(sparkSession);
        this.kuduMaster = kuduMaster;
    }

    public static TableBaseOperate Builder(SparkSession sparkSession, String kuduMaster) {
        return new KuduBaseOperate(sparkSession, kuduMaster);
    }

    /**
     * 实现kudu超集加载数据接口
     * @param path 读取路径
     * @return 数据集
     */
    @Override
    public Dataset<Row> loadData(String path) {
        HashMap<String, String> kuduMap = new HashMap<>();
        kuduMap.put("kudu.table", path);
        kuduMap.put("kudu.master", kuduMaster);
        return sparkSession.read().format(KUDU_IMPL_CLASS_NAME).options(kuduMap).load();
    }

    /**
     * 实现kudu的写入数据接口，只支持默认的一种写入方式 append
     * @param dataset 待写入数据集
     * @param path 写入路径
     */
    @Override
    public void saveData(Dataset<Row> dataset, String path) {
        HashMap<String, String> kuduMap = new HashMap<>();
        kuduMap.put("kudu.table", path);
        kuduMap.put("kudu.master", kuduMaster);
        dataset.write().format(KUDU_IMPL_CLASS_NAME).options(kuduMap).save();
    }
}
