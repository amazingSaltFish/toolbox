import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author: wmh
 * Create Time: 2021/6/24 9:37
 */
public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset<Row> parquet = spark.read().text("hdfs://svldl067.csvw.com/ci_sc/dws_tt_invoice_info/dt=20210623");
        Dataset<Row> text = spark.read().text("hdfs://svldl067.csvw.com/ci_sc/dws_tt_repair_yisun_info/dt=20210623");
        text.show(false);
        parquet.show(false);
        spark.stop();
    }
}
