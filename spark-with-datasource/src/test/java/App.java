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
        System.out.println(spark.read().text("hdfs://10.122.3.35:8020/data_from_sc/").count());
        spark.stop();
    }
}
