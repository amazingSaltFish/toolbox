import com.amazingfish.config.Configs;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class TruncateKudu {

    @Test
    public void truncateKuduTable(){
        SparkConf sparkConf = new SparkConf();
        if ("local".equalsIgnoreCase(Configs.ENV)) {
            sparkConf.setMaster("local");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();


        sparkSession.close();
    }
}
