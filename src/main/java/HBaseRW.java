import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by abhishektalluri on 4/25/18.
 */
public class HBaseRW {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
    private static final byte[] COLUMN_NAME = Bytes.toBytes("data");

    public static void main(String[] args) throws IOException {
        new HBaseRW().run();
    }

    public void run() throws IOException{
        SparkSession spark = SparkSession.builder().getOrCreate();
        Configuration configuration = HBaseConfiguration.create();
        try(HBaseAdmin admin = new HBaseAdmin(configuration)) {
            System.out.println(" ====> Trying for a HBase connection");

            HTableDescriptor descriptor = new
                    HTableDescriptor(TableName.valueOf("movie"));
            HColumnDescriptor columnDescriptor = new
                    HColumnDescriptor(Bytes.toBytes("desc"));
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
//            admin.close();
//        Put put = new Put(Bytes.toBytes("key"+ System.currentTimeMillis()));
//        put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes("StandardValue"));
//        table.put(put);
//            System.out.println(" ====> Put into  a HBase table");

        }
    }

}
