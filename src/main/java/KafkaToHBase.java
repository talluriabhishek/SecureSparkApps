import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.spark.sql.SparkSession;



/**
 * Created by abhishektalluri on 4/7/18.
 */
public class KafkaToHBase {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
    private static final byte[] COLUMN_NAME = Bytes.toBytes("data");

    public static void main(String[] args) throws IOException{
//         new KafkaToHBase().runPipeline();

        new KafkaToHBase().runt();
    }

    public void runt() throws IOException{
        SparkConf conf = new SparkConf();
        SparkSession spark = SparkSession.builder()
                .config("spark.sql.catalogImplementation", "in-memory")
                .getOrCreate();
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("test_table"));
        connection.close();
        table.close();
    }

    public void runPipeline(){

        SparkConf conf = new SparkConf().setAppName("KafkaToHBase");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "nightly513-1.gce.cloudera.com:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "gp1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");

        Collection<String> topics = Arrays.asList("test_topic");
        System.out.println(" ====> Creating a kafka direct stream ");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        System.out.println(" ====> Direct stream created ");
        stream.mapToPair(record -> new Tuple2<>(record.key(),record.value())).foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                System.out.println(" ====> Trying for a HBase connection");

                Configuration configuration = HBaseConfiguration.create();
                try(Connection connection = ConnectionFactory.createConnection(configuration);
                Table table = connection.getTable(TableName.valueOf("test_table"))) {

                    while (partition.hasNext()) {
                        String rowKey = "key1";
                        Tuple2<String,String> tup = partition.next();

                        Put put = new Put(Bytes.toBytes(tup._2.toString().split(",")[0]));
                        put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(tup._2.toString().split(",")[1]));
                        table.put(put);
                        System.out.println(" ====> Put into  a HBase table");
                    }

                }

            });
        });


        ssc.start();
        try {
            ssc.awaitTermination();
        }catch (InterruptedException e){
            System.out.println( "********** Streaming Interrupted **********");
        }

    }

}
