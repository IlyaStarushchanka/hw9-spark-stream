package com.epam.bigdata;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Created by Ilya_Starushchanka on 10/10/2016.
 */
public class SparkStreamingApp {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static SimpleDateFormat TMS_FORMATTER = new SimpleDateFormat("yyyyMMddhhmmss");
    private static SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err
                    .println("Usage: SparkStreamingLogAggregationApp {master} {zkQuorum} {group} {topic} {numThreads} {table} {columnFamily}");
            System.exit(1);
        }

        String master = args[0];
        String zkQuorum = args[1];
        String group = args[2];
        String[] topics = args[3].split(",");
        int numThreads = Integer.parseInt(args[4]);
        String tableName = args[5];
        String columnFamily = args[6];

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        jssc.checkpoint("hdfs://sandbox.hortonworks.com/develop/aux/checkpoint");

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        messages.checkpoint(new Duration(10000));


        //JavaDStream<String> lines = messages.map(tuple2 -> {
        messages.foreachRDD(rdd ->
                rdd.foreach(tuple2 -> {
            String[] fields = tuple2._2().toString().split("\\t");
            if (!"null".equals(fields[2])) {

                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
                conf.set("zookeeper.znode.parent", "/hbase-unsecure");
                HTable table = new HTable(conf, tableName);


                String rowKey = fields[2] + "_" + fields[1];

                UserAgent ua = UserAgent.parseUserAgentString(fields[3]);
                String device = ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
                String osName = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;

                Date date = TMS_FORMATTER.parse(fields[1]);

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bid_Id"), Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("timestamp_date"), Bytes.toBytes(FORMATTER.format(date)));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ipinyou_Id"), Bytes.toBytes(fields[2]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_agent"), Bytes.toBytes(fields[3]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ip"), Bytes.toBytes(fields[4]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("region"), Bytes.toBytes(Integer.parseInt(fields[5])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("city"), Bytes.toBytes(Integer.parseInt(fields[6])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_exchange"), Bytes.toBytes(Integer.parseInt(fields[7])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("domain"), Bytes.toBytes(fields[8]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("url"), Bytes.toBytes(fields[9]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("anonymous_url_id"), Bytes.toBytes(fields[10]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_id"), Bytes.toBytes(fields[11]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_width"), Bytes.toBytes(Integer.parseInt(fields[12])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_height"), Bytes.toBytes(Integer.parseInt(fields[13])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_visibility"), Bytes.toBytes(Integer.parseInt(fields[14])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ad_slot_format"), Bytes.toBytes(Integer.parseInt(fields[15])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("paying_price"), Bytes.toBytes(Integer.parseInt(fields[16])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("creative_id"), Bytes.toBytes(fields[17]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bidding_price"), Bytes.toBytes(Integer.parseInt(fields[18])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("advertiser_id"), Bytes.toBytes(Integer.parseInt(fields[19])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_tags"), Bytes.toBytes(Long.parseLong(fields[20])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stream_id"), Bytes.toBytes(Integer.parseInt(fields[21])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("device"), Bytes.toBytes(device));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("os_name"), Bytes.toBytes(osName));
                try {
                    table.put(put);
                    table.close();
                } catch (IOException e) {
                    System.out.println("### IOException" + e.getMessage());
                }
                System.out.println("###1 " + tuple2.toString());
            }
            //return new String(tuple2._2());
        }));
        //});

        messages.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
