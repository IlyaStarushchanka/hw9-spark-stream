package com.epam.bigdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

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

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(tuple2 -> {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
            conf.set("zookeeper.znode.parent", "/hbase-unsecure");
            HTable table = new HTable(conf, "logs_file");
                Put put = new Put(Bytes.toBytes(new java.util.Date().getTime()));
                put.add(Bytes.toBytes("logs"), Bytes.toBytes("logs_file1"), Bytes.toBytes(tuple2._2()));
                try {
                    table.put(put);
                } catch (IOException e) {
                    System.out.println("### IOException" + e.getMessage());
                }
                System.out.println("###1 " + tuple2.toString());
                return new String(tuple2._2());
        });
        JavaDStream<String> lines1 = messages.map(tuple2 -> {
            System.out.println("###1 " + tuple2.toString());
            return tuple2._2();
        });

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1,i2) -> i1 + i2);

        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
