package com.kafka.kafkaSparkNoSQL;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.hadoop.KVInputFormat;
import oracle.kv.hadoop.table.TableInputFormat;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import scala.Tuple2;

public class KafkaNoSQL {

	public static JavaSparkContext sparkContext;
	public static String tableName = "myTable";
	public static String storeName = "mystore";
	static KVStore store;
	static TableAPI tableAPI;
	
	public static void main(String[] args) throws Exception {
		
		// Set Log Level
		Logger.getLogger("org")
	        .setLevel(Level.OFF);
	    Logger.getLogger("akka")
	        .setLevel(Level.OFF);
	    
	    // Set Kafka Params
	    Map<String, Object> kafkaParams = new HashMap<>();
    	kafkaParams.put("bootstrap.servers", "localhost:9092");
    	kafkaParams.put("key.deserializer", StringDeserializer.class);
    	kafkaParams.put("value.deserializer", StringDeserializer.class);
    	kafkaParams.put("group.id", "groupid1");
    	kafkaParams.put("auto.offset.reset", "latest");
    	kafkaParams.put("enable.auto.commit", false);
    	Collection<String> topics = Arrays.asList("kafka_spark");
    	
    	// Set Spark Conf
    	SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("KafkaTosparkApp");

        // Set Streaming Contex Conf
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        sparkContext = streamingContext.sparkContext();
        streamingContext.checkpoint("./.checkpoint");
        
        SparkSession spark = SparkSession.builder().appName("sparkToNosqlApp").getOrCreate();

        // Set Oracle NoSQL Conf
        Configuration hconf = new Configuration();        
        hconf.set("oracle.kv.kvstore", storeName);
        hconf.set("oracle.kv.tableName", tableName); // just a major key prefix
        hconf.set("oracle.kv.hosts", "localhost:5000"); 

        // Kafka Stream to JSON message
        JavaInputDStream<ConsumerRecord<String, String>> stream =
    			KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        JavaPairDStream<String, String> results = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        JavaDStream<String> message = results.map(tuple2 -> tuple2._2());
        
        String[] hhosts = {"localhost:5000"};
		KVStoreConfig kconfig = new KVStoreConfig(storeName, hhosts);
        store = KVStoreFactory.getStore(kconfig);
        
        tableAPI = store.getTableAPI();
        Table table = getTable(tableName);
        
        message.foreachRDD(rdd -> {
    		//Dataset<org.apache.spark.sql.Row> data = spark.read().json(rdd);
    		//data.printSchema();
    		rdd.foreach(x->{
    			try {
    				Row nosqlRow = table.createRowFromJson(x, false);
        			tableAPI.put(nosqlRow, null, null);
    			}catch (Exception e) {
					System.out.println("Input Format Exception, data : " +x);
				}
    			
    		});
	        
    	});

        //read data from NoSQL db to Spark
        JavaPairRDD<PrimaryKey, Row> jrdd = sparkContext.newAPIHadoopRDD(hconf, TableInputFormat.class, PrimaryKey.class, Row.class);
        jrdd.foreach(x->System.out.println(x._2));
        
        // Start & Termination
        streamingContext.start();
        streamingContext.awaitTermination();
	}
	
	static Table getTable(String tableName) {
        Table table = tableAPI.getTable(tableName);
        return table;
    }

}
