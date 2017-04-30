package com.kafkaProducer;

import com.twitter.bijection.Injection;

import com.twitter.bijection.avro.GenericAvroCodecs;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class SparkStringConsumer {

	  private static Injection<GenericRecord, byte[]> recordInjection;

	  public static final String PRODUCTDETAILS_SCHEMA = "{"
	            + "\"type\":\"record\","
	            + "\"name\":\"productdetails\","
	            + "\"fields\":["
	            + "  { \"name\":\"productID\", \"type\":\"int\" },"
	            + "  { \"name\":\"companyID\", \"type\":\"int\" },"
	            + "  { \"name\":\"minAge\", \"type\":\"int\" },"
	            + "  { \"name\":\"maxAge\", \"type\":\"int\" },"
	            + "  { \"name\":\"gender\", \"type\":\"string\" },"
	            + "  { \"name\":\"city\", \"type\":\"string\" },"
	            + "  { \"name\":\"adCampaign\", \"type\":\"string\" }"
	            + "]}";
	  public static final String CAMPAIGN_SCHEMA = "{"
	            + "\"type\":\"record\","
	            + "\"name\":\"campaign\","
	            + "\"fields\":["
	            + "  { \"name\":\"campaignId\", \"type\":\"int\" },"
	            + "  { \"name\":\"adCost\", \"type\":\"int\" },"
	            + "  { \"name\":\"adCpm\", \"type\":\"int\" },"
	            + "  { \"name\":\"adBudget\", \"type\":\"int\" },"
	            + "  { \"name\":\"adBudget\", \"type\":\"int\" },"
	            + "  { \"name\":\"adImpressions\", \"type\":\"int\" },"
	            + "  { \"name\":\"adClicks\", \"type\":\"int\" },"
	            + "  { \"name\":\"adViews\", \"type\":\"int\" },"
	            + "  { \"name\":\"demographicsCity\", \"type\":\"string\" },"
	            + "  { \"name\":\"demographicsAgeMax\", \"type\":\"int\" },"
	            + "  { \"name\":\"demographicsAgeMin\", \"type\":\"int\" },"
	            + "  { \"name\":\"demographicsGender\", \"type\":\"string\" }"
	            + "]}";
	    

	    static {
	        Schema.Parser parser = new Schema.Parser();
	        Schema schema = parser.parse(CAMPAIGN_SCHEMA);
	        recordInjection = GenericAvroCodecs.toBinary(schema);
	    }

	    public static void main(String[] args) throws Throwable  {

	        SparkConf conf = new SparkConf();
	        conf.setAppName("kafka-sandbox");
	        conf.setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

	        HashMap<String, String> map = new HashMap<String, String>();
	        
	       
	        
	        
	        Set<String> topics = Collections.singleton("campaignData_kafka_queue");
	        Map<String, String> kafkaParams = new HashMap<>();
	        kafkaParams.put("metadata.broker.list", "localhost:9092");

	        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
	                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

	        directKafkaStream
	                .map(message -> recordInjection.invert(message._2).get())
	                .foreachRDD(rdd -> {
	                    rdd.foreach(record -> {
	                    	System.out.println("campaign id:"+record.get("campaignID"));
	                    	System.out.print("adType:"+record.get("adType"));
	                    	System.out.print("adCost:"+record.get("adCost"));
	                    	System.out.print("adCpm:"+record.get("adCpm"));
	                    	System.out.print("adBudget:"+record.get("adBudget"));
	                    	System.out.print("adImpressions:"+record.get("adImpressions"));
	                    	System.out.print("adClicks:"+record.get("adClicks"));
	                    	System.out.print("adViews:"+record.get("adViews"));
	                    	System.out.print("demographicsCity:"+record.get("demographicsCity"));
	                    	System.out.print("demographicsGender:"+record.get("demographicsGender"));
	                    	System.out.print("demographicsAgeMin:"+record.get("demographicsAgeMin"));
	                    	System.out.print("demographicsAgeMax:"+record.get("demographicsAgeMax"));
	                    	
	                    	});
	                });

	        ssc.start();
	        ssc.awaitTermination();
	    }
}


	
