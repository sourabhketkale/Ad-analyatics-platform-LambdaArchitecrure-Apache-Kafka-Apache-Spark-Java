package com.kafkaProducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.producer.pojo.CampiagnData;
import com.producer.pojo.ProductDetails;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class SimpleAvroProducer {

//productID,companyID,minAge,maxAge,gender,city,adCampaign
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
    
    /**
     * 	
	private int adType, demographicsAgeMin, demographicsAgeMax ;
	String demographicsCity, demographicsGender;
	long campaignId;
	int  adCost, adCpm, adBudget, adImpressions, adClicks, adViews;
     * */
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
    
    
    private final String productDetailsQueue = "test_kafka_queue";
    private final static String campiagnDataQueue = "campaignData_kafka_queue";
    
    /**
     * @author: Sourabh Ketkale
     * @method: This method is to handle the campaign data stream 
     * 
     * */
    public void campaignDataStream(String kafkaTopicName, String campaignSchema) throws InterruptedException {
    	
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(campaignSchema);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        ProducerUtilities utilities = new ProducerUtilities();
        List<CampiagnData> campaignData = new ArrayList<>(); 
        try {
			campaignData = utilities.csvToJsonForCampaignData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        for (int i = 0; i < campaignData.size(); i++) {
          GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("campaignID", campaignData.get(i).getCampaignId());
            avroRecord.put("adType", campaignData.get(i).getAdType());
            avroRecord.put("adCost", campaignData.get(i).getAdCost());
            avroRecord.put("adCpm", campaignData.get(i).getAdCpm());
            avroRecord.put("adBudget", campaignData.get(i).getAdBudget());
            avroRecord.put("adImpressions", campaignData.get(i).getAdImpressions());
            avroRecord.put("adClicks", campaignData.get(i).getAdClicks());
            avroRecord.put("adViews", campaignData.get(i).getAdViews());
            avroRecord.put("demographicsCity", campaignData.get(i).getDemographicsCity());
            avroRecord.put("demographicsGender", campaignData.get(i).getDemographicsGender());
            avroRecord.put("demographicsAgeMin", campaignData.get(i).getDemographicsAgeMin());
            avroRecord.put("demographicsAgeMax", campaignData.get(i).getDemographicsAgeMax());
            
            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(campiagnDataQueue, bytes);
            producer.send(record);

            Thread.sleep(250);
        }

        producer.close();
    	
    }
    
    /**
     * @author: Sourabh Ketkale
     * @throws InterruptedException 
     * @method: This method is to handle the product details data stream 
     * 
     * */
    public void productDetailsDataStream(String kafkaTopicName, String productDetailsSchema) throws InterruptedException{
    	
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(productDetailsSchema);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        ProducerUtilities utilities = new ProducerUtilities();
        List<ProductDetails> productDetails = new ArrayList<>(); 
        try {
			productDetails = utilities.csvToJson();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        for (int i = 0; i < productDetails.size(); i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("productID", productDetails.get(i).getProductId());
            avroRecord.put("companyID", productDetails.get(i).getCompanyID());
            avroRecord.put("minAge", productDetails.get(i).getMinAge());
            avroRecord.put("maxAge", productDetails.get(i).getMaxAge());
            avroRecord.put("gender", productDetails.get(i).getGender());
            avroRecord.put("city", productDetails.get(i).getCity());
            avroRecord.put("adCampaign", productDetails.get(i).getAdCampaign());
            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(productDetailsQueue, bytes);
            producer.send(record);

            Thread.sleep(250);
        }

        producer.close();
    	
    }
    
    

    public static void main(String[] args) throws InterruptedException {
    	
    	SimpleAvroProducer avroProducer = new SimpleAvroProducer();
    	avroProducer.campaignDataStream(campiagnDataQueue, CAMPAIGN_SCHEMA);

    }
}