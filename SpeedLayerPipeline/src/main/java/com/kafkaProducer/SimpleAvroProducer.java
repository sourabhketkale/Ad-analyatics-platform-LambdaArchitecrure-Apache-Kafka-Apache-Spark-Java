package com.kafkaProducer;

import com.producer.pojo.ProductDetails;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SimpleAvroProducer {

//productID,companyID,minAge,maxAge,gender,city,adCampaign
    public static final String USER_SCHEMA = "{"
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
    

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
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

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("test_kafka_queue", bytes);
            producer.send(record);

            Thread.sleep(250);
        }

        producer.close();
    }
}