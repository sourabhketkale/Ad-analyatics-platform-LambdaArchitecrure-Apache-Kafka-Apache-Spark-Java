package com.kafkaProducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.api.r.SerializationFormats;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.producer.pojo.ProductDetails;

import scala.util.parsing.json.JSONArray;

public class ProducerUtilities {

	public static final String USER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"productdetails\","
			+ "\"fields\":[" + "  { \"name\":\"productID\", \"type\":\"int\" },"
			+ "  { \"name\":\"companyID\", \"type\":\"int\" }," + "  { \"name\":\"minAge\", \"type\":\"int\" },"
			+ "  { \"name\":\"maxAge\", \"type\":\"int\" }," + "  { \"name\":\"gender\", \"type\":\"string\" },"
			+ "  { \"name\":\"city\", \"type\":\"string\" }," + "  { \"name\":\"adCampaign\", \"type\":\"string\" }"
			+ "]}";

	/**
	 * @author Sourabh Ketkale
	 * @function: to load csv data to kafka producer
	 */
	public void loadCsvToKafka() {

		String filePath = "/home/k2/Downloads/Product_Details.csv";
		BufferedReader reader = null;
		String line = "";
		String csvSplitBy = ",";

		try {
			reader = new BufferedReader(new FileReader(filePath));

			while ((line = reader.readLine()) != null) {

				String[] ProductDetail = line.split(csvSplitBy);
				// System.out.println("\n1:ProductId:"+ProductDetail[0]+"\t2:CompanyId:"+ProductDetail[1]+"\t3:minAge:"+ProductDetail[2]+"\t4:maxAge:"+ProductDetail[3]+"\t5:Gender:"+ProductDetail[4]+"\t6:City:"+ProductDetail[5]+"\t7:AdCampign:"+ProductDetail[6]);
			}

		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @author Sourabh Ketkale
	 * @function: Csv file listiner to the a CSV dump directory
	 * 
	 * */
	public ArrayList<String> csvFileListenier(String directoryPath){
		
		
		ArrayList<String> filePaths = new ArrayList<String>();
		
		String dirPath =  "/home/k2/Downloads/csvDump";
		//File file = new 
		
		return filePaths;
		
		
	}

	/**
	 * @author Sourabh Ketkale
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonGenerationException
	 * @function: convert csv data into JSON object using specified POJO
	 * 
	 */
	public List<ProductDetails> csvToJson() throws JsonGenerationException, JsonMappingException, IOException {
		JSONArray productDataJsonArray = null;
		String filePath = "/home/k2/Downloads/csvDump/Product_Details.csv";
		Pattern pattern = Pattern.compile(",");
		List<ProductDetails> ProductDetailsList = new ArrayList<>();
		try (BufferedReader in = new BufferedReader(new FileReader(filePath));) {
			ProductDetailsList = in.lines().skip(1).map(line -> {
				String[] productDetailsRecord = pattern.split(line);
				return new ProductDetails(Integer.parseInt(productDetailsRecord[0]),
						Integer.parseInt(productDetailsRecord[1]), Integer.parseInt(productDetailsRecord[2]),
						Integer.parseInt(productDetailsRecord[3]), productDetailsRecord[4], productDetailsRecord[5],
						productDetailsRecord[6]);
			}).collect(Collectors.toList());
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(System.out, ProductDetailsList);
		}
		return ProductDetailsList;
	}

	public static void main(String Argss[]) throws JsonGenerationException, JsonMappingException, IOException {

		ProducerUtilities utilities = new ProducerUtilities();
		List<ProductDetails> details = new ArrayList<>();
		// details = utilities.csvToJson();

		System.out.println(utilities.USER_SCHEMA);
	}
}
