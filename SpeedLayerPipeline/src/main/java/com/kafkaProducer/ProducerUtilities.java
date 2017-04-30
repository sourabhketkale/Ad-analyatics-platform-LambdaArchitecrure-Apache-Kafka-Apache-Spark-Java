package com.kafkaProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.producer.pojo.CampiagnData;
import com.producer.pojo.ProductDetails;

public class ProducerUtilities {

	public static final String PRODUCTDETAILS_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"productdetails\","
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
			}

		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	/**
	 * @function: Convert CSV data from campiagnData to JSON array
	 * 	//campiagnID, adtype, adcost, AdCpm, AdViews, AdImpressions,AdClicks,AdBudget, DemographicsAgeMax, DemographicsAgeMin, DemographicsGender,  DemographicsCity
	 */  
	public List<CampiagnData> csvToJsonForCampaignData()
			throws JsonGenerationException, JsonMappingException, IOException {
		
		String filePath = "//home//k2//data//campaignData.csv";
		Pattern pattern = Pattern.compile(",");
		List<CampiagnData> campiagnDataList = new ArrayList<>();
		try (BufferedReader in = new BufferedReader(new FileReader(filePath));) {
			campiagnDataList = in.lines().skip(1).map(line -> {
				String[] campiagnDataRecord = pattern.split(line);
				
				return new CampiagnData(Long.parseLong(campiagnDataRecord[0].substring(1, campiagnDataRecord[0].length()-2)),
						Integer.parseInt(campiagnDataRecord[1].substring(1, campiagnDataRecord[1].length()-1)), 
						Integer.parseInt(campiagnDataRecord[2].substring(1, campiagnDataRecord[2].length()-1)),
						Integer.parseInt(campiagnDataRecord[3].substring(1, campiagnDataRecord[3].length()-1)), 
						Integer.parseInt(campiagnDataRecord[4].substring(1, campiagnDataRecord[4].length()-1)), 
						Integer.parseInt(campiagnDataRecord[5].substring(1, campiagnDataRecord[5].length()-1)),
						Integer.parseInt(campiagnDataRecord[6].substring(1, campiagnDataRecord[6].length()-1)),
						Integer.parseInt(campiagnDataRecord[7].substring(1, campiagnDataRecord[7].length()-1)),
						Integer.parseInt(campiagnDataRecord[8].substring(1, campiagnDataRecord[8].length()-1)),
						Integer.parseInt(campiagnDataRecord[9].substring(1, campiagnDataRecord[9].length()-1)),
						campiagnDataRecord[10].substring(1, campiagnDataRecord[10].length()-1), 
						campiagnDataRecord[11].substring(1, campiagnDataRecord[11].length()-1));
			}).collect(Collectors.toList());
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(System.out, campiagnDataList);
		}
		return campiagnDataList;
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
		
		String filePath = "//home//k2//Downloads//csvDump//Product_Details.csv";
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
		List<CampiagnData> details = new ArrayList<>();
		
	}
}
