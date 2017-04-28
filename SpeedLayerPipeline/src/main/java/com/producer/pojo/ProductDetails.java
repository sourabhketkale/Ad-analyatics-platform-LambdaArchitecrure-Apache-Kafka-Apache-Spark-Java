package com.producer.pojo;


/**
 * 
 * @author Sourabh Ketkale
 * */
public class ProductDetails {
	
	int productId;
	int companyID;
	int minAge;
	int maxAge;
	String gender;
	String city;
	String adCampaign;
	
	
	public ProductDetails(int productId, int companyID, int minAge, int maxAge, String gender, String city,
			String adCampaign) {
		super();
		this.productId = productId;
		this.companyID = companyID;
		this.minAge = minAge;
		this.maxAge = maxAge;
		this.gender = gender;
		this.city = city;
		this.adCampaign = adCampaign;
	}
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public int getCompanyID() {
		return companyID;
	}
	public void setCompanyID(int companyID) {
		this.companyID = companyID;
	}
	public int getMinAge() {
		return minAge;
	}
	public void setMinAge(int minAge) {
		this.minAge = minAge;
	}
	public int getMaxAge() {
		return maxAge;
	}
	public void setMaxAge(int maxAge) {
		this.maxAge = maxAge;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getAdCampaign() {
		return adCampaign;
	}
	public void setAdCampaign(String adCampaign) {
		this.adCampaign = adCampaign;
	}
	
	
	
	
}
