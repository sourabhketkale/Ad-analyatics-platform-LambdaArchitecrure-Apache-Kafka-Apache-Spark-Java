package com.producer.pojo;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

/*
 * CSV structure:
 * campaignId, ad_type, ad_cost, ad_cpm, ad_views, ad_budget, demo_graphics_age_min, demographics_gender,
 *  demographics_city, ad_impressions, ad_clicks, ad_views, ad_budget, ad
 * 
 * */


public class CampiagnData {
	
	private int adType, demographicsAgeMin, demographicsAgeMax ;
	String demographicsCity, demographicsGender;
	long campaignId;
	int  adCost, adCpm, adBudget, adImpressions, adClicks, adViews;
	
	
	@Override
	public String toString() {
		return "CampiagnData [adType=" + adType + ", demographicsAgeMin=" + demographicsAgeMin + ", demographicsAgeMax="
				+ demographicsAgeMax + ", demographicsCity=" + demographicsCity + ", demographicsGender="
				+ demographicsGender + ", campaignId=" + campaignId + ", adCost=" + adCost + ", adCpm=" + adCpm
				+ ", adBudget=" + adBudget + ", adImpressions=" + adImpressions + ", adClicks=" + adClicks
				+ ", adViews=" + adViews + "]";
	}
	public long getCampaignId() {
		return campaignId;
	}
	public void setCampaignId(int campaignId) {
		this.campaignId = campaignId;
	}
	public int getAdCost() {
		return adCost;
	}
	public void setAdCost(int adCost) {
		this.adCost = adCost;
	}
	public int getAdCpm() {
		return adCpm;
	}
	public void setAdCpm(int adCpm) {
		this.adCpm = adCpm;
	}
	public int getAdBudget() {
		return adBudget;
	}
	public void setAdBudget(int adBudget) {
		this.adBudget = adBudget;
	}
	public int getDemographicsAgeMin() {
		return demographicsAgeMin;
	}
	public void setDemographicsAgeMin(int demographicsAgeMin) {
		this.demographicsAgeMin = demographicsAgeMin;
	}
	public int getDemographicsAgeMax() {
		return demographicsAgeMax;
	}
	public void setDemographicsAgeMax(int demographicsAgeMax) {
		this.demographicsAgeMax = demographicsAgeMax;
	}
	public String getDemographicsGender() {
		return demographicsGender;
	}
	public void setDemographicsGender(String demographicsGender) {
		this.demographicsGender = demographicsGender;
	}
	public int getAdImpressions() {
		return adImpressions;
	}
	public void setAdImpressions(int adImpressions) {
		this.adImpressions = adImpressions;
	}
	public int getAdClicks() {
		return adClicks;
	}
	public void setAdClicks(int adClicks) {
		this.adClicks = adClicks;
	}
	public int getAdViews() {
		return adViews;
	}
	public void setAdViews(int adViews) {
		this.adViews = adViews;
	}
	public String getDemographicsCity() {
		return demographicsCity;
	}
	public void setDemographicsCity(String demographicsCity) {
		this.demographicsCity = demographicsCity;
	}
	public int getAdType() {
		return adType;
	}
	public void setAdType(int adType) {
		this.adType = adType;
	}
	
}
