package it.polito.bigdata.spark.example;

public class CityAvg{
	
	String city;
	Double maxTempAverage;
	Double countryAvg;
	
	public CityAvg(String city, Double avg) {
		this.city = city;
		this.maxTempAverage = avg;
		this.countryAvg = 0.0;
	}
	
	public CityAvg(String city, Double avg, Double countryAvg) {
		this.city = city;
		this.maxTempAverage = avg;
		this.countryAvg = countryAvg;
	}
	
	public String getCity() {
		return this.city;
	}
	
	public Double getMaxTempAvg() {
		return this.maxTempAverage;
	}
	
	public void setCountryAvg(Double countryAvg) {
		this.countryAvg = countryAvg;
	}
	


}
