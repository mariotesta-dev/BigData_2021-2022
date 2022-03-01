package it.polito.bigdata.spark.example;

public class DatePurchases {
	
	String date;
	Integer numOfPurchases;
	
	public DatePurchases(String d, Integer n) {
		this.date = d;
		this.numOfPurchases = n;
	}

	public String getDate() {
		return date;
	}

	public Integer getNumOfPurchases() {
		return numOfPurchases;
	}
	
	

}
