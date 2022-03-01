package it.polito.bigdata.spark.example;

public class DatePrice{
	
	String date;
	Double price;
	
	public DatePrice(String date, Double price) {
		this.date = date;
		this.price = price;
	}
	
	public String getDate() {
		return date;
	}
	
	public Double getPrice() {
		return price;
	}
	
	public static Integer weekNumber(String date) {
		return 21; //dummy function
	}


}
