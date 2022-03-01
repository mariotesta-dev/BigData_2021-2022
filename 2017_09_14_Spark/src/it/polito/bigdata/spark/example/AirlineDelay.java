package it.polito.bigdata.spark.example;

public class AirlineDelay {
	
	String airline;
	Integer moreThan15MinsDelay;
	
	public AirlineDelay(String airline, Integer delay) {
		this.airline = airline;
		
		this.moreThan15MinsDelay = delay > 15 ? 1 : 0;
		
	}

	public String getAirline() {
		return this.airline;
	}

	public Integer getDelay() {
		return this.moreThan15MinsDelay;
	}
}
