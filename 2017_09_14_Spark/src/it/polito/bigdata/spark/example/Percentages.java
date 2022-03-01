package it.polito.bigdata.spark.example;

public class Percentages {
	
	Double percentageFull;
	Double percentageCancelled;
	
	public Percentages(Double full, Double cancelled) {
		this.percentageFull = full;
		this.percentageCancelled = cancelled;
	}
	
	public Double getFPerc() {
		return this.percentageFull;
	}
	
	public Double getCPerc() {
		return this.percentageCancelled;
	}

}
