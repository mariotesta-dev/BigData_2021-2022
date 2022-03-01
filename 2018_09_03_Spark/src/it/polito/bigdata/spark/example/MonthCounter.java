package it.polito.bigdata.spark.example;

public class MonthCounter {
	
	public Integer numOfMonthsWithAtLeast2Failures;
	public Integer totalDownTime;
	
	public MonthCounter(Integer downtime, Integer n) {
		this.numOfMonthsWithAtLeast2Failures = n;
		this.totalDownTime = downtime;
	}
	

}
