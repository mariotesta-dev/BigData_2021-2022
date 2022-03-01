package it.polito.bigdata.spark.example;

public class Counter {
	
	Integer beforeDate;
	Integer afterDate;
	
	public Counter(Integer b, Integer a) {
		this.beforeDate = b;
		this.afterDate = a;
	}

	public Integer getBeforeDate() {
		return beforeDate;
	}

	public Integer getAfterDate() {
		return afterDate;
	}


}
