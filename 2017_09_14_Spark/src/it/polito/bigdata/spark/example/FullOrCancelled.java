package it.polito.bigdata.spark.example;

public class FullOrCancelled {
	
	Integer full;
	Integer cancelled;
	Integer count;
	
	public FullOrCancelled(Integer full, Integer cancelled, Integer count) {
		this.full = full;
		this.cancelled = cancelled;
		this.count = count;
	}

	public Integer getFull() {
		return this.full;
	}

	public Integer getCancelled() {
		return this.cancelled;
	}
	
	public Integer getCount() {
		return this.count;
	}
}
