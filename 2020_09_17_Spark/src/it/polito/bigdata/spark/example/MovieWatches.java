package it.polito.bigdata.spark.example;

public class MovieWatches {
	
	String movie;
	Integer numOfUsers;
	
	public MovieWatches(String m, Integer n) {
		this.movie = m;
		this.numOfUsers = n;
	}

	public String getMovie() {
		return movie;
	}

	public Integer getNumOfUsers() {
		return numOfUsers;
	}


}
