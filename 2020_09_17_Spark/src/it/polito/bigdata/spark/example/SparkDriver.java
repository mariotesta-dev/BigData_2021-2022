package it.polito.bigdata.spark.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		String inputPath3;
		String outputFolder;
		String outputFolder2;

		inputPath = args[0];
		inputPath2 = args[1];
		inputPath3 = args[2];
		outputFolder = args[3];
		outputFolder2 = args[4];
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		/** A. **/
		// Selecting all movies that have been watched between 2015 and 2020
		
		//read inputfile
		JavaRDD<String> watchedMoviesRDD = sc.textFile(inputPath3)
				.cache();
		
		//filter year range 2015-2020
		JavaRDD<String> watchedMoviesFiltRDD = watchedMoviesRDD.filter(line -> {
			
			String fields[] = line.split(",");
			
			Integer year = Integer.parseInt(fields[2].split("_")[0].split("/")[0]);
			
			if(year >= 2015 && year <=2020) {
				return true;
			}else {
				return false;
			}
			
		});
		
		//map each line as key:{movie,year} - value{1}
		JavaPairRDD<String, Integer> yearlyCounterRDD = watchedMoviesFiltRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			String movie = fields[1];
			Integer year = Integer.parseInt(fields[2].split("_")[0].split("/")[0]);
			
			return new Tuple2<String, Integer>(movie+"_"+year, 1);
		});
		
		
		//count for each movie, year the number of times it has been watched <"movie_year",#timeViewed>
		JavaPairRDD<String, Integer> yearlyCountRDD = yearlyCounterRDD.reduceByKey((v1,v2) -> v1+v2)
				.cache(); 
		
		//filter lines having #timeViewed > 1000
		JavaPairRDD<String, Integer> moreThan1000TimesRDD = yearlyCountRDD.filter(line -> line._2() >= 1000);
		
		//map yearlyCountRDD as key:{movie} - value:{#timeViewed > 0 ? 1 : 0}
		JavaPairRDD<String,Integer> counterOfYearsPerMovieRDD = yearlyCountRDD.mapToPair(line -> {
			
			String fields[] = line._1().split("_");
			String movie = fields[0];
			Integer numTimeViewed = line._2();
			
			return new Tuple2<String,Integer>(movie, numTimeViewed > 0 ? 1 : 0);
			
		});
		
		//count number of years a movie have been viewed
		JavaPairRDD<String,Integer> numberOfYearsPerMovieRDD = counterOfYearsPerMovieRDD.reduceByKey((v1,v2) -> v1+v2);
		
		//filter only years having #years == 1
		JavaPairRDD<String, Integer> filtered1YearRDD = numberOfYearsPerMovieRDD.filter(line -> line._2() == 1);
		
		//join movies viewed 1y only with "movie,year" viewed > 1000 times
		//but first map the 2nd RDD
		JavaPairRDD<String, YearWatches> yearWatchesRDD = moreThan1000TimesRDD.mapToPair(line -> {
			
			String fields[] = line._1().split("_");
			String movie = fields[0];
			String year = fields[1];
			
			return new Tuple2<String, YearWatches>(movie, new YearWatches(year, line._2()));
			
		});
		
		JavaPairRDD<String, Tuple2<Integer, YearWatches>> joinedRDD = filtered1YearRDD.join(yearWatchesRDD);
		
		//map to correct format
		JavaRDD<String> resultA = joinedRDD.map(line -> {
			
			return line._1() + "," + line._2()._2().getYear();
			
		});
		
		resultA.saveAsTextFile(outputFolder);		
		
		
		/** B. Most popular movie in the last 2 years **/
		
		//considering all years, select the movies that have been the most popular in at least 2y
		//annual popularity is given by #distinct users who watched that movie in that specific year
		
		//from watchedMoviesRDD ...
		JavaRDD<String> userMovieYearRDD = watchedMoviesRDD.map(line -> {
			String fields[] = line.split(",");
			String user = fields[0];
			String movie = fields[1];
			String year = fields[2].substring(0,4);
			
			return user + "_" + movie + "_" + year;
		}).distinct(); //"distinct user has watched that movie that year"
		
		JavaPairRDD<String, Integer> movieYearRDD = userMovieYearRDD.mapToPair(line -> {
			
			String fields[] = line.split("_");
			
			return new Tuple2<String, Integer>(fields[2] + "_" + fields[3], 1);
			
		});
		
		//count for each movie,year the number of distinct users who have watched
		JavaPairRDD<String,Integer> numOfUsersPerYRDD = movieYearRDD.reduceByKey((v1,v2) -> v1+v2);
		
		//map to have key: year
		JavaPairRDD<String, MovieWatches> yearMovieNumRDD = numOfUsersPerYRDD.mapToPair(line -> {
			
			String fields[] = line._1().split("_");
			
			return new Tuple2<String, MovieWatches>(fields[1], new MovieWatches(fields[0], line._2()));
			
		});
		
		//get per each year, the most popular movie
		JavaPairRDD<String, MovieWatches> annualMostPopularRDD = yearMovieNumRDD.reduceByKey((m1, m2) -> {
			
			if(m1.getNumOfUsers() > m2.getNumOfUsers()) {
				return m1;
			}else {
				return m2;
			}
			
		});
		
		//map MID, +1
		JavaPairRDD<String, Integer> counterNumOfYearsRDD = annualMostPopularRDD.mapToPair(line -> {
			
			String movie = line._2().getMovie();
			
			return new Tuple2<String, Integer>(movie, 1);
			
		});
		
		//sum values
		
		JavaPairRDD<String, Integer> sumOfYearsRDD = counterNumOfYearsRDD.reduceByKey((v1,v2) -> v1+v2);
		
		//filter values >= 2
		JavaPairRDD<String, Integer> resultB = sumOfYearsRDD.filter(line -> line._2() >= 2);
		
		resultB.keys().saveAsTextFile(outputFolder2);
		
		sc.close(); 
		
	}
}
