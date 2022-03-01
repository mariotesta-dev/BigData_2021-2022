package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class SparkDriver {

	public static void main(String[] args) {

		String inputPath1;
		String inputPath2;
		String outputFolder;
		String outputFolder2;

		inputPath1 = args[0];
		inputPath2 = args[1];
		outputFolder = args[2];
		outputFolder2 = args[3];
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/** A. Critial bicycles during year 2018 **/
		
		//Read input file failures.txt
		JavaRDD<String> failuresRDD = sc.textFile(inputPath2);
		
		//Filter failures related to 2018
		JavaRDD<String> filteredRDD = failuresRDD.filter(line -> {
			
			String fields[] = line.split(",");
			String year = fields[0].split("_")[0].split("/")[0];
			
			return year.compareTo("2018") == 0;
			
		}).cache();
		
		//Map to have key: {bid, month} / value: {wheelFailure 1 or 0}
		JavaPairRDD<String, Integer> mappedFailuresRDD = filteredRDD.mapToPair(line -> {
			
			String fields[] = line.split(",");
			String BID = fields[1];
			String month = fields[0].split("_")[0].split("/")[0];
			String failure = fields[2];
			
			if(failure.compareTo("wheel")==0) {
				return new Tuple2<String, Integer>(BID + "," + month, 1);
			}else {
				return new Tuple2<String, Integer>(BID + "," + month,0);
			}
		});
		
		//Reduce to count for each bid,month the number of wheel failures
		JavaPairRDD<String,Integer> wheelFailuresPerMonthRDD = mappedFailuresRDD.reduceByKey((v1,v2) -> {
			return v1+v2;
		});
		
		//Filter only bid,month with value > 2
		JavaPairRDD<String, Integer>filteredFailuresRDD = wheelFailuresPerMonthRDD.filter(line -> {
			return line._2() > 2;
		});
		
		//Map bid only
		JavaRDD<String> resultA_RDD = filteredFailuresRDD.map(line -> line._1().split(",")[0]);
		
		resultA_RDD.distinct().saveAsTextFile(outputFolder);
		
		
		/** B. Cities characterized by bicycles with failures in 2018 **/
		
		
		//Map failures to get key: {BID} / value:{1}
		JavaPairRDD<String, Integer> failuresMappedRDD = filteredRDD.mapToPair(line -> {
			
			String fields[] = line.split(",");
			
			return new Tuple2<String, Integer>(fields[1],1);
			
		});
		
		JavaPairRDD<String,Integer> allFailuresRDD = failuresMappedRDD.reduceByKey((v1,v2) -> v1+v2);
		
		//filter BID having > 20 failures
		JavaPairRDD<String, Integer> BIDsMore20Failure = allFailuresRDD.filter(line -> line._2() > 20);
		
		//Read input file bicycles.txt
		JavaRDD<String> bicyclesRDD = sc.textFile(inputPath1).cache();
				
			//Map bicycles to get key: {BID} / value: {city,country}
		JavaPairRDD<String, String> bicyclesMappedRDD = bicyclesRDD.mapToPair(line -> {
				
			String fields[] = line.split(",");
			String BID = fields[1];
			String city = fields[2];
			String country = fields[3];
				
			return new Tuple2<String, String>(BID, city+","+country);
		});
		
		//join the two rdds
		JavaPairRDD<String, Tuple2<String,Integer>> joinRDD =  bicyclesMappedRDD.join(BIDsMore20Failure);
		
		//Get city only and remove duplicates
		JavaRDD<String> citiesToRemove = joinRDD.map(line -> {
			return line._2()._1(); //return city,country only
		}).distinct();
		
		//Get all cities
		JavaRDD<String> allCities = bicyclesRDD.map(line -> {
			
			return line.split(",")[2]+ "," + line.split(",")[3];
		});
		
		allCities.subtract(citiesToRemove).saveAsTextFile(outputFolder2);
		
		System.out.println("Number of selected cities: " + allCities.subtract(citiesToRemove).count());
		
		sc.close(); 
		
	}
}
