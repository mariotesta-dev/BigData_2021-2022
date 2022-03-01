package it.polito.bigdata.spark.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;


public class SparkDriver {

	public static void main(String[] args) {

		@SuppressWarnings("unused")
		String inputPath;
		String inputPath2;
		String inputPath3;
		String outputFolder;
		String outputFolder2;

		inputPath = "ProductionPlants.txt";
		inputPath2 = "Robots.txt";
		inputPath3 = "Failures.txt";
		outputFolder = "outPart1/";
		outputFolder2 = "outPart2/";
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		/** A. Production plants with at least one robot with at elast 50 failures in 2020 **/
		
		//Get failures.txt data
		JavaRDD<String> failuresRDD = sc.textFile(inputPath3);
		
		//Filter failures by year 2020
		JavaRDD<String> failures2020RDD = failuresRDD.filter(line -> {
			String fields[] = line.split(",");
			String year = fields[2].substring(0,4);
			
			return year.compareTo("2020") == 0;
		});
		
		//Map as key: RID, value: 1
		JavaPairRDD<String,Integer> counterFailuresRDD = failures2020RDD.mapToPair(line -> {
			String fields[] = line.split(",");
			String RID = fields[0];
			
			return new Tuple2<String,Integer>(RID, 1);
		});
		
		//Reduce to get number of failures per each robot in the year 2020
		JavaPairRDD<String, Integer> failuresPerRobotRDD = counterFailuresRDD.reduceByKey((v1,v2) -> v1+v2).cache();
		
		//Filter #failures >= 50
		JavaPairRDD<String, Integer> selectedRobotsRDD = failuresPerRobotRDD.filter(line -> line._2() >= 50);
		
		//Get robots.txt data
		JavaRDD<String> robotsRDD = sc.textFile(inputPath2);
		
		//Map as key: RID, value: PlantID
		JavaPairRDD<String, String> mappedRobotsRDD = robotsRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			String RID = fields[0];
			String plantID = fields[1];
			
			return new Tuple2<String, String>(RID, plantID);
		}).cache();
		
		//join
		JavaPairRDD<String, Tuple2<Integer, String>> joinedRDD = selectedRobotsRDD.join(mappedRobotsRDD);
		
		//get PID only and select distincts
		JavaRDD<String> selectedPIDs = joinedRDD.map(line -> {
			return line._2()._2();
		}).distinct();
		
		selectedPIDs.saveAsTextFile(outputFolder);

		
		/** B. For each production plant find the number of robots with at least one failure in 2020 **/
		
		//join mappedRobotsRDD with failuresPerRobotRDD
		JavaPairRDD<String, Tuple2<String, Optional<Integer>>> joined2RDD = mappedRobotsRDD.leftOuterJoin(failuresPerRobotRDD);
		
		//map as key: PID, value: 1 or 0
		JavaPairRDD<String, Integer> pidRDD = joined2RDD.mapToPair(line -> {
			String PID = line._2()._1();
			Optional<Integer> numOfFailures = line._2()._2(); //can be null
			
			if(numOfFailures.isPresent() && numOfFailures.get() >= 1) {
				return new Tuple2<String,Integer>(PID, 1);
			}else {
				return new Tuple2<String,Integer>(PID, 0);
			}
			
		});
		
		JavaPairRDD<String,Integer> resultB = pidRDD.reduceByKey((v1,v2) -> v1+v2);
		
		resultB.saveAsTextFile(outputFolder2);
				
		
		sc.close(); 
		
	}
}
