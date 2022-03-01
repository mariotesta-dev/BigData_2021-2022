package it.polito.bigdata.spark.example;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputFolder;
		String outputFolder2;

		inputPath = args[0];
		outputFolder = args[1];
		outputFolder2 = args[2];
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/** A. **/
		//Read input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath).cache();
		
		//filter year 2017
		JavaRDD<String> filterRDD = inputRDD.filter(line -> {
			String fields[] = line.split(",");
			
			String year = fields[1].split("/")[0];
			String app = fields[2];
			
			return year.compareTo("2017") == 0 && (app.compareTo("Windows 10") == 0 || app.compareTo("Ubuntu 18.04") == 0);
		});
		
		//Map key: {app, month} / value: {1}
		JavaPairRDD<String, PatchCount> appMonthRDD = filterRDD.mapToPair(line -> {
			
			String fields[] = line.split(",");
			String month = fields[1].split("/")[1];
			String app = fields[2];
			
			Integer winCount = 0;
			Integer ubuntuCount = 0;
			
			if(app.compareTo("Windows 10")==0) {
				winCount = 1;
			}else {
				ubuntuCount = 1;
			}
			
			return new Tuple2<String, PatchCount>(month, new PatchCount(winCount, ubuntuCount));
			
		});
		
		//Count num of patches using reduceByKey for each month
		JavaPairRDD<String, PatchCount> appMonthCount = appMonthRDD.reduceByKey((c1,c2) -> {
			
			return new PatchCount(c1.windows+c2.windows, c1.ubuntu + c2.ubuntu);
			
		}).cache().filter(line -> line._2().windows != line._2().ubuntu);
		

		//Map to the correct format
		JavaPairRDD<Integer, String> monthW = appMonthCount.mapToPair(line -> {
			String fields[] = line._1().split("_");
			
			Integer month = Integer.parseInt(fields[1]);
			String app = line._2().windows > line._2().ubuntu ? "W" : "U";
			
			return new Tuple2<Integer, String>(month, app);
		});
		
		monthW.saveAsTextFile(outputFolder);
		
				
		//The rest of lines are not saved in HDFS
	
		
		/** B. For each app, compute the windows of three consecutive months with many patches **/
		//Considering all applications, but only patches from 2018
		
		//Filter year == 2018
		JavaRDD<String> filter2018 = inputRDD.filter(line -> {
			String fields[] = line.split(",");
			String year = fields[1].split("/")[0];
			
			return year.compareTo("2018") == 0;
		});
		
		//Map key: {app, month}, value: {numOfPatches:1}
		JavaPairRDD<String, Integer> patches = filter2018.mapToPair(line -> {
			String fields[] = line.split(",");
			String month = fields[1].split("/")[1];
			String app = fields[2];
			
			return new Tuple2<String, Integer>(app+"_"+month, 1);
		});
		
		//Count for each app, month how many patches are present
		JavaPairRDD<String, Integer> numOfPatches = patches.reduceByKey((v1,v2) -> v1+v2);
		
		//Filter months with num of patches > 4
		JavaPairRDD<String, Integer> filtered4Patches = numOfPatches.filter(line -> line._2()>= 4);
		
		//use flatMapToPair to create "windows"
		JavaPairRDD<String, Integer> windows = filtered4Patches.flatMapToPair(line -> {
			ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
			
			Integer currentMonth = Integer.parseInt(line._1().split("_")[1]);
			String currentSoftware = line._1().split("_")[0];
			
			list.add(new Tuple2<>(currentMonth+"_"+currentSoftware, 1));
			
			if(currentMonth - 1 > 0) {
				list.add(new Tuple2<>(currentMonth-1+"_"+currentSoftware, 1));
			}
			
			if(currentMonth - 2 > 0) {
				list.add(new Tuple2<>(currentMonth-2+"_"+currentSoftware, 1));
			}
			
			return list.iterator();
			
		});
		
		//Count number of el of each window
		JavaPairRDD<String,Integer> countWindows = windows.reduceByKey((v1,v2) -> v1+v2);
		
		//filter if num of el == 3
		JavaPairRDD<String,Integer> filterCount3 = countWindows.filter(line -> line._2() == 3);
		
		filterCount3.keys().saveAsTextFile(outputFolder2);
		
		Integer n = (int) filterCount3.keys().map(line -> line.split("_")[1]).distinct().count();
		
		System.out.println("Number of software application in selected windows: " + n);
		
		
		sc.close(); 
		
	}
}
