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
		String inputPath2;
		String outputFolder;
		String outputFolder2;

		inputPath = args[0];
		inputPath2 = args[1];
		outputFolder = args[2];
		outputFolder2 = args[3];
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/** A. **/
		
		//input data
		JavaRDD<String> patchesRDD  = sc.textFile(inputPath)
				.cache(); //caching this since we are going to re-use it in part B
		
		//filter year 2018 and 2019
		JavaRDD<String> filteredPatches = patchesRDD.filter(line -> {
			
			String fields[] = line.split(",");
			Integer year = Integer.parseInt(fields[1].split("/")[0]);
			
			return year == 2018 || year == 2019;
		});
		
		//map SID, [is2017: 1 or 0, is2018: 1 or 0] to count numbers of patches per year for each SID
		JavaPairRDD<String, YearPatches> yearPatchesRDD = filteredPatches.mapToPair(line -> {
			
			String fields[] = line.split(",");
			Integer year = Integer.parseInt(fields[1].split(",")[0]);
			String SID = fields[0];
			
			if(year == 2018) {
				return new Tuple2<String, YearPatches>(SID, new YearPatches(1,0));	
			}else {
				return new Tuple2<String, YearPatches>(SID, new YearPatches(0,1));
			}
			
		});
		
		//count number of yearly patches (2018 and 2019 separately) per each SID
		JavaPairRDD<String, YearPatches> numOfYearlyPatches = yearPatchesRDD.reduceByKey((v1,v2) -> {
			
			Integer count2018 = v1.getPatchOf2018() + v2.getPatchOf2018();
			Integer count2019 = v1.getPatchOf2019() + v2.getPatchOf2019();
			
			return new YearPatches(count2018, count2019);
			
		});
		
		//filter only SIDs having count2019 < 0.5*count2018
		JavaPairRDD<String, YearPatches> filterRequestRDD = numOfYearlyPatches.filter(line -> {
			return line._2().getPatchOf2019() < 0.5*line._2().getPatchOf2018();
		});
		
		//get first file to retrieve model name
		JavaRDD<String> servers = sc.textFile(inputPath2); 
		
		//map as key: SID, value: modelName
		JavaPairRDD<String, String> serversRDD = servers.mapToPair(line -> {
			
			String fields[] = line.split(",");
			
			String SID = fields[0];
			String model = fields[1];
			
			return new Tuple2<String, String>(SID, model);
			
		})
		.cache();
		
		//join filterRequestRDD and serversRDD by SID
		JavaPairRDD<String, Tuple2<YearPatches, String>> joinRDD = filterRequestRDD.join(serversRDD);
		
		//map as (SID, Model)
		JavaPairRDD<String, String> resultA = joinRDD.mapToPair(line -> {
			return new Tuple2<String, String>(line._1(), line._2()._2());
		});
		
		resultA.saveAsTextFile(outputFolder);
		
		/** B. Servers with at most (al massimo) one applied patch per date **/
		
		//Map patches as key:{SID, date} - value: 1
		JavaPairRDD<String, Integer> mappedPatches = patchesRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			
			String SID = fields[0];
			String date = fields[1];
			
			return new Tuple2<String, Integer>(SID+"_"+date, 1);
		});
		
		//Count n of patches per each sid, date pair
		JavaPairRDD<String,Integer> numOfPatches = mappedPatches.reduceByKey((v1,v2) -> v1+v2);
		
		//Filter SIDs lines having num of patches > 1 (per each date)
		JavaPairRDD<String, Integer> filteredPatchNum = numOfPatches.filter(line -> {
			
			return line._2() > 1;
			
		});
		
		//Map to get SID only
		JavaPairRDD<String, String> SIDtoRemove = filteredPatchNum.mapToPair(line -> {
			return new Tuple2<String, String>(line._1().split(",")[0], "");
		});
		
		//Subtract from serversRDD
		JavaPairRDD<String, String> resultB = serversRDD.subtract(SIDtoRemove)
				.cache();
		
		//Save result
		resultB.saveAsTextFile(outputFolder2);
		
		//Print num of distinct models
		System.out.println("Number of distinct models: " + resultB.values().distinct().count());
	
		
		sc.close(); 
		
	}
}
