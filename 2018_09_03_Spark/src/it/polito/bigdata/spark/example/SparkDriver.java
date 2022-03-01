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
		
		/** A. Number of failures per data center in 2017 **/
		
		//Read from failures.txt
		JavaRDD<String> inputRDD = sc.textFile(inputPath1);
		
		//Filter lines: year 2017
		JavaRDD<String> filteredYearRDD = inputRDD.filter(line -> {
			
			String fields[] = line.split(",");
			
			String year = fields[0].split("/")[0];
			
			return year.compareTo("2017") == 0;
		}).cache();
		
		//Map failures as <SID, 1>
		JavaPairRDD<String, Integer> serverFailuresRDD = filteredYearRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			return new Tuple2<String, Integer>(fields[2], 1);
		});
		
		//Read servers.txt
		JavaPairRDD<String, String> serversRDD = sc.textFile(inputPath2).mapToPair(line -> {
			
			String fields[] = line.split(",");
			String sID = fields[0];
			String dcID = fields[2];
			
			return new Tuple2<String, String>(sID,dcID);
			
		});
		
		//Join serverFailuresRDD and serverRDD <sID, [dcId, 1]>
		JavaPairRDD<String, Tuple2<String, Integer>> joinedRDD = serversRDD.join(serverFailuresRDD);
		
		//Get just [dcId, 1]
		JavaPairRDD<String, Integer> dataCenterFailedRDD = joinedRDD.mapToPair(line -> {
			return new Tuple2<String, Integer>(line._2()._1(), line._2()._2());
		});
		
		//count number of failures per each dcID
		JavaPairRDD<String, Integer> dataCenterFailuresRDD = dataCenterFailedRDD.reduceByKey((v1, v2) -> {
			return v1+v2;
		}).filter(line -> line._2() >= 365);
		
		dataCenterFailuresRDD.saveAsTextFile(outputFolder);
		
		
		
		/** B. SIDs of faulty servers in 2017 **/
		//Faulty server means that the server had at least 2 failures per month && sumOfDownTime > 1440 mins
		//So to do this analysis I need: [sID, month, downTime]
		
		//Map RDD to get just the data needed <"sID, month" | [downTime, +1]>
		JavaPairRDD<String, Counter> mappedRDD = filteredYearRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			
			String sID = fields[2];
			String month = fields[0].split("/")[1];
			Integer downTime = Integer.parseInt(fields[4]);
			
			return new Tuple2<String, Counter>(sID + "," + month, new Counter(downTime, 1));
			
		});
		
		//Reduce by key <sID, month> to sum downTime and numOfFailures (per month and per sID)
		JavaPairRDD<String, Counter> reducedRDD = mappedRDD.reduceByKey((c1, c2) -> {
			
			Integer downTime = 0;
			Integer count = 0;
			
			downTime = c1.downTime + c2.downTime;
			count = c1.count + c2.count;
			
			return new Counter(downTime, count);
			
		});
		// Here I have for each "sID, month" -> [downTime, numOfFailures], so I have to map for each sID now
		
		JavaPairRDD<String, MonthCounter> sIDMappedRDD = reducedRDD.mapToPair(line -> {
			
			String fields[] = line._1().split(",");
			
			String sID = fields[0];
			
			if(line._2().count >= 2) {
				return new Tuple2<String, MonthCounter>(sID, new MonthCounter(line._2().downTime, 1));
			}else {
				return new Tuple2<String, MonthCounter>(sID, new MonthCounter(line._2().downTime, 0));
			}
			
			//I have <sID, [downTimePerMonth, monthHadAtLeast2Failures: 1 or 0]
			
		});
		
		//For each sID count number of months with at least 2 failures and in the meantime sum downtime value
		JavaPairRDD<String, MonthCounter> almostResultRDD = sIDMappedRDD.reduceByKey((c1,c2) -> {
			
			Integer totalDownTime = 0;
			Integer numberOfMonths = 0;
			
			totalDownTime = c1.totalDownTime + c2.totalDownTime;
			numberOfMonths = c1.numOfMonthsWithAtLeast2Failures + c2.numOfMonthsWithAtLeast2Failures;
			
			return new MonthCounter(totalDownTime, numberOfMonths);
			
		});
		
		//Filter lines having numOfMonths == 12 && totalDownTime > 1440
		JavaPairRDD<String, MonthCounter> resultB_RDD = almostResultRDD.filter(line -> {
			
			if(line._2().numOfMonthsWithAtLeast2Failures == 12 && line._2().totalDownTime > 1440) {
				return true;
			}else {
				return false;
			}
		});
		
		resultB_RDD.keys().saveAsTextFile(outputFolder2);
		
		sc.close(); 
		
	}
}
