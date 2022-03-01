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

		
		/** A. Maximum number of daily purchases per book in 2018 **/
		
		//read purchases.txt
		JavaRDD<String> purchases = sc.textFile(inputPath);
		
		//filter only purchases in 2018+
		JavaRDD<String> purchases2018 = purchases.filter(line -> {
			String fields[] = line.split(",");
			String year = fields[2].substring(0,4);
			
			return year.compareTo("2018") == 0;
		}); 
		
		//map as key: {bid, date} - value:{1}
		JavaPairRDD<String, Integer> countPurchases = purchases2018.mapToPair(line -> {
			
			String fields[] = line.split(",");
			
			String BID = fields[1];
			String date = fields[2];
			
			return new Tuple2<String, Integer>(BID+"_"+date, 1);
			
		});
		
		//count number of purchases per each day
		JavaPairRDD<String, Integer> dailyPurchases = countPurchases.reduceByKey((v1,v2) -> v1+v2)
				.cache();
		
		//map as key: BID - value : daily purchases
		JavaPairRDD<String, Integer> bidPurch = dailyPurchases.mapToPair(line -> {
			
			return new Tuple2<String, Integer>(line._1().split("_")[0], line._2());
			
		}).cache();
		
		//find max of daily numOfPurchases per each BID
		JavaPairRDD<String, Integer> maxNumPurch = bidPurch.reduceByKey((v1,v2) -> {
			
			return v1 > v2 ? v1 : v2;
			
		});
		
		maxNumPurch.saveAsTextFile(outputFolder);
		
		
		/** B. Windows of 3 consecutive days with many purchases **/
		
		//Calculate total number of purchases for each BID in the year 2018
		JavaPairRDD<String, Integer> totalPurch = bidPurch.reduceByKey((v1,v2) -> v1+v2);
		
		//Map countPurchases as key: BID - value: [date, numOfPurchases]
		JavaPairRDD<String, DatePurchases> reMappedCountPurchases = countPurchases.mapToPair(line -> {
			
			String fields[] = line._1().split("_");
			String BID = fields[0];
			String date = fields[1];
			Integer numOfPurchases = line._2();
			
			return new Tuple2<String, DatePurchases>(BID, new DatePurchases(date, numOfPurchases));
			
		});
		
		//join the two --> key: BID - value: {[date, #dailyPurch] , total}
		JavaPairRDD<String, Tuple2<DatePurchases, Integer>> dailyAndMaxRDD = reMappedCountPurchases.join(totalPurch);
		
		//select for each book the dates with more than #dailyPurch > 0.1*total
		JavaPairRDD<String, Tuple2<DatePurchases, Integer>> selectedDailyAndMaxRDD = dailyAndMaxRDD.filter(line -> {
			
			Integer daily = line._2()._1().getNumOfPurchases();
			Integer total = line._2()._2();
			
			return daily > 0.1*total;
			
		});
		
		//create windows of 3 consecutive dates for each book
		// (bid_date, 1), (bid_date-1, 1), (bid_date-2, 1)
		
		JavaPairRDD<String, Integer> windows = selectedDailyAndMaxRDD.flatMapToPair(line -> {
			
			ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
			
			String bid = line._1();
			String date = line._2()._1().getDate();
			
			String oneDayAgo = DateTool.previousDeltaDate(date, 1);
			String twoDaysAgo = DateTool.previousDeltaDate(date, 2);
			
			list.add(new Tuple2<>(bid+","+date, 1));
			list.add(new Tuple2<>(bid+","+oneDayAgo, 1));
			list.add(new Tuple2<>(bid+","+twoDaysAgo, 1));
			
			return list.iterator();
			
		});
		
		//count number of dates for each window
		JavaPairRDD<String, Integer> countWindows = windows.reduceByKey((v1,v2) -> v1+v2);
		
		//filter lines having v == 3
		JavaPairRDD<String, Integer> selectedWindows = countWindows.filter(line -> line._2() == 3);
		
		selectedWindows.keys().saveAsTextFile(outputFolder2);
		
		
		sc.close(); 
		
	}
}
