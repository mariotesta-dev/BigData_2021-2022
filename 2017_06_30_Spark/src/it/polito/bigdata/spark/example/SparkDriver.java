package it.polito.bigdata.spark.example;


import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;		 //prices.txt
		Integer NW;
		String outputFolder1;
		String outputFolder2;

		inputPath = args[0];
		NW = Integer.parseInt(args[1]);
		outputFolder1 = args[2];
		outputFolder2 = args[3];

		// Create a Spark Session object and set the name of the application
		//SparkSession ss = SparkSession.builder().appName("Exam 2017-06-30 Spark").getOrCreate();
		
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
				
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		/** A. Lowest stock price per pair (stock, date) **/

		//Read input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		//filter year=2016
		JavaRDD<String> filteredRDD = inputRDD.filter(line -> {
			String fields[] = line.split(",");
			String year = fields[1].split("/")[0];
			
			return year.compareTo("2016") == 0;
		});
		
		//create pairRDD as <"Stock,date" , price>
		JavaPairRDD<String, Double> stockDatePriceRDD = filteredRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			
			return new Tuple2<String, Double>(fields[0]+","+fields[1], Double.parseDouble(fields[3]));
		});
		
		//get lowest foreach stock,date pair + sorting by key... since the key is stock
		//the sort is going to be automatically based on stock first and then date
		JavaPairRDD<String,Double> lowestPriceRDD = stockDatePriceRDD.reduceByKey((v1,v2) -> {
			if(v1.doubleValue() < v2.doubleValue())
				return v1;
			else
				return v2;
		}).sortByKey(false);
		
		
		lowestPriceRDD.saveAsTextFile(outputFolder1);
		
		
		/** B. Stocks characterized by a "positive weekly trend" **/
		/* Basic idea: we need the highest price of the first day and the highest price of the last day
		 * for each pair (stock, week), considering only 2016.
		 * Given (stock, week, maxfirstday, maxlastday) we can compute (stock, week, difference)
		 * */
		
		//Get max value of price from each day --> pair <"stock,week,date" , price>
		JavaPairRDD<String, Double> maxPriceForEachDay = filteredRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			Integer weekNum = DatePrice.weekNumber(fields[1]);
		
			return new Tuple2<String, Double>(fields[0]+"," + weekNum + "," + fields[1], Double.parseDouble(fields[3]));
		}).reduceByKey((v1,v2) -> {
			if(v1 > v2)
				return v1;
			else
				return v2;
		});
		
		//Create pair <"stock, week" , "date, price">
		JavaPairRDD<String, DatePrice> stockWeekDatePriceRDD = maxPriceForEachDay.mapToPair(line -> {
			
			String fields[] = line._1().split(",");
			
			String stock = fields[0];
			Integer weekNum = Integer.parseInt(fields[1]);
			
			return new Tuple2<String, DatePrice>(stock+","+weekNum, new DatePrice(fields[2], line._2()));
			
		});
		
		//Group by key "stock, week" so we get an iterable of DatePrice
		/** IMPORTANT: GroupByKey() is highly inefficient! More recent exercises have better solutions! **/
		JavaPairRDD<String, Iterable<DatePrice>> stockWeekGroupByRDD = stockWeekDatePriceRDD.groupByKey();
		
		/* Using flatMapToPair to reduce from groupByKey, we get a list of DatePrice, we sort it by date and get
		 * first and last element (so first and last day) already with the maxPrice for that day
		 * We then compute the difference and return one single tuple2<"stock, week" , difference>
		 * */
		JavaPairRDD<String, Double> stockWeekDifferenceRDD = stockWeekGroupByRDD.flatMapToPair(tuple -> {
			ArrayList<Tuple2<String,DatePrice>> list = new ArrayList<>();
			
		for(DatePrice e : tuple._2()) {
				list.add(new Tuple2<String, DatePrice>(tuple._1(), e));
		}
			
		//sort list by date and return just first and last element
		list.sort((e1, e2) -> e1._2().getDate().compareTo(e2._2().getDate()));
		
		Double difference = list.get(list.size()-1)._2().getPrice() - list.get(0)._2().getPrice();
			
		ArrayList<Tuple2<String,Double>> finale = new ArrayList<>();
		finale.add(new Tuple2<String,Double>(tuple._1(), difference));
			
			return finale.iterator();
			
		});
		
		//return a tuple<stock, 1 or 0> --> to count if that week is a positive trend week for the stock
		JavaPairRDD<String, Integer> isAPositiveWeek = stockWeekDifferenceRDD.mapToPair(line -> {
			return new Tuple2<String, Integer>(line._1().split(",")[0], line._2() > 0 ? 1 : 0);
		});
		
		//sum values to get tot number of positive trends week per stock
		JavaPairRDD<String,Integer> numWeeksGreaterThanNW = isAPositiveWeek.reduceByKey((v1,v2) -> v1+v2);
		
		JavaRDD<String> resultRDD = numWeeksGreaterThanNW.filter(line -> line._2() > NW).map(line -> line._1());
		
		resultRDD.saveAsTextFile(outputFolder2);
		
		sc.close();
		
	}
}
