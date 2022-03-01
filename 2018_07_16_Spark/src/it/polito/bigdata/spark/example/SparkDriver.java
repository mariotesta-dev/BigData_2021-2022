package it.polito.bigdata.spark.example;

import java.util.ArrayList;
import java.time.LocalDate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class SparkDriver {

	public static void main(String[] args) {

		String inputPath1;
		String outputFolder;
		String outputFolder2;

		inputPath1 = args[0];
		outputFolder = args[1];
		outputFolder2 = args[2];
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/** A. Analyses of daily variations in year 2017 **/
	
		//Read the fiel
		JavaRDD<String> inputRDD = sc.textFile(inputPath1);
		
		//filter year 2017
		JavaRDD<String> filteredYearRDD = inputRDD.filter(line -> {
			String fields[] = line.split(",");
			
			String year = fields[1].split("/")[0];
			
			return year.compareTo("2017") == 0;
		
		});
		
		//Compute for each stock,date pair... the highest and lowest price, so i store the price for 2 times
		JavaPairRDD<String, MinMax> mappedRDD = filteredYearRDD.mapToPair(line -> {
			
			String fields[] = line.split(",");
			String stock = fields[0];
			String date = fields[1];
			Double price = Double.parseDouble(fields[3]);
			
			return new Tuple2<String, MinMax>(stock + "," + date, new MinMax(price, price));
			
		});
		
		//Reduce to calculate at the same time max and min price for a given date
		JavaPairRDD<String, MinMax> minMaxPricesRDD = mappedRDD.reduceByKey((p1,p2) -> {
		
			MinMax newValues = new MinMax(p1.min, p1.max);
			
			if(p2.min > newValues.min) {
				newValues.min = p2.min;
			}
			
			if(p2.max > newValues.max) {
				newValues.max = p2.max;
			}
			
			return newValues;
		});
		
		//Map each line to get daily price variation
		JavaPairRDD<String, Double> variationRDD = minMaxPricesRDD.mapToPair(line -> {
			
			return new Tuple2<String, Double>(line._1(), line._2().max - line._2().min);
		}).cache();
		
		//Filter variation > 10
		JavaPairRDD<String,Double> filteredVariationRDD = variationRDD.filter(line -> line._2() > 10.0);
		
		//output
		filteredVariationRDD.keys().saveAsTextFile(outputFolder);
		
		/** B. Stable trends in year 2017 for each stock **/
		//Given a stock and two consecutive dates, see if there is a stable trend
		//two dates are in a stable trend if the difference between the 2 daily variation is <= 0.1
		
		/* Basic idea is to have for each stock,date... a tuple stock,date-1 with the same variation
		 * In this way, i can then use group by to aggregate date_x with its predecessor just by decreasing the
		 * value of the date by 1 day
		 * */
		
		JavaPairRDD<String, Double> strangeRDD = variationRDD.flatMapToPair(line -> {
			
			String key[] = line._1().split(",");
			String stock = key[0];
			String date = key[1];
			
			String precedentDate = LocalDate.parse(date).minusDays(1).toString();
			
			ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
			
			list.add(new Tuple2<>(line._1(), line._2()));
			
			list.add(new Tuple2<>(stock+","+precedentDate, line._2()));
			
			return list.iterator();
			
		});
		
		JavaPairRDD<String, Iterable<Double>> groupedRDD = strangeRDD.groupByKey();
		
		JavaPairRDD<String, Iterable<Double>> almostResultRDD = groupedRDD.filter(line -> {
			
			Double variation1 = null;
			Double variation2 = null;
			
			for(Double v : line._2()) {
				if(variation1 == null) {
					variation1 = new Double(v);
				}else {
					variation2 = new Double(v);
				}
			}
			
			if(Math.abs(variation2-variation1) <= 0.1) {
				return true;
			}else {
				return false;
			}
			
		});
		
		//I must return stockId, firstDate, so..
		almostResultRDD.keys().saveAsTextFile(outputFolder2);
		
		
		
		sc.close(); 
		
	}
}
