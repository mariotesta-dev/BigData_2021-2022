package it.polito.bigdata.spark.example;

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
		
		//Get data from input and filter only lines between June 1, 2015 & August 31, 2015
		JavaRDD<String> inputRDD = sc.textFile(inputPath)
				.filter(line -> {
					String fields[] = line.split(",");
					
					if(fields[0].compareTo("2015/06/01") >= 0 && fields[0].compareTo("2015/08/31") <= 0) {
						return true;
					}else {
						return false;
					}
					
				});
		
		JavaPairRDD<String, Tuple2<Double, Integer>> mappedRDD = inputRDD.mapToPair(line -> {
			
			String fields[] = line.split(",");
			String cityCountry = fields[1] + "-" + fields[2];
			
			return new Tuple2<String, Tuple2<Double, Integer>>(cityCountry, new Tuple2<>(Double.parseDouble(fields[3]),1)); //city-country , maxTemp
		}).reduceByKey((t1,t2) -> {
			return new Tuple2<>(t1._1()+t2._1(), t1._2()+t2._2());
		});
		
		JavaPairRDD<String, Double> avgMaxTempRDD = mappedRDD.mapValues(line -> {
			return line._1()/line._2();
		});
		
		avgMaxTempRDD.saveAsTextFile(outputFolder);
		
		/** B. **/
		//We already have the list of cities with thir avg temperature (and date constraints as well)
		
		//But we must find the average maxTemp for each country first and then check for each city if their maxTemp is at least 5deg hotter
		JavaPairRDD<String, CityAvg> countryCityTempRDD = avgMaxTempRDD.mapToPair(line -> {
			String fields[] = line._1().split("-");
			return new Tuple2<String, CityAvg>(fields[1], new CityAvg(fields[0], line._2()));
		});
		
		//map as <country, [maxtemp, +1]> and calculate sums
		JavaPairRDD<String, Tuple2<Double,Integer>> countriesAvgRDD = countryCityTempRDD.mapToPair(line -> {
			String country = line._1().split("-")[1];
			
			return new Tuple2<>(country, new Tuple2<>(line._2().getMaxTempAvg(),1));
			
		}).reduceByKey((t1,t2) -> {
			return new Tuple2<>(t1._1()+t2._1(), t1._2()+t2._2());
		});
		
		//calculate average
		JavaPairRDD<String, Double> countryAvg = countriesAvgRDD.mapValues(line -> {
			return line._1()/line._2();
		});
		
		//Now i can join the two RDDs
		JavaPairRDD<String, String> CountryCityAvgCity = avgMaxTempRDD.mapToPair(line -> {
			String fields[] = line._1().split("-");
			
			return new Tuple2<String, String>(fields[1], fields[0] + "," + line._2()); //country, [city, temp]
		});
		
		//country(line.1), [city, temp ,,, countrytemp]
		JavaPairRDD<String, Tuple2<String,Double>> countryCityAvgCityAvgCountry = CountryCityAvgCity.join(countryAvg);
		
		//<city-country , [temp, conuntryTemp]
		JavaRDD<String> resultRDD = countryCityAvgCityAvgCountry.mapToPair(line -> {
			String fields[] = line._2()._1().split(",");
			
			return new Tuple2<String,String>(fields[0]+"-"+line._1(),fields[1]+","+line._2()._2());
		}).filter(line -> {
			String fields[] = line._2().split(",");
			
			Double cityTempAvg = Double.parseDouble(fields[0]);
			Double countryTempAvg = Double.parseDouble(fields[1]);
			
			if(cityTempAvg.compareTo(countryTempAvg+5) > 0) return true;
			else
				return false;
			
		}).map(line -> line._1());
		
		resultRDD.saveAsTextFile(outputFolder2);
		
		sc.close(); 
		
	}
}
