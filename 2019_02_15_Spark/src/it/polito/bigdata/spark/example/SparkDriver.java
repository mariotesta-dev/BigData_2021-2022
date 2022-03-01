package it.polito.bigdata.spark.example;

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
		
		/** A.  **/
		
		//Read the file POIs.txt
		JavaRDD<String> inputRDD = sc.textFile(inputPath1);
		
		//Filter italian cities
		JavaRDD<String> filteredCountryRDD = inputRDD.filter(line -> {
			String fields[] = line.split(",");
			String country = fields[4];
			
			return country.compareTo("Italy") == 0;
		}).cache();
		
		//Map <city, CountCategory[taxi: 1 or 0, bus: 1 or 0]>
		JavaPairRDD<String, CountCategory> cityCountRDD = filteredCountryRDD.mapToPair(line -> {
		
			String fields[] = line.split(",");
			String city = fields[3];
			String subcategory = fields[6];
			
			Integer isTaxi = 0;
			Integer isBus = 0;
			
			if(subcategory.compareTo("Taxi") == 0) {
				isTaxi = 1;
			}else if(subcategory.compareTo("Busstop") == 0) {
				isBus = 1;
			}
			
			return new Tuple2<String, CountCategory>(city, new CountCategory(isTaxi, isBus));
			
		});
		
		//Count numberOfTaxi POI and numberOfBus POI, per each city
		JavaPairRDD<String, CountCategory> numOfPOIsRDD = cityCountRDD.reduceByKey((c1,c2) -> {
			
			Integer newTaxi = c1.numOfTaxi + c2.numOfTaxi;
			Integer newBus = c1.numOfBus + c2.numOfBus;
			
			return new CountCategory(newTaxi, newBus);
			
		});
		
		//Filter only city with TaxiNum >= 1 && BusNum = 0
		JavaPairRDD<String, CountCategory> filteredCitiesRDD = numOfPOIsRDD.filter(line -> {
			
			Integer numOfTaxis = line._2().numOfTaxi;
			Integer numOfBus = line._2().numOfBus;
			
			if(numOfTaxis >= 1 && numOfBus == 0) {
				return true;
			}else {
				return false;
			}
			
		});
		
		filteredCitiesRDD.keys().saveAsTextFile(outputFolder);
		
		
		/** B. Italian cities with "many" museums with respect to the other italian cities **/
		//Considering only italian cities, select the cities with a number of museum POIs > average number of museums in italy
		
		//we must calculate the average and then for each city compare it with its number of museum
		JavaPairRDD<String, Integer> museumsRDD = filteredCountryRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			String city = fields[3];
			String subcategory = fields[6];
			
			return new Tuple2<String, Integer>(city, subcategory.compareTo("museum") == 0 ? 1 : 0);
		});
		
		//count num of museums per city
		JavaPairRDD<String, Integer> numOfMuseumsRDD = museumsRDD.reduceByKey((v1,v2) -> {
			return v1+v2;
		}).cache();
		
		//find average numOfMuseums, since each line refers to a city + its numOfMuseums
		JavaRDD<AverageCount> averageCountRDD = numOfMuseumsRDD.map(line -> {
			return new AverageCount(1, line._2());
		});
		
		//calculate sum
		AverageCount averageC = averageCountRDD.reduce((a1,a2) -> {
			return new AverageCount(a1.numOfCities+a2.numOfCities, a1.numOfMuseums + a2.numOfMuseums);
		});
		
		Double average = (double) averageC.numOfMuseums / (double) averageC.numOfCities;
		
		//Filter italian cities having numOfMuseums > average
		JavaPairRDD<String, Integer> filteredMuseumRDD = numOfMuseumsRDD.filter(line -> {
			Double n = (double) line._2();
			return n > average;
		});
		
		filteredMuseumRDD.keys().saveAsTextFile(outputFolder2);
		
		
		sc.close(); 
		
	}
}
