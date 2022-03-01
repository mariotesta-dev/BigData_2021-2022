package it.polito.bigdata.spark.example;

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
		
		JavaPairRDD<String, String> airportsRDD = sc.textFile(inputPath2).mapToPair(line -> {
			String fields[] = line.split(",");
			
			return new Tuple2<String,String>(fields[0], fields[3] + "," + fields[1]);
		}).filter(line -> line._2().split(",")[0].compareTo("Germany") == 0)
				.mapToPair(line -> {
					return new Tuple2<String, String>(line._1(), line._2().split(",")[1]);
				});
		// here I have --> <airportId,airportName>
		
		
		JavaPairRDD<String, AirlineDelay> flightsRDD = sc.textFile(inputPath).mapToPair(line -> {
			String fields[] = line.split(",");
			
			return new Tuple2<String, AirlineDelay>(fields[5], new AirlineDelay(fields[1], Integer.parseInt(fields[7])));
		}); 
		// --> here I have: <dep_airportID , [airline, 1 or 0]>
		
		JavaPairRDD<String, Tuple2<String,AirlineDelay>> joinedRDD = airportsRDD.join(flightsRDD);
		// --> here I have: <dep_airportId, [airportName, [airline, 1 or 0]]
		
		JavaPairRDD<String, Integer> mappedRDD = joinedRDD.mapToPair(line -> {
			
			String airportName = line._2()._1();
			String airline = line._2()._2().getAirline();
			Integer delay = line._2()._2().getDelay();
			
			return new Tuple2<String, Integer>(airline + "," + airportName, delay);

		});
		
		// --> <"airline,airportName" | delay>
		
		JavaPairRDD<String, Integer> resultA = mappedRDD.reduceByKey((v1,v2) -> v1+v2);
		
		resultA.saveAsTextFile(outputFolder);
		
		/** B. Find overloaded routes, an overloaded route is a pair "dep_airport, arr_airport"
		 * 	with >99% of flights fully booked && >5% of flights cancelled
		 *  **/
		
		JavaPairRDD <String, FullOrCancelled> flights = sc.textFile(inputPath).mapToPair(line -> {
			
			String fields[] = line.split(",");
			
			Integer booked = Integer.parseInt(fields[10]);
			Integer seats = Integer.parseInt(fields[9]);
			Integer cancelled = fields[8].compareTo("yes") == 0 ? 1 : 0;
			Integer full = seats == booked ? 1 : 0;
			
			
			return new Tuple2<String, FullOrCancelled>(fields[5]+","+fields[6],
					new FullOrCancelled(full, cancelled, 1));
			
		}).reduceByKey((t1,t2) -> {
			return new FullOrCancelled(t1.getFull()+t2.getFull(), t1.getCancelled()+t2.getCancelled(), t1.getCount()+t2.getCount());
		});
		// --> <"dep_airport, arr_airport" | [full: 0 or 1, cancelled: 0 or 1, counter: 1]> and then I summed them
		
		JavaPairRDD<String, Percentages> reducedRDD = flights.mapValues(line -> {
			return new Percentages((double)line.getFull()/line.getCount()*100, (double)line.getCancelled()/line.getCount()*100);
		});
		
		JavaPairRDD<String, Percentages> filteredRDD = reducedRDD.filter(line -> {
			if(line._2().getFPerc().compareTo(99.0) >= 0 && line._2().getCPerc().compareTo(5.0)>= 0) {
				return true;
			}else {
				return false;
			}
		});
		
		JavaRDD<String> resultB = filteredRDD.map(line -> line._1());
		
		resultB.saveAsTextFile(outputFolder2);
	
		
		sc.close(); 
		
	}
}
