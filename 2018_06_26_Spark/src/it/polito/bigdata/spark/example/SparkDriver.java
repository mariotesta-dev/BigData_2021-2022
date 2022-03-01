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
		Double CPUthr;
		Double RAMthr;

		inputPath = args[0];
		outputFolder = args[1];
		outputFolder2 = args[2];
		CPUthr = Double.parseDouble(args[3]);
		RAMthr = Double.parseDouble(args[4]);
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/** A **/
		
		//Read file into inputRDD
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		
		//Filter Date and Hour
		JavaRDD<String> filteredDateRDD = inputRDD.filter(line -> {
			String fields[] = line.split(",");
			
			String date = fields[0];
			
			if(date.compareTo("2018/05/01") >= 0 && date.compareTo("2018/05/31")<=0) {
					return true;
			}
			
			return false;
			
		}).cache();
		
		
		//Map <"VSID, hour" | [CPUUsage, RAMUsage, +1]>
		JavaPairRDD<String, Usage> pairedRDD = filteredDateRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			
			String hour = fields[1].split(".")[0];
			String VSID = fields[2];
			Double CPUUsage = Double.parseDouble(fields[3]);
			Double RAMUsage = Double.parseDouble(fields[4]);
			
			return new Tuple2<String, Usage>(VSID + "," + hour, new Usage(CPUUsage, RAMUsage));
			
		}); 
		
		//Reduce by Key and compute sum of usages and number of lines for each key
		JavaPairRDD<String, Usage> reducedRDD = pairedRDD.reduceByKey((u1, u2) -> {
			
			Usage u = new Usage(u1.cpu + u2.cpu, u1.ram+u2.ram);
			u.count = u1.count + u2.count;
			
			return u;
		});
		
		
		JavaRDD<String> resultA = reducedRDD.filter(line -> {
			
			if(line._2().cpu/line._2().count > CPUthr && line._2().ram/line._2().count > RAMthr) {
				return true;
			}else {
				return false;
			}
		}).map(line -> line._1()); //or .keys() can be used to retrieve just keys from RDD
		
		resultA.saveAsTextFile(outputFolder);
		
		/** B. **/
		
		//Starting from a RDD filtered for May 2018 [date, hour, vsid, cpuUsage, ramUsage]
		/* Basic idea is to map like this <"vsid, date, hour(only)" | cpuUsage, ramUsage> */
		 
		JavaPairRDD<String, Usage> inputBRDD = filteredDateRDD.mapToPair(line -> {
			String fields[] = line.split(",");
			
			String date = fields[0];
			String hour = fields[1];
			String VSID = fields[2];
			Double CPUUsage = Double.parseDouble(fields[3]);
			Double RAMUsage = Double.parseDouble(fields[4]);
			
			return new Tuple2<String, Usage>(VSID+","+date+","+hour.split("/")[0], new Usage(CPUUsage, RAMUsage));
		});
	
		
		JavaPairRDD<String, Usage> maxUsagePerHourCPU = inputBRDD.reduceByKey((u1, u2) -> {
			
			if(u1.cpu > u2.cpu) return u1;
			else return u2;
		
		});
		
		JavaPairRDD<String, Usage> maxUsagePerHourCPUorRAM = maxUsagePerHourCPU.filter(line -> {
			if(line._2().cpu > 90.0 || line._2().ram < 10.0) {
				return true;
			}
			return false;
		});
		
		
		/* Then map to: <"vsid, date" | [unbalancedCPU, unbalancedRAM]> */
		JavaPairRDD<String, Unbalance> mappedMaxUsagePerHour = maxUsagePerHourCPUorRAM.mapToPair(line -> {
			
			String fields[] = line._1().split(",");
			String VSID = fields[0];
			String date = fields[1];
			
			Integer isUnbalancedRAM = line._2().ram < 10.0 ? 1 : 0;
			Integer isUnbalancedCPU = line._2().cpu > 90.0 ? 1 : 0;
			
			return new Tuple2<String, Unbalance>(VSID+","+date, new Unbalance(isUnbalancedCPU, isUnbalancedRAM));
		});
		
		//sum values of unbalanced cpu and unbalanced ram for each date
		JavaPairRDD<String, Unbalance> numberOfDaysRDD = mappedMaxUsagePerHour.reduceByKey((u1,u2) -> {
			Integer numHoursCPU = u1.unbalancedCPU + u2.unbalancedCPU;
			Integer numHoursRAM = u1.unbalancedRAM + u2.unbalancedRAM;
			
			Unbalance u = new Unbalance(numHoursCPU, numHoursRAM);
			
			return u;
			
		});
		
		//filter 8h |--> example:  <"VSID,date" | [unbalancedCPU: 9, unbalancedRAM: 8]>
		JavaPairRDD<String, Unbalance> resultB = numberOfDaysRDD.filter(line -> {
			if(line._2().unbalancedCPU >= 8 && line._2().unbalancedRAM >= 8) {
				return true;
			}else {
				return false;
			}
		});
		
		
		resultB.keys().saveAsTextFile(outputFolder2);		
		
		
		sc.close(); 
		
	}
}
