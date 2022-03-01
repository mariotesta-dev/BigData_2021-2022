package it.polito.bigdata.spark.example;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class SparkDriver {

	public static void main(String[] args) {

		@SuppressWarnings("unused")
		String inputPath;
		String inputPath2;
		String inputPath3;
		String outputFolder;
		String outputFolder2;

		inputPath = "Apps.txt";
		inputPath2 = "Users.txt";
		inputPath3 = "Actions.txt";
		outputFolder = "outPart1/";
		outputFolder2 = "outPart2/";
		

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		/** PART 1 **/
		
		//select apps that in 2021, in each 12 months they had more installations than removals
		//RDD from actions.txt
		JavaRDD<String> actionsRDD = sc.textFile(inputPath3).cache();
		
		//filter year 2021 and only install/remove actions
		JavaRDD<String> installsRemoves2021RDD = actionsRDD.filter(line -> {
			
			String fields[] = line.split(",");
			Integer year = Integer.parseInt(fields[2].substring(0,4));
			String action = fields[3];
			
			return year.equals(2021) && (action.equals("Install") || action.equals("Remove"));
			
		});
		
		//map key: {app,month} - value:{1 if install, -1 if remove}
		//we get something like: <"app_month", + or - something>
		//if value > 0 --> we had more installations than removals (we can filter)
		JavaPairRDD<String, Integer> counterInstRemRDD = installsRemoves2021RDD.mapToPair(line -> {
			
			String fields[] = line.split(",");
			String month = fields[2].substring(5,7);
			String app = fields[1];
			Integer action = fields[3].equals("Install") ? 1 : -1;
			
			return new Tuple2<String,Integer>(app + "_" + month, action);
			
		}).reduceByKey((v1,v2) -> v1+v2).filter(line -> line._2() > 0);
		
		//map as key: {app} - value: {1} ... to count numOfMonths left
		//we then reduce by key
		//and filter only lines (so apps) having numOfMonths == 12, this means 12 months with more "install" than "remove"
		JavaPairRDD<String,Integer> numOfMonthsLeftPerApp = counterInstRemRDD.mapToPair(line -> {
			
			String app = line._1().split(",")[0];
			
			return new Tuple2<String, Integer>(app, 1);
			
		}).reduceByKey((v1,v2) -> v1+v2).filter(line -> line._2() == 12);
		
		//get Apps.txt as key: {app} - value: {appName}
		JavaPairRDD<String, String> appsRDD = sc.textFile(inputPath2).mapToPair(line -> {
			
			String fields[] = line.split(",");
			String app = fields[0];
			String appName = fields[1];
			
			return new Tuple2<String,String>(app, appName);
			
		});
		
		//join in order to get appName for each appId -> <appId, [Integer not needed, appName]>
		JavaPairRDD<String, Tuple2<Integer,String>> joinPartARDD = numOfMonthsLeftPerApp.join(appsRDD);
		
		//map as key: {appId} - value: {appName}
		JavaPairRDD<String,String> resultA = joinPartARDD.mapToPair(line -> {
			return new Tuple2<String,String>(line._1(), line._2()._2());
		});
		
		resultA.saveAsTextFile(outputFolder);
		
		/** Part B **/
		
		//filter installs only
		//map as key:{app,user} - value:{[install beforeDate 0 or 1, install afterDate 0 or 1]}
		//sum values of before and after
		//filter lines, so users, that has beforeDate Installations == 0 && afterDate Installations > 0
		JavaPairRDD<String, Counter> actionsPartBRDD = actionsRDD.filter(line -> {
			String action = line.split(",")[3];
			
			return action.equals("Install");
		}).mapToPair(line -> {
			
			String fields[] = line.split(",");
			String user = fields[0];
			String app = fields[1];
			String date = fields[2].substring(0,10);
			
			if(date.compareTo("2021/12/31") <= 0) {
				return new Tuple2<>(app + "_" + user, new Counter(1,0));
			}else {
				return new Tuple2<>(app + "_" + user, new Counter(0,1));
			}
			
		}).reduceByKey((c1,c2) -> {
			Integer beforeDate = c1.getBeforeDate() + c2.getBeforeDate();
			Integer afterDate = c1.getAfterDate() + c2.getAfterDate();
			
			return new Counter(beforeDate, afterDate);
			
		}).filter(line -> {
			
			return line._2().getBeforeDate() == 0 && line._2().getAfterDate() > 0;
			
		});
		
		//now for each app I must find the max value of distinct users, so first I map as <app, +1> to then count number of distinct users
		JavaPairRDD<String, Integer> countDistUsersRDD = actionsPartBRDD.mapToPair(line -> {
			
			String app = line._1().split("_")[0];
			
			return new Tuple2<>(app, 1);
			
		}).reduceByKey((v1,v2) -> v1+v2).cache();
		
		//now find MAX
		Integer max = countDistUsersRDD.values().reduce((v1,v2) -> v1 > v2 ? v1 : v2);
		
		//filter lines in countDistUsersRDD having value == max
		JavaPairRDD<String, Integer> resultB = countDistUsersRDD.filter(line -> line._2() == max);
		
		resultB.keys().saveAsTextFile(outputFolder2);
		
		sc.close(); 
		
	}
}
