package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	//Getting values from file
    	// [flight_no, airline, dep_date, dep_time, arr_time, dep_airport, arr_airport, delay, cancelled, numSeats, numBooked]
    	String fields[] = value.toString().split(",");
    	
    	
    	/* We need to compute airports with num_cancelled_flights/num_tot_flights > 1%
    	 * So we can map dep_airport, cancelled (filtered by date in the range given)
    	 * */
    	if(fields[2].compareTo("2016/09/01") >= 0 && fields[2].compareTo("2017/08/31") <= 0) {
    		context.write(new Text(fields[5]), new IntWritable(fields[8].compareTo("yes") == 0 ? 1 : 0));
    	}
    	
    	//output: <dep_airport, cancelled> for each line having  2016/09/01 <= dep_date <= 2017/08/31
    	
    		
    }
}
