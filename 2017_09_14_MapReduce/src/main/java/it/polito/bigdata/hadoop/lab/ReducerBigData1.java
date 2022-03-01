package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	/* For each line we have <dep_airport, cancelled[...]>
    	 * Basic idea is to sum all cancelled values while counting the number of flights
    	 * and at the end emit <dep_airport \t %cancelledflights , Null>
    	 * */
    	
    	Integer numCancelledFlights = 0;
    	Integer totNumOfFlights = 0;
    	Double percentage = 0.0;
    	
    	for(IntWritable t : values) {
    		numCancelledFlights += t.get();
    		totNumOfFlights++;
    	}
    	
    	percentage = (double) numCancelledFlights/totNumOfFlights * 100;
    	
    	if(percentage.compareTo(1.0) > 0) {
    		context.write(new Text(key), new DoubleWritable(percentage));
    	}

    	
    }
}
