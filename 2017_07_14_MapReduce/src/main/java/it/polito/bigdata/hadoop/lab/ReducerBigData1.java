package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	/* Each line has a city as key and an iterable of [maxtemp, mintemp] ,
    	 * the idea is to check if at least one element of the iterable contains a max_temp > 35
    	 * and if at leasnt one element of the iterable contains a min_temp < -20  
    	 * */
    	
    	Boolean hasMax = false;
    	Boolean hasMin = false;
    	
    	for(Text v : values) {
    		String fields[] = v.toString().split(",");
    		Double maxTemp = Double.parseDouble(fields[0]);
    		Double minTemp = Double.parseDouble(fields[1]);
    		
    		if(maxTemp.doubleValue() > 35.0) {
    			hasMax = true;
    		}
    		
    		if(minTemp.doubleValue() < -20.0) {
    			hasMin = true;
    		}
    	}
    	
    	if(hasMax && hasMin) {
    		context.write(key,NullWritable.get());
    	}
    	

    	
    }
}
