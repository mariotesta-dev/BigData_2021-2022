package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	/* Must get highest and lowest prices to then calculate absolute difference and %price variation */
    	
    	Double max = 0.0;
    	Double min = 0.0;
    	
    	for(DoubleWritable price : values) {
    		if(price.get() > max) {
    			max = price.get();
    		}
    		
    		if(price.get() < min) {
    			min = price.get();
    		}  
    	}
    	
    	Double absoluteDifference = Math.abs(max-min);
    	Double monthlyPercentageVariation = (max-min)/min*100;
    	
    	if(monthlyPercentageVariation > 5) {
    		context.write(key,new Text(absoluteDifference+","+monthlyPercentageVariation));
    	}
    	
    }
}
