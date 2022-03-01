package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                DoubleWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	String firstTimestamp = null;
    	Double highestPrice = 0.0;
    	
    	//iterate over all the elements inside values to glue local top prices together
    	for(Text t : values) {
    		//split price and date
    		
    		String fields[] = t.toString().split(",");
    		String timestamp = fields[1];
    		Double price = Double.parseDouble(fields[0]);
    		
    		if(price > highestPrice) {
    			highestPrice = price;
    			firstTimestamp = timestamp;
    		}else if(price == highestPrice) {
    			if(firstTimestamp == null || timestamp.compareTo(firstTimestamp)<0) {
    				firstTimestamp = timestamp;
    			}
    		}
    		
    	}
    	
    	context.write(new DoubleWritable(highestPrice), new Text(firstTimestamp));
    	
    	
    }
}
