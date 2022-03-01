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
    	
    	Integer count = 0;
    	Integer numOfMuseums = 0;
    	
    	for(Text t : values) {
    		String fields[] = t.toString().split(",");
    		Integer c = Integer.parseInt(fields[0]);
    		Integer museum = Integer.parseInt(fields[1]);
    		
    		count += c;
    		numOfMuseums += museum;
    	}
    	
    	if(count > 1000 && numOfMuseums >= 20) {
    		context.write(new Text(key),NullWritable.get());
    	}
    	
    	
    	
    }
}
