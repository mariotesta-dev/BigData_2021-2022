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
    	
    	Integer numOf2017Patches = 0;
    	Integer numOf2018Patches = 0;
    	
    	for(Text t : values) {
    		numOf2017Patches += Integer.parseInt(t.toString().split(",")[0]);
    		numOf2018Patches += Integer.parseInt(t.toString().split(",")[1]);
    	}
    	
    	if(numOf2018Patches > numOf2017Patches) {
    		context.write(new Text(key), NullWritable.get());
    	}

    	
    }
}
