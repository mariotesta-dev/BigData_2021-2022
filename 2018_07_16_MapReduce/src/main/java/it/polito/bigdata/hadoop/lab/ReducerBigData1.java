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
                FailuresWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FailuresWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	Integer HDFailures = 0;
    	Integer RAMFailures = 0;
    	
    	for(FailuresWritable f : values) {
    		HDFailures += f.HDFailure;
    		RAMFailures += f.RAMFailure;
    	}
    	
    	if(HDFailures >= 1 && RAMFailures >= 1) {
    		context.write(key, NullWritable.get());
    	}
    	
    	
    }
}
