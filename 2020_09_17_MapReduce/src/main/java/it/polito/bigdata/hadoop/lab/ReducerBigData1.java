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
    	
    	String user = null;
    	Boolean allTheSame = true;
    	
    	for(Text u : values) {
    		if(user == null) {
    			user = u.toString();
    		}else if(user.compareTo(u.toString()) != 0) {
    			allTheSame = false;
    			break;
    		}
    	}
    	
    	if(allTheSame) {
    		context.write(new Text(key), NullWritable.get());
    	}
    	
    }
}
