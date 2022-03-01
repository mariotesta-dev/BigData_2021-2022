package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
   	// |--> <city, [manufacturer1, manufacturer2, ecc...]>
    	
    	String m = null;
    	Boolean allTheSame = true;
    	
    	for(Text t : values) {
    		if(m == null) {
    			m = t.toString();
    		}else if(m.compareTo(t.toString()) != 0) {
    			allTheSame = false;
    			break;
    		}
    	}
    	
    	if(allTheSame) {
    		context.write(key, new Text(m)); //emit <city, manufacturer>
    	}
    	
    }
}
