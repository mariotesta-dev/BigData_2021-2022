package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                    FailuresWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	//Failures.txt
    	// |--> date
    	// |--> time
    	// |--> sID
    	// |--> typeOfFailure
    	// |--> mins
    	
    	//Get data from failures.txt
    	String fields[] = value.toString().split(",");
    	
    	String date = fields[0];
    	String sID = fields[2];
    	String typeOfFailure = fields[3];
    	
    	//filtro per data aprile 2016
    	if(date.compareTo("2016/04/01") >= 0 && date.compareTo("2016/04/30") <= 0) {
    		
    		if(typeOfFailure.compareTo("hard_drive") == 0) {
    			//emit sID, [hd: 1, ram: 0]
    			context.write(new Text(sID), new FailuresWritable(1,0));
    			
    		}else if(typeOfFailure.compareTo("RAM") == 0) {
    			//emit sID, [hd: 0, ram: 1]
    			context.write(new Text(sID), new FailuresWritable(0,1));
    			
    		}
    	}
    	
    	
    		
    }
}
