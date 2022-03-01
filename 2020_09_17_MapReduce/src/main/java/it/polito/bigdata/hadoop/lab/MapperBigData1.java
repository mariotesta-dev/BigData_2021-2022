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
                    Text> {// Output value type
	
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	//Username, MID, StartTimestamp, EndTimestamp
    	String fields[] = value.toString().split(",");
    	String fieldContainingYear = fields[2];
    	String movie = fields[1];
    	String user = fields[0];
    	
    	//for each movie watched in 2019, emit movieID,userID
    	if(fieldContainingYear.startsWith("2019")) {
    		context.write(new Text(movie), new Text(user));
    	}
  
    	
    }
}
