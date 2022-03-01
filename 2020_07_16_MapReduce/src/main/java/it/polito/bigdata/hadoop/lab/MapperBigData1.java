package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
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
                    IntWritable> {// Output value type
	
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	//SID, PID, Date
    	String fields[] = value.toString().split(",");
    	
    	String SID = fields[0];
    	Integer year = Integer.parseInt(fields[2].split("/")[0]);
    	
    	//Filter year 2017 and 2018
    	if(year == 2017 || year == 2018) {
    		//emit key: SID - value: 2017 or 2018
    		context.write(new Text(SID), new IntWritable(year));
    	}
  
    	
    }
}
