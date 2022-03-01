package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
                    NullWritable> {// Output value type
	
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	//reading from Failures.txt
    	String fields[] = value.toString().split(",");
    	
    	String robot = fields[0];
    	String fCode = fields[1];
    	
    	if(fCode.compareTo("FCode122")==0) {
    		context.write(new Text(robot), NullWritable.get());
    	}
  
    	
    }
}
