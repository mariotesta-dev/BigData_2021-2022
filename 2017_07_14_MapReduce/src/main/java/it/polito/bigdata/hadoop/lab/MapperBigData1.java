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

    	//Getting values from file [date, city, country, maxTemp, minTemp]
    	String fields[] = value.toString().split(",");
    	
    	Double maxTemp = Double.parseDouble(fields[3]);
    	Double minTemp = Double.parseDouble(fields[4]);
    	
    	//already filter some lines which doesn't have the temp value requested to minimize data sent to the network
    	if(maxTemp.doubleValue() > 35.0 || minTemp.doubleValue() < -20.0) {
    		context.write(new Text(fields[1]), new Text(maxTemp + "," + minTemp));
    	}
    	
    		
    }
}
