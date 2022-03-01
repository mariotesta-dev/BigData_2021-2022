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
    	
    	//Purchases: CustomerId, BID, date, price
    	
    	String fields[] = value.toString().split(",");
    	
    	String CustomerId = fields[0];
    	String BID = fields[1];
    	Integer year = Integer.parseInt(fields[2].split("/")[0]);
    	
    	//emit key: {customerId, BID} - value: 1
    	
    	//filter year == 2018
    	if(year == 2018) {
    		context.write(new Text(CustomerId + "\\t" +BID), new IntWritable(1));
    	}
    	
    }
}
