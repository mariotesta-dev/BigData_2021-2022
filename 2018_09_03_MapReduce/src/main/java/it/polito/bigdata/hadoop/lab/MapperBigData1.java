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
                    NullWritable,         // Output key type
                    Text> {// Output value type
	
	Double higherPrice = 0.0;
	String firstTimestamp = null;
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	String fields[] = value.toString().split(",");
    	
    	String stock = fields[0];
    	String date = fields[1];
    	String time = fields[2];
    	String timestamp = date + "," + time;
    	String year = date.split("/")[0];
    	Double price = Double.parseDouble(fields[3]);
    	
    	//Filter stock and year + create local higherPrice and update timestamp with the earliest one
    	if(stock.compareTo("GOOG") == 0 && year.compareTo("2017") == 0) {
    		
    		if(price > higherPrice) {
    			
    			higherPrice = price;		//update local higherPrice
    			firstTimestamp = timestamp;	//update local first timeStamp
    			
    		}else if(price == higherPrice) {
    			
    			if(timestamp == null || timestamp.compareTo(firstTimestamp) < 0) {    				
    				firstTimestamp = timestamp;
    			}
    		}
    	}
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException{
    	//emit local top price and date
    	context.write(NullWritable.get(), new Text(higherPrice + "," + firstTimestamp));
    }
}
