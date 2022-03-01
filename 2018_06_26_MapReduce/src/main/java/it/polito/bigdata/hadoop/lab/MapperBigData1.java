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

    	// |--> fields[]:
    	// |--> 0: date
    	// |--> 1: hour
    	// |--> 2: virtualServer
    	// |--> 3: CPUUsage%
    	// |--> 4: RAMUsage%
    	
    	String fields[] = value.toString().split(",");
    	
    	/* Filtrare data: May_2018 */
    	if(fields[0].compareTo("2018/05/01") >= 0 && fields[0].compareTo("2018/05/31") <= 0){
    		
    		/* Filtrare ora:  9.00 < hour < 17:59 */
    		if(fields[1].compareTo("09.00") >= 0  && fields[1].compareTo("17:59") <= 0) {
    			
    			/* Filtrare CPU Usage value > 99.8 */
    			if(Double.valueOf(fields[3]).compareTo(98.8) > 0) {
    				context.write(new Text(fields[2]), new IntWritable(1)); //<VSID, 1>
    			}
    		}
    	}
    	
    		
    }
}
