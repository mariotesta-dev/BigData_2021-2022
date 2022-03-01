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
    	
    	String fields[] = value.toString().split(",");
    	
    	String year = fields[1].split("/")[0];
    	String app = fields[2];
    	
    	//if patch is from 2017 write 1,0, if it's from 2018 write 0,1
    	if(year.compareTo("2017") == 0) {
    		context.write(new Text(app), new Text("1,0"));
    	}else if(year.compareTo("2018") == 0) {
    		context.write(new Text(app), new Text("0,1"));
    	}
    	
  
    	
    }
}
