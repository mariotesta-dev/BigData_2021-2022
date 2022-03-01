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
    	//Considering italian cities only
    	
    	String city = fields[3];
    	String country = fields[4];
    	String category = fields[5];
    	String subcategory = fields[6];
    	
    	
    	if(country.compareTo("Italy") == 0 && category.compareTo("tourism") == 0) {
    		//se museo ritorna count:1, countMuseo:1
    		if(subcategory.compareTo("museum") == 0) {
    			context.write(new Text(city), new Text("1,1"));
    		}else {
    			//se non museo ritorna count:1, countMuseo:0
    			context.write(new Text(city), new Text("1,0"));
    		}
    	}
    	
    	
    }
}
