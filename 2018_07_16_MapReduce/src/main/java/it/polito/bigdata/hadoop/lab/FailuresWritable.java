package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FailuresWritable implements Writable{
	
	public Integer HDFailure;
	public Integer RAMFailure;

	@Override
	public void readFields(DataInput in) throws IOException {
		
		HDFailure = in.readInt();
		RAMFailure = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(HDFailure);
		out.writeInt(RAMFailure);
		
	}
	
	public FailuresWritable(Integer hd, Integer ram) {
		this.HDFailure = hd;
		this.RAMFailure = ram;
	}

	

}
