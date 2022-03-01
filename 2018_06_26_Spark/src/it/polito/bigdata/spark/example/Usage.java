package it.polito.bigdata.spark.example;

public class Usage {
	
	public Double cpu;
	public Double ram;
	public Integer count; //helpful when counting numbersOfLines (by Key)
	
	public Usage(Double cpu, Double ram) {
		this.cpu = cpu;
		this.ram = ram;
		count = 1;
	}


}
