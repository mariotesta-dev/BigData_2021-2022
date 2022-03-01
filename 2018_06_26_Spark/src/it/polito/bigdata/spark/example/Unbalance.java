package it.polito.bigdata.spark.example;

public class Unbalance {
	
	public Integer unbalancedCPU;
	public Integer unbalancedRAM;
	
	public Unbalance(Integer cpu, Integer ram) {
		this.unbalancedCPU = cpu;
		this.unbalancedRAM = ram;
	}

}
