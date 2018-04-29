package bigdata.mapreduce;

import bigdata.mapreduce.femicidios.*;

public class Main {
	public static void main(String args[]) {
	
		if (args.length != 2) {
			System.out.println("There must be exactly two arguments. First argument is for the input path and the second one is for the output path.");
			return;
		}
		
		System.setProperty("hadoop.home.dir", "/");
		
		try {
			Driver driver = new Driver(args[0], args[1]);
			driver.resolve();
		}
		catch(Exception e) { }
	}
}
