package de.webdataplatform.settings;

import de.webdataplatform.log.Log;

public class TestSettings {


	
	
	
	public static void main(String[] args) {

		
		Log log = new Log("newlog");
		
		SystemConfig.load(log);
		System.out.println();
		NetworkConfig.load(log);
		System.out.println();
		DatabaseConfig.load(log);
		System.out.println();		
		EvaluationConfig.load(log);
		
	}

}
