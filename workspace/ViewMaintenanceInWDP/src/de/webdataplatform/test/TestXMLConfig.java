package de.webdataplatform.test;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;

public class TestXMLConfig {

	/**
	 * @param args
	 */
	
	public static void main(String[] args) {

		Log log = new Log("TestClient");

		SystemConfig.load(log);
		NetworkConfig.load(log);
		DatabaseConfig.load(log);
		EvaluationConfig.load(log);
		
//		for (ICreateTable createTable : EvaluationConfig.getCreateViewTables()) {
//			
//			System.out.println(createTable.getBasetable());
//		}
	}

}
