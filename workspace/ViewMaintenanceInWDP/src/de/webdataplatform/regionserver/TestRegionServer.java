package de.webdataplatform.regionserver;

import java.util.Map;

import de.webdataplatform.client.ClientProcess;
import de.webdataplatform.log.Log;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;

public class TestRegionServer {

	private static Map<String, Integer> aggregationSumMapTestSetup;
	/**
	 * @param args
	 */
	public static void main(String[] args) {



		if(args == null || args.length == 0){
			
			Log log = new Log("exception-rs.log");
			log.infoToFile(TestRegionServer.class, "Arguments: name ip messagePort zookeeper messageDelay");
			System.exit(0);
		}
		
		Log log = new Log(args[0]+".log");
		SystemConfig.load(log);
		NetworkConfig.load(log);
		EvaluationConfig.load(log);

		try{
			
			RegionServer regionServer = new RegionServer(log, args[0], args[1], Integer.parseInt(args[2]));
	        regionServer.initialize();
	        
		}catch(Exception e){
			log.error(TestRegionServer.class, e);
		}
        
//        RegionServer regionServer = new RegionServer(args[0], "localhost", Integer.parseInt(args[1]), "deltaserver1");
//        
//        regionServer.initialize();
		
		
		
	}


}
