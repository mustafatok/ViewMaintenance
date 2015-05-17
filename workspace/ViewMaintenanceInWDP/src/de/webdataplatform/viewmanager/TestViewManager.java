package de.webdataplatform.viewmanager;

import de.webdataplatform.log.Log;
import de.webdataplatform.regionserver.TestRegionServer;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;

public class TestViewManager {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args == null || args.length == 0){
			
			Log log = new Log("exception-vm.log");
			log.infoToFile(TestRegionServer.class, "Arguments: name ip updatePort messagePort zookeeper");
			System.exit(0);
		}
		
		ViewManager viewManager=null;
//		try {
//			viewManager = new ViewManager(Constants.VIEWMANAGER_NAME, Constants.VIEWMANAGER_ADDRESS, Constants.VIEWMANAGER_UPDATEPORT, Constants.VIEWMANAGER_MESSAGEPORT, Constants.ZOOKEEPER_ADDRESS+":2181");
//		} catch (NumberFormatException | UnknownHostException e1) {
//
//			e1.printStackTrace();
//		}
		
		Log log = new Log(args[0]+".log");

		SystemConfig.load(log);
		NetworkConfig.load(log);
		
			
		try {
			viewManager = new ViewManager(log, args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
			viewManager.initialize();
			
		} catch (Exception e) {

			log.error(TestViewManager.class, e);
		}
		
//		viewManager = new ViewManager("viewmanager2", "localhost", 5144, 5150, "192.168.26.133");
//		
//		try {
//			viewManager.initialize();
//			
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}		


	}

}
