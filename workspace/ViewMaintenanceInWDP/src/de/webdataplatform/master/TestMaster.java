package de.webdataplatform.master;

import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;

public class TestMaster {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
//		try {
//			System.out.println(InetAddress.getLocalHost().getHostAddress());
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		

		
		Master master=null;
		
		master = new Master(args[0], args[1], Integer.parseInt(args[2]));
		
		master.initialize();
		
		

	}

}
