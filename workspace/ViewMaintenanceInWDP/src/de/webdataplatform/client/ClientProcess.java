package de.webdataplatform.client;

import java.util.Arrays;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;



public class ClientProcess {

	/**
	 * @param args
	 */
	public static void main(String[] args) {


		
		
		if(args == null || args.length == 0){
			
			Log log = new Log("exception-client.log");
			log.infoToFile(ClientProcess.class, "Possible Comands: createtable createsplittable");
			System.exit(0);
		}
		
//		if(args[0].equals("createandfillbasetables") && args.length != 9){
//			
//			System.out.println("Comand createbasetables arguments: zookeeper, viewTableTypes, recordCount, regionCount, numOfAggKeys, numOfAggValues, useDeletes, useUpdates");
//			System.exit(0);
//		}
		
		if(args[1].equals("createtable") && args.length != 3){
			
			Log log = new Log("exception-client.log");
			log.infoToFile(ClientProcess.class, "Comand createtable arguments: tablename");
			System.exit(0);
		}
		
		if(args[1].equals("createsplittable") && args.length != 4){
			
			Log log = new Log("exception-client.log");
			log.infoToFile(ClientProcess.class, "Comand createsplittable arguments: tablename, regionCount");
			System.exit(0);
		}
		
		if(args[1].equals("queuefinishmarkers") && args.length != 4){
			
			Log log = new Log("exception-client.log");
			log.infoToFile(ClientProcess.class, "Comand queuefinishmarkers arguments: tablename, regionCount");
			System.exit(0);
		}
		
		if(args[1].equals("filltable") && args.length != 5){
			
			Log log = new Log("exception-client.log");
			log.infoToFile(ClientProcess.class, "Comand createsplittable arguments: tablename, distribution, numOfOperations, ");
			System.exit(0);
		}

		
		
			
			
			


			
			if(args[1].equals("createtable")){

				Log log = new Log("createtable-"+args[2]+".log");
				loadConfig(log);
				log.info(ClientProcess.class, "called with arguments: "+Arrays.toString(args));
				log.info(ClientProcess.class, "executing createtable");
				
				Client client = new Client(args[0], log);
				try{
	
					client.deleteTable(args[2]);
				}catch(Exception e){
					log.error(ClientProcess.class, e);
				}
				try{
					
					client.createTable(args[2]);
				}catch(Exception e){
					log.error(ClientProcess.class, e);
				}
				
				System.exit(0);
			}
			
			
			
			
			if(args[1].equals("createsplittable")){
				
				Log log = new Log("createsplittable-"+args[2]+".log");
				loadConfig(log);
				log.info(ClientProcess.class, "called with arguments: "+Arrays.toString(args));
				log.info(ClientProcess.class, "executing createsplittable");
				
				Client client = new Client(args[0], log);
				try{
					client.deleteTable(args[2]);
				}catch(Exception e){
					log.error(ClientProcess.class, e);
				}
				
				try{
					client.createRangeSplitTable(args[2], Integer.parseInt(args[3]));
				}catch(Exception e){
					log.error(ClientProcess.class, e);
				}
				
				System.exit(0);
			}
			
			if(args[1].equals("queuefinishmarkers")){
				
				Log log = new Log("queuefinishmarkers.log");
				loadConfig(log);
				log.info(ClientProcess.class, "called with arguments: "+Arrays.toString(args));
				log.info(ClientProcess.class, "executing queuefinishmarkers");
				
				Client client = new Client(args[0], log);
				
				try{
					client.queueFinishMarkers(args[2], Integer.parseInt(args[3]));
				}catch(Exception e){
					log.error(ClientProcess.class, e);
				}
				
				System.exit(0);
			}			
			
			
			if(args[1].equals("filltable")){
				
				Log log = new Log(args[0]+"-filltable-"+args[2]+".log");
				loadConfig(log);
				log.info(ClientProcess.class, "called with arguments: "+Arrays.toString(args));
				log.info(ClientProcess.class, "executing filltable");
				
				Client client = new Client(args[0], log);

				try{
					client.fillBaseTable(args[2], args[3], Integer.parseInt(args[4]));
				}catch(Exception e){
					log.error(ClientProcess.class, e);
				}
				
				System.exit(0);
			}
		

		System.out.println("Comand not found");
		System.exit(0);
	}	
	
	public static void loadConfig(Log log){
		SystemConfig.load(log);
		DatabaseConfig.load(log);
		NetworkConfig.load(log);
	}

}
