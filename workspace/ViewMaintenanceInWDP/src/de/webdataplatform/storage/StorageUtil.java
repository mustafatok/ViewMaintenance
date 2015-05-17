package de.webdataplatform.storage;

import java.util.Map;

import de.webdataplatform.view.IViewRecord;

public class StorageUtil {
	
	
	public static void printStorage(Map<String, IBaseRecord> baseTable){
		
		System.out.println("Printing out key value store");
		System.out.println();
		System.out.println("-------------------------");
		for(String key : baseTable.keySet()){
			
			System.out.println("key="+key+", value="+baseTable.get(key));
			
			
		}
		System.out.println("-------------------------");
		System.out.println();
		
	}
	
	
	public static void printView(Map<String, IViewRecord> viewTable){
		
		System.out.println("Printing out View Table");
		System.out.println();
		System.out.println("-------------------------");
		for(String key : viewTable.keySet()){
			
			System.out.println("key="+key+", value="+viewTable.get(key));
			
			
		}
		System.out.println("-------------------------");
		System.out.println();
		
		
	}
}
