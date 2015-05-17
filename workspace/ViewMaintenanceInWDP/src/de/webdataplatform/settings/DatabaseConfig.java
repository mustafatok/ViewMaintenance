package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import de.webdataplatform.log.Log;

public class DatabaseConfig {


	
	
	public static List<TableDefinition> TABLEDEFINITIONS;
	
	public static TableDefinition getTableDefinition(String name){
		
		for (TableDefinition tableDefinition : TABLEDEFINITIONS) {
			
			
			if(tableDefinition.getName().equals(name))return tableDefinition;
			
		}
		return null;
	}
	
	public static void load(Log log) {
	
		
		
		
		try
		{
			
			log.info(DatabaseConfig.class, "Loading database config:");
			
			XMLConfiguration config = new XMLConfiguration();
			config.setDelimiterParsingDisabled(true);
			config.load("VMDatabaseConfig.xml");
   

		    DatabaseConfig.TABLEDEFINITIONS = new ArrayList<TableDefinition>();
		    List<String> tabledefs = config.getList("dbSchema.tableDefinition.name");

		    
		    for (int i = 0; i < tabledefs.size(); i++) {
			

		   
		    	
			    String name = config.getString("dbSchema.tableDefinition("+i+").name");
			    
			    
			    
			    
			    String keyPrefix = config.getString("dbSchema.tableDefinition("+i+").primaryKey.prefix");
			    String keyStartRange = config.getString("dbSchema.tableDefinition("+i+").primaryKey.startRange");
			    String keyEndRange = config.getString("dbSchema.tableDefinition("+i+").primaryKey.endRange");
			    
			    KeyDefinition keyDef = new KeyDefinition(keyPrefix, Long.parseLong(keyStartRange), Long.parseLong(keyEndRange));
			    
			    			    
			    List<String> coldefs = config.getList("dbSchema.tableDefinition("+i+").column.name");
			    			    
			    List<ColumnDefinition> colDefs = new ArrayList<ColumnDefinition>();
			    
			    for (int x = 0; x < coldefs.size(); x++) {
			    	
				    String colName = config.getString("dbSchema.tableDefinition("+i+").column("+x+").name");
				    String colFamily = config.getString("dbSchema.tableDefinition("+i+").column("+x+").family");
				    String colPrefix = config.getString("dbSchema.tableDefinition("+i+").column("+x+").prefix");
				    String colStartRange = config.getString("dbSchema.tableDefinition("+i+").column("+x+").startRange");
				    String colEndRange = config.getString("dbSchema.tableDefinition("+i+").column("+x+").endRange");
			    
				    
				    ColumnDefinition colDef = new ColumnDefinition(colName, colFamily, colPrefix, Long.parseLong(colStartRange), Long.parseLong(colEndRange));
				    
				    colDefs.add(colDef);
		    
				    
			    }
			    
			    TableDefinition tableDefinition = new TableDefinition(name, keyDef, colDefs);
			    
			    log.info(DatabaseConfig.class, tableDefinition.toString());
			    
			    DatabaseConfig.TABLEDEFINITIONS.add(tableDefinition);
			    
			    

			}

		    
//		    log.info(DatabaseConfig.class, numOfExperiments+" experiments found");
		    
		    
		}
		catch(ConfigurationException cex)
		{
			
			log.error(DatabaseConfig.class, cex);
			cex.printStackTrace();
			System.exit(0);
		} 
		

		log.info(DatabaseConfig.class, "--------------------------------------------------------------");
  



	}
	
	public static void main(String[] args){
//		 load();
	}
	

}
