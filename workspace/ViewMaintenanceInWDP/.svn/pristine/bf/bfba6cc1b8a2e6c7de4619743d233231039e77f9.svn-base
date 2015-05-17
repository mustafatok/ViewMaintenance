package de.webdataplatform.test;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class TestConfig {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		
		try
		{
		    XMLConfiguration config = new XMLConfiguration("SystemConfig.xml");
		    
		    
		    String user = config.getString("systemconfig.user");
		    String password = config.getString("systemconfig.password");
		    String zookeeper = config.getString("systemconfig.zookeeper");
		   
		    String hbasemaster = config.getString("systemconfig.hbasemaster");
		    String vmmaster = config.getString("systemconfig.vmmaster");

		    List<String> clients = config.getList("systemconfig.clients.node");
		    List<String> regionServers = config.getList("systemconfig.regionServers.node");
		    List<String> viewManagers = config.getList("systemconfig.viewManagers.node");

		    System.out.println("Running system config:");
		    System.out.println("user:"+user);
		    System.out.println("password:"+password);
		    System.out.println("zookeeper:"+zookeeper);
		    
		    System.out.println("hbasemaster:"+hbasemaster);
		    System.out.println("vmmaster:"+vmmaster);
		    
		    System.out.println("clients:"+clients);
		    System.out.println("regionServers:"+regionServers);
		    System.out.println("viewManagers:"+viewManagers);
		    
		    
		}
		catch(ConfigurationException cex)
		{
			
			cex.printStackTrace();
		}

		try
		{
		    XMLConfiguration config = new XMLConfiguration("EvaluationConfig.xml");
		    
		    

		    List<String> experiments = config.getList("experiments.experiment.distribution");
		    
		    System.out.println(experiments);
		    System.out.println(experiments.size());

		    
		    for (int i = 0; i < experiments.size(); i++) {
			
		    	String restartDatabase = config.getString("experiments.experiment("+i+").restartDatabase");
			    String iterations = config.getString("experiments.experiment("+i+").iterations");
			    String numOfRegionServers = config.getString("experiments.experiment("+i+").numOfRegionServers");
			    String numOfViewManagers = config.getString("experiments.experiment("+i+").numOfViewManagers");
			    String distribution = config.getString("experiments.experiment("+i+").distribution");
			    String viewTypes = config.getString("experiments.experiment("+i+").viewTypes");
			    String numOfViews = config.getString("experiments.experiment("+i+").numOfViews");
			    String numOfOperations = config.getString("experiments.experiment("+i+").numOfOperations");
			    String numOfRecords = config.getString("experiments.experiment("+i+").numOfRecords");
			    String numOfAggregationKeys = config.getString("experiments.experiment("+i+").numOfAggregationKeys");
			    String numOfBaseTableRegions = config.getString("experiments.experiment("+i+").numOfBaseTableRegions");
			    String numOfViewTableRegions = config.getString("experiments.experiment("+i+").numOfViewTableRegions");
			    String useDeletes = config.getString("experiments.experiment("+i+").useDeletes");
			    String useUpdates = config.getString("experiments.experiment("+i+").useUpdates");
			    String readDelay = config.getString("experiments.experiment("+i+").readDelay");
			    
			    System.out.println("Running Experiment "+i+" with params");
			    System.out.println("restartDatabase:"+restartDatabase);
			    System.out.println("iterations:"+iterations);
			    System.out.println("numOfRegionServers:"+numOfRegionServers);
			    System.out.println("numOfViewManagers:"+numOfViewManagers);
			    System.out.println("distribution:"+distribution);
			    System.out.println("viewTypes:"+viewTypes);
			    System.out.println("numOfViews:"+numOfViews);
			    System.out.println("numOfOperations:"+numOfOperations);
			    System.out.println("numOfRecords:"+numOfRecords);
			    System.out.println("numOfAggregationKeys:"+numOfAggregationKeys);
			    System.out.println("numOfBaseTableRegions:"+numOfBaseTableRegions);
			    System.out.println("numOfViewTableRegions:"+numOfViewTableRegions);
			    System.out.println("useDeletes: "+useDeletes);
			    System.out.println("useUpdates:"+useUpdates);
			    System.out.println("readDelay:"+readDelay);
			    
			    System.out.println("-------------------------------------------");
			}
		    
		    
		    
		}
		catch(ConfigurationException cex)
		{
			
			cex.printStackTrace();
		}		
		
		

	}

}
