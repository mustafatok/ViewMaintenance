package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import de.webdataplatform.client.Client;
import de.webdataplatform.log.Log;
import de.webdataplatform.ssh.SSHConnection;

public class NetworkConfig {


	
	
	
	

	public static String ZOOKEEPER_QUORUM = "172.17.0.63";
	public static int ZOOKEEPER_CLIENTPORT;
	
	public static String HDFS="hdfs://deltaserver1";


	public static Node HBASE_MASTER;
	public static Node VM_MASTER;
	public static List<Node> CLIENTS;
	public static List<Node> REGIONSERVERS;
	public static List<Node> VIEWMANAGERS;
	
	
	
	
	public static void load(Log log) {
		
		
		try
		{
			
			log.info(NetworkConfig.class, "Loading network config:");
		    
			XMLConfiguration config = new XMLConfiguration();
			config.setDelimiterParsingDisabled(true);
			config.load("VMNetworkConfig.xml");
			
		    

//		    String zookeeper = config.getString("networkconfig.zookeeper");

		    NetworkConfig.ZOOKEEPER_QUORUM = config.getString("networkconfig.zookeeper.quorum");
		    
		    String clientPort = config.getString("networkconfig.zookeeper.clientPort");
		    int clientPortTemp = 0;
		    if(clientPort != null)clientPortTemp = Integer.parseInt(clientPort);
		    NetworkConfig.ZOOKEEPER_CLIENTPORT = clientPortTemp;
		    		
		    NetworkConfig.HDFS = config.getString("networkconfig.hdfs");
		    
		    String master = config.getString("networkconfig.master.address");
		    String masterPort = config.getString("networkconfig.master.hbasePort");
		    String vmMasterPort = config.getString("networkconfig.master.vmPort");


		    List<String> clients = config.getList("networkconfig.client.nodes.node");
		    List<String> regionServers = config.getList("networkconfig.regionServer.nodes.node");
		    String regionServerMessagePort = config.getString("networkconfig.regionServer.messagePort");
		    List<String> viewManagers = config.getList("networkconfig.viewManager.nodes.node");
		    String viewManagerMessagePort = config.getString("networkconfig.viewManager.messagePort");
		    String viewManagerUpdatePort = config.getString("networkconfig.viewManager.updatePort");

		    int masterPortTemp = 0;
		    if(masterPort != null)masterPortTemp = Integer.parseInt(masterPort);
		    
		    NetworkConfig.HBASE_MASTER = new Node(master, masterPortTemp,0, new SSHConnection(master, 22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD));
		    NetworkConfig.VM_MASTER =  new Node(master, Integer.parseInt(vmMasterPort),0, new SSHConnection(master, 22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD));
		    NetworkConfig.CLIENTS = new ArrayList<Node>();
		    NetworkConfig.REGIONSERVERS = new ArrayList<Node>();
		    NetworkConfig.VIEWMANAGERS = new ArrayList<Node>();

		    
		    for (String string : clients) {
		    	NetworkConfig.CLIENTS.add(new Node(string, 0, 0, new SSHConnection(string, 22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD)));
				
			}
		    for (String string : regionServers) {
		    	NetworkConfig.REGIONSERVERS.add(new Node(string, Integer.parseInt(regionServerMessagePort), 0,  new SSHConnection(string, 22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD)));
				
			}
		    for (String string : viewManagers) {
		    	NetworkConfig.VIEWMANAGERS.add(new Node(string,  Integer.parseInt(viewManagerMessagePort), Integer.parseInt(viewManagerUpdatePort),
		    			new SSHConnection(string, 22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD)));
				
			}			 		    


		    log.info(NetworkConfig.class, "NetworkConfig.ZOOKEEPER_QUORUM:"+NetworkConfig.ZOOKEEPER_QUORUM);
		    log.info(NetworkConfig.class, "NetworkConfig.ZOOKEEPER_CLIENTPORT:"+NetworkConfig.ZOOKEEPER_CLIENTPORT);
		    
		    log.info(NetworkConfig.class, "NetworkConfig.HBASE_MASTER:"+NetworkConfig.HBASE_MASTER);
		    log.info(NetworkConfig.class, "NetworkConfig.VM_MASTER:"+NetworkConfig.VM_MASTER);
		    
		    
		    log.info(NetworkConfig.class, "NetworkConfig.CLIENTS:"+NetworkConfig.CLIENTS);
		    log.info(NetworkConfig.class, "NetworkConfig.REGIONSERVERS:"+NetworkConfig.REGIONSERVERS);
		    log.info(NetworkConfig.class, "NetworkConfig.VIEWMANAGERS:"+NetworkConfig.VIEWMANAGERS);
		    
		    log.info(NetworkConfig.class, "NetworkConfig.HDFS:"+NetworkConfig.HDFS);	    
		}
		catch(ConfigurationException cex)
		{
			log.error(NetworkConfig.class, cex);
			cex.printStackTrace();
			System.exit(0);
		} 

		log.info(NetworkConfig.class, "--------------------------------------------------------------");

	}
	
	public static Configuration getHBaseConfiguration(Log log){
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", NetworkConfig.ZOOKEEPER_QUORUM);	
		log.info(Client.class, "NetworkConfig.ZOOKEEPER_QUORUM: "+NetworkConfig.ZOOKEEPER_QUORUM);
		
		if(NetworkConfig.ZOOKEEPER_CLIENTPORT != 0){
			conf.set("hbase.zookeeper.property.clientPort", NetworkConfig.ZOOKEEPER_CLIENTPORT+"");
			log.info(Client.class, "NetworkConfig.ZOOKEEPER_CLIENTPORT: "+NetworkConfig.ZOOKEEPER_CLIENTPORT);
		}
	
		conf.set("hbase.master", NetworkConfig.HBASE_MASTER.getIpAddress());
		log.info(Client.class, "NetworkConfig.HBASE_MASTER: "+NetworkConfig.HBASE_MASTER.getIpAddress());

		if(NetworkConfig.HBASE_MASTER.getMessagePort() != 0){
			conf.set("hbase.master.port", NetworkConfig.HBASE_MASTER.getMessagePort()+"");
			log.info(Client.class, "NetworkConfig.HBASE_MASTER_PORT: "+NetworkConfig.HBASE_MASTER.getMessagePort());
		}
		
		return conf;
	}
	
	public static Configuration getHadoopConfiguration(){
		
		Configuration conf = HBaseConfiguration.create();

		System.setProperty("HADOOP_USER_NAME", SystemConfig.HDFS_USERNAME);
		
		conf = new Configuration();
		conf.set("fs.default.name", NetworkConfig.HDFS);

		return conf;
	}
	
	
	public static void main(String[] args){
//		 load();
	}
	

}
