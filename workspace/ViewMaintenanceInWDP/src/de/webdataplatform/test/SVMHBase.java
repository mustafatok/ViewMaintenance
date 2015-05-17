package de.webdataplatform.test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.Node;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.ssh.SSHService;

public class SVMHBase {
	
	
	
	private Log log;

	
	public SVMHBase(Log log) {
		super();
		this.log = log;
	}
	
	
	public void configureAndStart(int numOfRegionServers, String directoryName){
		
		killAll(numOfRegionServers);
		
		configureHadoop(numOfRegionServers, directoryName);
		configureHbase(numOfRegionServers, directoryName);
		
		cleanZookeeper();
		
		
		cleanHadoop(numOfRegionServers);
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		formatHadoop();
		
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		startHadoop();

		try {
			Thread.sleep(45000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		startHBase();
		
		
	}
	
	public void stopAll(){
		
		stopHBase();
		try {
			Thread.sleep(40000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		stopHadoop();
		
	}
	
	public void killAll(int numOfRs) {
		
		log.info(this.getClass(), "killing all processes");
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("pkill -9 -f hbase");
		startupCommand.add("pkill -9 -f hadoop");
		startupCommand.add("pkill -9 -f zookeeper");
		startupCommand.add("pkill -9 -f de.webdataplatform.viewmanager.TestViewManager");
		startupCommand.add("pkill -9 -f de.webdataplatform.regionserver.TestRegionServer");
		startupCommand.add("pkill -9 -f de.webdataplatform.master.TestMaster");
		startupCommand.add("pkill -9 -f de.webdataplatform.client.ClientProcess");
		
		int x=0;
		while (x < numOfRs) {
			
			Node node = NetworkConfig.REGIONSERVERS.get(x);
			
			log.info(this.getClass(), "killing node: "+node.getIpAddress());	
			SSHService.sendCommand(log, node.getSshConnection(), startupCommand);

			x++;
		
		}
		
		SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);
		

		log.info(this.getClass(), "\n");
		
	}
	
	
	public void startHBase() {
		
		log.info(this.getClass(), "starting hbase:"+NetworkConfig.HBASE_MASTER);
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HBASE+"bin");
		startupCommand.add("./start-hbase.sh");
		
		List<String> result = SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);
		
		
		log.info(this.getClass(), "\n");
		
	}
	public void stopHBase() {
		
		log.info(this.getClass(), "stopping hbase:"+NetworkConfig.HBASE_MASTER);
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HBASE+"bin");
		startupCommand.add("./stop-hbase.sh");
		
		
		List<String> result = SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);
		
		
		log.info(this.getClass(), "\n");
		
	}
	
	public void configureHbase(int numOfRs, String directory) {
		
		log.info(this.getClass(), "configuring hbase");
		log.info(this.getClass(), "region server nodes: "+numOfRs);
		log.info(this.getClass(), "setting config in directory: "+SystemConfig.DIRECTORY_HBASE+"conf");
		log.info(this.getClass(), "on node: "+NetworkConfig.HBASE_MASTER.getSshConnection());
		
		
		
		PrintWriter writer=null;
		
		try {
			writer = new PrintWriter(directory+"/regionservers", "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		
		int x = 0;
		
		while (x < numOfRs) {
			
			Node node = NetworkConfig.REGIONSERVERS.get(x);
			
				
			writer.print(node.getIpAddress()+"\n");
			writer.flush();

			x++;
		
		}
		writer.close();
		
		SSHService.copyFile(log, NetworkConfig.HBASE_MASTER.getSshConnection(), directory, SystemConfig.DIRECTORY_HBASE+"conf",  "regionservers");
		
		log.info(this.getClass(), "\n");
	}
	
	
	
	public void startHadoop() {
		
		log.info(this.getClass(), "starting hadoop:"+NetworkConfig.HBASE_MASTER);
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HDFS+"bin");
		startupCommand.add("./start-dfs.sh");
		
		SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);

		
		log.info(this.getClass(), "\n");
		
	}
	
	public void stopHadoop() {
		
		log.info(this.getClass(), "stopping hadoop:"+NetworkConfig.HBASE_MASTER);
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HDFS+"bin");
		startupCommand.add("./stop-dfs.sh");
		
		
		List<String> result = SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);
		
		log.info(this.getClass(), "\n");
		
	}
	
	public void cleanHadoop(int numOfRs) {
		
		log.info(this.getClass(), "cleaning hadoop");
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HDFSSTORAGE);
		startupCommand.add("rm -R ./*");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HDFS+"logs");
		startupCommand.add("rm -R ./*");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HBASE+"logs");
		startupCommand.add("rm -R ./*");
		
		int x=0;
		while (x < numOfRs) {
			
			Node node = NetworkConfig.REGIONSERVERS.get(x);
			
			log.info(this.getClass(), "cleaning node: "+node.getIpAddress());	
			SSHService.sendCommand(log, node.getSshConnection(), startupCommand);

			x++;
		
		}
		
		SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);
		

		log.info(this.getClass(), "\n");
		
	}
	
	public void formatHadoop() {
		
		log.info(this.getClass(), "formating hadoop");
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_HDFS+"bin");
		startupCommand.add("./hadoop namenode -format");
		
		List<String> result = SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);


		log.info(this.getClass(), "\n");
		
	}
	
	public void configureHadoop(int numOfRs, String directory) {
		
		log.info(this.getClass(), "configuring hadoop");
		log.info(this.getClass(), "region server nodes: "+numOfRs);
		log.info(this.getClass(), "setting config in directory: "+SystemConfig.DIRECTORY_HDFS+"conf");
		log.info(this.getClass(), "on node: "+NetworkConfig.HBASE_MASTER.getSshConnection());
		
		
		PrintWriter writer=null;
		
		try {
			writer = new PrintWriter(directory+"/slaves", "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		
		int x = 0;
		
		while (x < numOfRs) {
			
			Node node = NetworkConfig.REGIONSERVERS.get(x);
			
				
			writer.print(node.getIpAddress()+"\n");
			writer.flush();

			x++;
		
		}
		writer.close();
		log.info(this.getClass(), "\n");
		log.info(this.getClass(), "\n");
		SSHService.copyFile(log, NetworkConfig.HBASE_MASTER.getSshConnection(), directory, SystemConfig.DIRECTORY_HDFS+"conf",  "slaves");
		
		log.info(this.getClass(), "\n");
	}
	
	public void cleanZookeeper() {
		
		log.info(this.getClass(), "cleaning zookeeper:"+NetworkConfig.HBASE_MASTER);
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_ZOOKEEPERSTORAGE);
		startupCommand.add("rm -R ./*");
				
		SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);
		

		log.info(this.getClass(), "\n");
		
	}
	

}
