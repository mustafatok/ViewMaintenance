package de.webdataplatform.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.client.Client;
import de.webdataplatform.log.Log;
import de.webdataplatform.settings.CreateAggregationView;
import de.webdataplatform.settings.CreateBaseTable;
import de.webdataplatform.settings.CreateIndexView;
import de.webdataplatform.settings.CreateJoinView;
import de.webdataplatform.settings.CreateSelectionView;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.Experiment;
import de.webdataplatform.settings.ICreateTable;
import de.webdataplatform.settings.ICreateView;
import de.webdataplatform.settings.JoinTablePair;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.Node;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.settings.TableDefinition;
import de.webdataplatform.settings.ViewTester;
import de.webdataplatform.ssh.SSHConnection;
import de.webdataplatform.ssh.SSHService;
import de.webdataplatform.view.TableService;
import de.webdataplatform.view.ViewDefinitions;

public class SVMSystem {


	
	
	private Log log;
	
	private SVMHBase svmHBase;
	
	public void stopVMSystem(boolean hardReset, Experiment experiment){
		

		if(hardReset)log= new Log("stop.log");	
		log.info(this.getClass(), "stopping system....");
		stopClients(experiment.getNumOfClients());
		stopMaster();
		stopRegionServers(experiment.getNumOfRegionServers());
		stopViewManagers(experiment.getNumOfViewManagers());
		
	}


	
//	public void completeTestRun(boolean completeSetup, String runName,  int numOfRegionServers, int numOfViewManager, String distribution, List<String> viewTableTypes, int numOfViews, long numOfOperations, long numOfRecords, int numOfBaseTableRegions, int numOfViewTableRegions, int numOfAggKeys, boolean useDeletes, boolean useUpdates, long readDelay){
		public void completeTestRun(Experiment experiment){		
		
			


		String datestring = new SimpleDateFormat("dd-MM-yyyy,HH-mm").format(new Date());
		
		
		String directoryName = "testresults/testrun,"+datestring+","+experiment.getNumOfRegionServers()+","+experiment.getNumOfViewManagers()+","+experiment.getNumOfClients();
		
		File dir = new File(directoryName);
		dir.mkdir();
		dir = new File(directoryName+"/regionserverStatistics");
		dir.mkdir();
		dir = new File(directoryName+"/regionserverLogs");
		dir.mkdir();
		dir = new File(directoryName+"/viewmanagerStatistics");
		dir.mkdir();
		dir = new File(directoryName+"/viewmanagerLogs");
		dir.mkdir();
		dir = new File(directoryName+"/masterLogs");
		dir.mkdir();
		
		log= new Log("experiment.log");		
		svmHBase = new SVMHBase(log);
//		log.info(this.getClass(), "setting hbase config");
//		setRegionServersHbase(experiment.getNumOfRegionServers(), directoryName);
		
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			log.info(this.getClass(), "configuring and starting database");
			svmHBase.configureAndStart(experiment.numOfRegionServers, directoryName);
		} catch (Exception e) {

			log.error(this.getClass(), e);
		}
		

		try {
			Thread.sleep(35000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
			
		ViewDefinitions viewDefinitions = new ViewDefinitions(log, "viewdefinitions");
		
		try {

			viewDefinitions.generateTable();
			viewDefinitions.generateViewDefinitions(experiment.getCreateViewTables());
			viewDefinitions.loadViewDefinitions();
			
		} catch (Exception e) {

			log.error(this.getClass(), e);
		}
		
		

		
		log.info(this.getClass(), "creating view tables, setting process flag....");
		log.info(this.getClass(), "view tables "+experiment.getCreateViewTables());
		createTables(experiment.getNumOfClients(),experiment.getCreateViewTables());

		
		if(experiment.getCreateBaseTables().size() > 0){
			
				log.info(TestClient.class, "creating base tables, setting process flag....");
				log.info(TestClient.class, "base tables "+experiment.getCreateBaseTables());
//				createTables(experiment.getNumOfClients(),experiment.getCreateBaseTables());
				
				List<ICreateTable> baseTables = new ArrayList<ICreateTable>();
				
				for (ICreateTable iCreateTable : experiment.getCreateBaseTables()) {
					baseTables.add(iCreateTable);
//					CreateBaseTable createBaseTable = iCreateTable.copy();
//					createBaseTable.setName(createBaseTable.getName()+"_commit");
//					baseTables.add(createBaseTable);
					
				}
				
				createTables(experiment.getNumOfClients(), baseTables);
				
		}		

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		

		
		
		log.info(this.getClass(), "starting components....");
		startMaster();
		try {
			Thread.sleep(8000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		startRegionServers(experiment.getNumOfRegionServers());
		
		startViewManagers(experiment.getNumOfViewManagers());

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
//		
//		if(createAndFillBaseTables && EvaluationConfig.getCreateBaseTables().size() > 0){
//					


		log.info(TestClient.class, "filling base tables....");
		log.info(TestClient.class, "base tables "+experiment.getCreateBaseTablesAsBaseTables());
		long start = new Date().getTime();
		startClients(experiment.getNumOfClients(), (List<CreateBaseTable>)experiment.getCreateBaseTablesAsBaseTables());
		log.info(this.getClass(), "clients started in "+(new Date().getTime() - start)+" ms");					
//					
//		
//		}else{
//			log.info(this.getClass(), "base table already filled");
//		}
		
		


		
	
//		Killing View Managers

		boolean viewManagerKilled=false;
		
		
		start = new Date().getTime();
		log.info(this.getClass(), "checking if base table filled....");

		while(true){

			if(experiment.getNumOfKilledViewManagers() > 0 && ((new Date().getTime() - start) > EvaluationConfig.VIEWMANAGER_KILLDELAY) && !viewManagerKilled){
				
				killViewManagers(experiment.getNumOfKilledViewManagers());
				viewManagerKilled = true;
			}
			
			if(checkDatabaseFilled(experiment.getNumOfClients())){
				log.info(this.getClass(), "base table filled in "+(new Date().getTime() - start)+" ms");
				queueMarkers((List<CreateBaseTable>)experiment.getCreateBaseTablesAsBaseTables());
				
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
		log.info(this.getClass(), "checking if update process is finished....");
		long lastMeasure = new Date().getTime();
		long currentTime2;
		
		while(true){
			
			if(experiment.getNumOfKilledViewManagers() > 0 && ((new Date().getTime() - start) > EvaluationConfig.VIEWMANAGER_KILLDELAY) && !viewManagerKilled){
				
				killViewManagers(experiment.getNumOfKilledViewManagers());
				viewManagerKilled = true;
			}
			
			currentTime2 = new Date().getTime();
			if(EvaluationConfig.UPDATEPROCESSTIMEOUT != 0 && (currentTime2 - lastMeasure) > EvaluationConfig.UPDATEPROCESSTIMEOUT){
				log.info(this.getClass(), "update process failed....");
				break;
			}
			
			if(checkProcessFinished((List<CreateBaseTable>)experiment.getCreateBaseTablesAsBaseTables())){
				log.info(this.getClass(), "view table filled in "+(currentTime2 - lastMeasure)+" ms");
				log.info(this.getClass(), "update process is finished, cleaning up....");
				break;
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		try {
			getTable().close();
			setTable(null);
		} catch (IOException e1) {
			log.error(this.getClass(), e1);
		}
		
		stopVMSystem(false, experiment);
		
//		if(stopDatabase){

//		}
		
		log.info(this.getClass(), "checking view tables....");
		TableService tableService = new TableService(log);
		
		
		
		for (ICreateTable createTable : experiment.getCreateViewTables()) {
			
			log.info(this.getClass(), "checking view table:"+createTable.getName()+", basetable: "+createTable.getControlTables());
			if(createTable.getControlTables() != null && !createTable.getControlTables().equals("") ){
				
				log.info(this.getClass(), "checking table: "+createTable.getName());
				
				ICreateView cAV = (ICreateView)createTable;	
				ViewTester viewTester = new ViewTester(log, new TableService(log));
				viewTester.check(createTable.getControlTables(), createTable.getName(), cAV.getViewDefinition());
				
				
			}
		}
		tableService.close();
		
		

			
		
		log.info(this.getClass(), "collecting statistics....");
		readStatisticsLogMaster(directoryName);
		readStatisticsLogsRegionServers(experiment.getNumOfRegionServers(), directoryName);
		readStatisticsLogsViewManagers(experiment.getNumOfViewManagers(), directoryName);
		readStatisticsLogsClients(experiment.getNumOfClients(), experiment.getCreateBaseTables(), directoryName);
		log.info(this.getClass(), "exiting....");
		log.info(this.getClass(), "stopping database....");
		
		
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		}
		
//		StatisticLog.close();
		
		svmHBase.stopAll();
		
		
		readStatisticsLogExperiment(directoryName, experiment);

		SSHService.closeSessions();
		log.close();
		
		
//		try {
//			Thread.sleep(30000);
//		} catch (InterruptedException e) {
//
//			e.printStackTrace();
//		}
//		
	}

	
		public void onlyBaseTables(Experiment experiment){		
			
			


			String datestring = new SimpleDateFormat("dd-MM-yyyy,HH-mm").format(new Date());
		
			String directoryName = "testresults/testrun,"+datestring+","+experiment.getNumOfRegionServers()+","+experiment.getNumOfViewManagers()+","+experiment.getNumOfClients();
			File dir = new File(directoryName);
			dir.mkdir();
			
			log= new Log("experiment.log");		
			svmHBase = new SVMHBase(log);
			
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			

			log.info(this.getClass(), "configuring and starting database");
			svmHBase.configureAndStart(experiment.numOfRegionServers, directoryName);

			try {
				Thread.sleep(35000);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
				
			ViewDefinitions viewDefinitions = new ViewDefinitions(log, "viewdefinitions");
			
			try {

				viewDefinitions.generateTable();

				
			} catch (Exception e) {

				log.error(this.getClass(), e);
			}
			
			
			if(experiment.getCreateBaseTables().size() > 0){
				
					log.info(TestClient.class, "creating base tables, setting process flag....");
					log.info(TestClient.class, "base tables "+experiment.getCreateBaseTables());
					
					
					List<ICreateTable> baseTables = new ArrayList<ICreateTable>();
					
					for (ICreateTable iCreateTable : experiment.getCreateBaseTables()) {
						baseTables.add(iCreateTable);
//						CreateBaseTable createBaseTable = iCreateTable.copy();
//						createBaseTable.setName(createBaseTable.getName()+"_commit");
//						baseTables.add(createBaseTable);
						
					}
					
					createTables(experiment.getNumOfClients(), baseTables);
					
			}		

			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}


			log.info(TestClient.class, "filling base tables....");
			log.info(TestClient.class, "base tables "+experiment.getCreateBaseTables());
			long start = new Date().getTime();
			startClients(experiment.getNumOfClients(), (List<CreateBaseTable>)experiment.getCreateBaseTablesAsBaseTables());
			log.info(this.getClass(), "clients started in "+(new Date().getTime() - start)+" ms");			

			
			log.info(this.getClass(), "checking if base table filled....");
			start = new Date().getTime();
			while(true){

				if(checkDatabaseFilled(experiment.getNumOfClients())){
					log.info(this.getClass(), "base table filled in "+(new Date().getTime() - start)+" ms");
					queueMarkers((List<CreateBaseTable>)experiment.getCreateBaseTablesAsBaseTables());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}

			
			try {
				getTable().close();
				setTable(null);
			} catch (IOException e1) {
				log.error(this.getClass(), e1);
			}
				
			
			log.info(this.getClass(), "collecting statistics....");
			readStatisticsLogsClients(experiment.getNumOfClients(), experiment.getCreateBaseTables(), directoryName);
			log.info(this.getClass(), "exiting....");
			log.info(this.getClass(), "stopping database....");
			

			svmHBase.stopAll();
			readStatisticsLogExperiment(directoryName,experiment);
			SSHService.closeSessions();
			log.close();
			
		
		}
	

	public void createTables(int numOfClients, List<ICreateTable> createTables) {

			

			
			for(ICreateTable createTable : createTables){
			
				createTable(numOfClients, createTable);
			}
		
	}



	
	
	public void createTable(int numOfClients, ICreateTable createTable) {
		
		log.info(this.getClass(), "");
		log.info(this.getClass(), "client nodes: "+numOfClients);
		log.info(this.getClass(), "creating table");
		log.info(this.getClass(), "name: "+createTable.getName());
		log.info(this.getClass(), "type: "+createTable.getType());

		
		Node node = NetworkConfig.CLIENTS.get(0);
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		
//		TableDefinition tableDefinition = DatabaseConfig.getTableDefinition(createTable.getName());
		
//		if(tableDefinition == null){
//			log.info(this.getClass(), "create standard table");
//			startupCommand.add("nohup java -cp vmsystem.jar de.webdataplatform.client.ClientProcess ccreatetable createtable "+createTable.getName()+" &");
//			
//		}else{
		log.info(this.getClass(), "create split table");
		startupCommand.add("java -cp vmsystem.jar de.webdataplatform.client.ClientProcess ccreatetable createsplittable "+createTable.getName()+" "+createTable.getNumOfRegions());
			
//		}

		
		log.info(this.getClass(), "starting client:"+node);
		List<String> result = SSHService.sendCommand(log, node.getSshConnection(), startupCommand);

		log.info(this.getClass(), "\n");
	}
	

	

	public void startClients(Integer numOfClients, List<CreateBaseTable> createBaseTables) {

			
			for(CreateBaseTable createBaseTable : createBaseTables){
			
				fillBaseTable(numOfClients, createBaseTable);
			}
		
	}
	
	public void fillBaseTable(Integer numOfClients, CreateBaseTable createBaseTable) {

		log.info(this.getClass(), "filling base table "+createBaseTable);
		log.info(this.getClass(), "using  "+numOfClients+" client");
		log.info(this.getClass(), ""+(createBaseTable.getNumOfOperations()/numOfClients)+" operations per client");
		
		int x = 0;
		
//		int messagePort = vmMessagePort;
		while (x < numOfClients) {
			
			Node node = NetworkConfig.CLIENTS.get(x%NetworkConfig.CLIENTS.size());
			
			List<String> startupCommand = new ArrayList<String>();
			startupCommand.add("cd /");
			startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
			startupCommand.add("java -cp vmsystem.jar de.webdataplatform.client.ClientProcess c"+(x+1)+" filltable "+createBaseTable.getName()+" "+createBaseTable.getDistribution()+" "+(createBaseTable.getNumOfOperations()/numOfClients));
			
			log.info(this.getClass(), "starting client:"+node);
			List<String> result = SSHService.sendCommand(log, node.getSshConnection(), startupCommand);
			x++;
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//
//				e.printStackTrace();
//			}

		}
		log.info(this.getClass(), "\n");
	
	
}

	
	public void queueMarkers(List<CreateBaseTable> createTables) {
		

		Node node = NetworkConfig.CLIENTS.get(0);
		
		for (CreateBaseTable createTable : createTables) {
				
			List<String> startupCommand = new ArrayList<String>();
			startupCommand.add("cd /");
			startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
			
	
			log.info(this.getClass(), "queue markers to base table");
			startupCommand.add("java -cp vmsystem.jar de.webdataplatform.client.ClientProcess cqueuemarker queuefinishmarkers "+createTable.getName()+" "+createTable.getNumOfRegions());
			
			log.info(this.getClass(), "starting client:"+node);
			List<String> result = SSHService.sendCommand(log, node.getSshConnection(), startupCommand);
	
			log.info(this.getClass(), "\n");
		}
	}
	

	private void stopClients(int numOfClients) {
		
		int x = 0;
		
		while (x < numOfClients) {
			
			Node node = NetworkConfig.CLIENTS.get(x%NetworkConfig.CLIENTS.size());
			
			log.info(this.getClass(), "stopping client:"+node);
			
			List<String> stopCommand = new ArrayList<String>();
			stopCommand.add("pkill -9 -f ClientProcess");
			
			List<String> result = SSHService.sendCommand(log, node.getSshConnection(), stopCommand);
			x++;
		}
		log.info(this.getClass(), "\n");
	}
	

	
	
	private void startMaster() {
		
		log.info(this.getClass(), "starting master:"+NetworkConfig.VM_MASTER);
		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		startupCommand.add("java -Xmx1000m -cp vmsystem.jar de.webdataplatform.master.TestMaster master1 "+NetworkConfig.VM_MASTER.getIpAddress()+" "+NetworkConfig.VM_MASTER.getMessagePort());
		
		
		List<String> result = SSHService.sendCommand(log, NetworkConfig.VM_MASTER.getSshConnection(), startupCommand);
		
		log.info(this.getClass(), "\n");
		
	}

	
	private void stopMaster() {
		List<String> stopCommand = new ArrayList<String>();
		stopCommand.add("pkill -9 -f TestMaster");
		
		log.info(this.getClass(), "stopping master:");
		List<String> result = SSHService.sendCommand(log, NetworkConfig.VM_MASTER.getSshConnection(), stopCommand);
		
		log.info(this.getClass(), "\n");

	}
	

	
	
	HTable table;
	
	public void setTable(HTable table){
		this.table = table;
	}
	
	public 	HTable getTable(){
		
		
		if(table == null){
				Configuration conf = NetworkConfig.getHBaseConfiguration(log);
				try {
					table = new HTable(conf, "finish_markers");
				} catch (IOException e) {
					e.printStackTrace();
				}
	
		}
		return table;
		
	}
	
	private boolean checkDatabaseFilled(int numOfClients){
	

		
		int x = 0;
		
		while (x < numOfClients) {
			
			
			Get get = new Get(Bytes.toBytes("c"+(x+1)));
			
			try {
				if(!getTable().exists(get)){
					log.info(this.getClass(), "c"+(x+1)+":false");
					return false;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			x++;
		}
		
		return true;
		
	}
	
	
	
	private boolean checkProcessFinished(List<CreateBaseTable> baseTables){
		

		Client client = new Client(log);

		for (CreateBaseTable createBaseTable : baseTables) {
			log.info(Client.class, "checking base table: "+createBaseTable.getName());
			try {
				byte[][] regions =  client.createRegionArray(createBaseTable.getName(), createBaseTable.getNumOfRegions());
				for (int i = 0; i < regions.length; i++) {
					
					
					String key = Bytes.toString(regions[i])+"_finishmarker";
					log.info(Client.class, "checking finish marker: "+key);
					Get get = new Get(Bytes.toBytes(key));
					
					if(!getTable().exists(get)){
						log.info(this.getClass(), key+":false");
						return false;
					}
					
					
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			}
		}
		
		
		
		

		
		
//		while (x < numOfRs) {
//			
//			
//			Get get = new Get(Bytes.toBytes("rs"+(x+1)));
//			
//			try {
//				if(!getTable().exists(get)){
//					log.info(this.getClass(), "rs"+(x+1)+":false");
//					return false;
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//			x++;
//		}
		
//		HTable baseTable = new HTable(conf, tableName);
//		
//		byte[][] regions = createRegionArray(tableName, regCount);
//		
//		
//		for (int i = 0; i < regions.length; i++) {
//			
//			
//			String key = Bytes.toString(regions[i])+"_finishmarker";
//			log.info(Client.class, "queueing finish marker: "+key);
//			
//			Put put = new Put(Bytes.toBytes(key));
//			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("finishMarker"), Bytes.toBytes(name));	
//			baseTable.put(put);
//			
//		}

//		baseTable.close();
		
//		byte[][] regions = createRegionArray(tableName, regCount);
//		
//		
//		for (int i = 0; i < regions.length; i++) {
//			
//			
//			String key = Bytes.toString(regions[i])+"_finishmarker";
//			log.info(Client.class, "queueing finish marker: "+key);
//			
//			Put put = new Put(Bytes.toBytes(key));
//			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("finishMarker"), Bytes.toBytes(name));	
//			baseTable.put(put);
//			
//		}
//		
		return true;
		
	}
	
	
	private void startRegionServers(int numOfRs) {
		
		log.info(this.getClass(), "");
		log.info(this.getClass(), "region server nodes: "+numOfRs);
		log.info(this.getClass(), "");
		
		int x = 0;
		
		while (x < numOfRs) {
			
			Node node = NetworkConfig.REGIONSERVERS.get(x%NetworkConfig.REGIONSERVERS.size());
			
			List<String> startupCommand = new ArrayList<String>();
			startupCommand.add("cd /");
			startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
			startupCommand.add("java -Xmx1000m -cp vmsystem.jar de.webdataplatform.regionserver.TestRegionServer rs"+(x+1)+" "+node.getIpAddress()+" "+node.getMessagePort());
			
			log.info(this.getClass(), "starting region server:"+node);
			List<String> result = SSHService.sendCommand(log, node.getSshConnection(), startupCommand);

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}

			x++;
		
		}
		
		
		log.info(this.getClass(), "\n");
	}
	

	
	
	
	
	
	
	
	
	private void readStatisticsLogsRegionServers(int numOfRs, String directory) {
		
		
		int x = 0;
		
		
		while (x < numOfRs) {
				

				Node node = NetworkConfig.REGIONSERVERS.get(x%NetworkConfig.REGIONSERVERS.size());
				
				log.info(this.getClass(), "reading statistics log of region server rs"+(x+1)+" on node: "+node);
				SSHService.retrieveFile(log, node.getSshConnection(), SystemConfig.DIRECTORY_VMSYSTEM+"/logs", directory+"/regionserverStatistics", "rs"+(x+1)+"-statistic.log");
				SSHService.retrieveFile(log, node.getSshConnection(), SystemConfig.DIRECTORY_VMSYSTEM+"/logs",directory+"/regionserverLogs", "rs"+(x+1)+".log");
				
				
				
				x++;
		}
		
		
		
		log.info(this.getClass(), "\n");
	}	
	
	

	
	private void stopRegionServers(int numOfRs) {
		
		int x = 0;
		
		while (x < numOfRs) {
			
			Node node = NetworkConfig.REGIONSERVERS.get(x%NetworkConfig.REGIONSERVERS.size());
			
			log.info(this.getClass(), "stopping region server:"+node);
			
			List<String> stopCommand = new ArrayList<String>();
			stopCommand.add("pkill -9 -f TestRegionServer");
			
			List<String> result = SSHService.sendCommand(log, node.getSshConnection(), stopCommand);
			x++;
		}
		log.info(this.getClass(), "\n");
	}
	
	
	private void startViewManagers(Integer viewManagerInstances) {
		

		Integer instancesPerNode = (viewManagerInstances/NetworkConfig.VIEWMANAGERS.size())+1;
		if(instancesPerNode == 0)instancesPerNode=1;
		
		
//		log.info(this.getClass(), "viewmanager nodes: "+NetworkConfig.VIEWMANAGERS.size());
		log.info(this.getClass(), "viewManagerInstances: "+viewManagerInstances);
		log.info(this.getClass(), "instancesPerNode: "+instancesPerNode);
		log.info(this.getClass(), "");
		
		int x= 0;

			
//			int messagePort = vmMessagePort;
			while (x < viewManagerInstances) {
				
				Node node = NetworkConfig.VIEWMANAGERS.get(x%NetworkConfig.VIEWMANAGERS.size());
				int updatePort = node.getUpdatePort()+(x/NetworkConfig.VIEWMANAGERS.size());
				int messagePort = node.getMessagePort()+(x/NetworkConfig.VIEWMANAGERS.size());
				
				List<String> startupCommand = new ArrayList<String>();
				startupCommand.add("cd /");
				startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
				startupCommand.add("java  -Xmx1000m -cp vmsystem.jar de.webdataplatform.viewmanager.TestViewManager vm"+(x+1)+" "+node.getIpAddress()+" "+updatePort+" "+messagePort);
				log.info(this.getClass(), "starting viewmanager vm"+(x+1)+" on node: "+node.getIpAddress()+" udpatePort:"+updatePort+", messagePort:"+messagePort);
				List<String> result = SSHService.sendCommand(log, node.getSshConnection(), startupCommand);

				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {

					e.printStackTrace();
				}
				x++;
			
			}

		log.info(this.getClass(), "\n");
	}
	
	
	
	private void readStatisticsLogsViewManagers(Integer viewManagerInstances, String directory) {
		
		int x= 0;
		
		
		while (x < viewManagerInstances) {
				

				Node node = NetworkConfig.VIEWMANAGERS.get(x%NetworkConfig.VIEWMANAGERS.size());
				
				log.info(this.getClass(), "reading statistics log of viewmanager vm"+(x+1)+" on node: "+node);
				SSHService.retrieveFile(log, node.getSshConnection(),SystemConfig.DIRECTORY_VMSYSTEM+"/logs", directory+"/viewmanagerStatistics", "vm"+(x+1)+"-statistic.log");
				SSHService.retrieveFile(log, node.getSshConnection(),SystemConfig.DIRECTORY_VMSYSTEM+"/logs", directory+"/viewmanagerLogs", "vm"+(x+1)+".log");
				
			
				
				x++;
		}

		
		log.info(this.getClass(), "\n");
	}
	
	private void readStatisticsLogMaster(String directory) {
		
		
		log.info(this.getClass(), "reading statistics log of master on node: "+NetworkConfig.VM_MASTER);
		SSHService.retrieveFile(log, NetworkConfig.VM_MASTER.getSshConnection(),SystemConfig.DIRECTORY_VMSYSTEM+"/logs", directory+"/masterLogs", "master.log");
				

		
		log.info(this.getClass(), "\n");
	}
	
	private void readStatisticsLogsClients(Integer numOfClients, List<ICreateTable> baseTables, String directory) {
		
		for (ICreateTable createTable : baseTables) {
			
			int x= 0;
						
			while (x < numOfClients) {
					

				Node node = NetworkConfig.CLIENTS.get(x%NetworkConfig.CLIENTS.size());
				log.info(this.getClass(), "reading statistics log of client "+"c"+(x+1)+"-filltable-"+createTable.getName()+" on node: "+node);
				SSHService.retrieveFile(log, node.getSshConnection(),SystemConfig.DIRECTORY_VMSYSTEM+"/logs" , directory+"", "c"+(x+1)+"-filltable-"+createTable.getName()+".log");
				x++;
				
	
			}
		}
	}
	
	
	private void readStatisticsLogExperiment(String directory, Experiment experiment) {
		
		
		log.info(this.getClass(), "reading statistics log of experiment: "+NetworkConfig.VM_MASTER);
		SSHService.retrieveFile(log, new SSHConnection("localhost",22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD),SystemConfig.DIRECTORY_VMSYSTEM+"/logs" , directory+"", "experiment.log");
		SSHService.retrieveFile(log, new SSHConnection("localhost",22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD),SystemConfig.DIRECTORY_VMSYSTEM+"/logs" , directory+"", "evaluation.log");
		SSHService.retrieveFile(log, new SSHConnection("localhost",22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD),SystemConfig.DIRECTORY_VMSYSTEM+"/logs" , directory+"", "queuefinishmarkers.log");
//		SSHService.retrieveFile(log, new SSHConnection("localhost",22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD),SystemConfig.DIRECTORY_VMSYSTEM+"/logs" , directory+"", "ccreatetable.log");
		
		
		for (ICreateTable createTable : experiment.getCreatedTables())
		SSHService.retrieveFile(log, new SSHConnection("localhost",22, SystemConfig.SSH_USERNAME, SystemConfig.SSH_PASSWORD),SystemConfig.DIRECTORY_VMSYSTEM+"/logs" , directory+"", "createsplittable-"+createTable.getName()+".log");


		
		
		
		log.info(this.getClass(), "\n");
	}	
	
	
	
	private void stopViewManagers(int viewManagerInstances) {
		
		
		int x= 0;
		
		
		while (x < viewManagerInstances) {
				

			Node node = NetworkConfig.VIEWMANAGERS.get(x%NetworkConfig.VIEWMANAGERS.size());
			
			log.info(this.getClass(), "stopping view manager:"+node);
			
			List<String> stopCommand = new ArrayList<String>();
			stopCommand.add("pkill -9 -f TestViewManager");
			
			List<String> result = SSHService.sendCommand(log, node.getSshConnection(), stopCommand);
			x++;

		}
		log.info(this.getClass(), "\n");
	}
	
	private void killViewManagers(Integer numOfKilledViewManagers) {
		


		
		
		log.info(this.getClass(), "viewmanager nodes: "+NetworkConfig.VIEWMANAGERS.size());
		log.info(this.getClass(), "Instances to kill: "+numOfKilledViewManagers);

		log.info(this.getClass(), "");
		
		int x= 0;

			
//			int updatePort = vmUpdatePort;
//			int messagePort = vmMessagePort;
			while (x < numOfKilledViewManagers) {
				
				Node node = NetworkConfig.VIEWMANAGERS.get(x%NetworkConfig.VIEWMANAGERS.size());
				
				List<String> startupCommand = new ArrayList<String>();

				startupCommand.add("pkill -9 -f vm"+(x+1));
				log.info(this.getClass(), "killing viewmanager vm"+(x+1)+" on node: "+node);
				List<String> result = SSHService.sendCommand(log, node.getSshConnection(), startupCommand);

				x++;
			
			}

		log.info(this.getClass(), "\n");
	}





	public Log getLog() {
		return log;
	}





	public void setLog(Log log) {
		this.log = log;
	}
	
	////////////////////////////////////////////////////////////////////////////
	
//	log.info(this.getClass(), "checking if basetable has been filled...");
//	boolean baseTableFilled=false;
	
//	
//	long firstMeasure = new Date().getTime();
//	while(!baseTableFilled){
//		
//		baseTableFilled = true;
//		log.info(this.getClass(), "---------------------------");
//		for (Node node : NetworkConfig.CLIENTS) {
//			
//			boolean processFinished = checkProcesesFinished(node, "basetable-filled");
//			log.info(this.getClass(), "node: "+node.getIpAddress()+", status: "+processFinished);
//			if(!processFinished)baseTableFilled=false;
//		}
//		
//
//		long currentTime2;
//			
//		currentTime2 = new Date().getTime();
//		if(EvaluationConfig.CLIENT_FILLTABLESTIMEOUT != 0 && (currentTime2 - firstMeasure) > EvaluationConfig.CLIENT_FILLTABLESTIMEOUT){
//			log.info(this.getClass(), "filling database process failed....");
//			break;
//		}
//		
//		
//		try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//	}
//	log.info(this.getClass(), "base table filled in "+((new Date().getTime() - currentTime)/1000)+" sec");


}
