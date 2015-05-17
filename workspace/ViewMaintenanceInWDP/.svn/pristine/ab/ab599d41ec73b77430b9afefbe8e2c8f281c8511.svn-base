package de.webdataplatform.viewmanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import de.webdataplatform.log.Log;
import de.webdataplatform.log.StatisticLog;
import de.webdataplatform.message.Message;
import de.webdataplatform.message.MessageClient;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.system.Command;
import de.webdataplatform.system.Component;
import de.webdataplatform.system.Event;

public class VMController implements Runnable{

	
	

	
	private ViewManager viewManager;
	
	private Log log; 

	public VMController(Log log, ViewManager viewManager) {
		
		
		super();

		this.viewManager = viewManager;
		this.log = log;
	}


	
	
	
	public void initialize() throws Exception{
	
		viewManager.setCondition(Condition.INITIALIZING);
		
		/** register at Zookeeper */
		
		log.info(this.getClass(),"connecting to zookeeper on address: "+NetworkConfig.ZOOKEEPER_QUORUM+":"+NetworkConfig.ZOOKEEPER_CLIENTPORT);
		viewManager.getZooKeeperService().startup();
		
		boolean created=false;
		
//		do{
		try{
			log.info(this.getClass(),"creating session node");
			created = viewManager.getZooKeeperService().createSessionNode(SystemConfig.VIEWMANAGER_ZOOKEEPERPATH+"/"+viewManager.getSystemID().toString());
			log.info(this.getClass(),"session node created: "+created);
		}catch(Exception e){
			log.error(this.getClass(), e);
		}

//		}while(!created);
		
//		if(!created)throw new Exception("Zookeeper Session node could not be created");
		List<String> result = viewManager.getZooKeeperService().getChildren(SystemConfig.MASTER_ZOOKEEPERPATH);
		if(result.size() == 1){
			viewManager.setMaster(new SystemID(result.get(0)));
			log.info(this.getClass(),"master found at: "+viewManager.getMaster());
			
		}
		
		log.info(this.getClass(),"starting message server on port:"+viewManager.getSystemID().getMessagePort());
		viewManager.getMessageServer().start();
		
		log.info(this.getClass(),"starting update server on port:"+viewManager.getSystemID().getUpdatePort());
		viewManager.getUpdateServer().start();
		
		
		if(SystemConfig.FAULTTOLERANCE_COMMITLOG){
			
			viewManager.getProcessing().getCommitLog().createLogDirectory();
			viewManager.getProcessing().getCommitLog().createLogFile();
		}
		
		log.info(this.getClass(),"start pre-processing udpates");
		Thread tpp = new Thread(viewManager.getPreProcessing());
		
		log.info(this.getClass(),"start processing udpates");
		Thread tp = new Thread(viewManager.getProcessing());
		
		tpp.start();
		tp.start();
		
		viewManager.setCondition(Condition.RUNNING);
		
		
	}



	public void sendMessage(String ip, int port, Message message){
		
		
		MessageClient.send(log, ip, port, message);
		
		
	}
	
	public void sendMessageToSystemID(String systemID, Message message){
		
		
		SystemID masterID = new SystemID(systemID);
		
		sendMessage(masterID.getIp(), masterID.getMessagePort(), message);	
		
		
	}
	
	

	

//	VIEW MANAGER CALLS
	
	

	
	public void assignViewManager(String regionServer, String viewManager){
		
		log.info(this.getClass(),"sending assign vm:"+viewManager+" to rs:"+regionServer);
		
		Message message = new Message(Component.viewManager, viewManager, Command.ASSIGN_VIEWMANAGER, viewManager);
		
		sendMessageToSystemID(regionServer, message);
		
	}
	
	public void withdrawViewManager(String regionServer, String viewManager){
		
		log.info(this.getClass(),"sending withdraw vm:"+viewManager+" to rs:"+regionServer);
		
		Message message = new Message(Component.viewManager, viewManager, Command.WITHDRAW_VIEWMANAGER, viewManager);
		
		sendMessageToSystemID(regionServer, message);
		
	}
	
	
	
	
	public void viewManagerAssigned(String master, String regionServer, String viewManager){
		
		
		this.viewManager.setRegionServer(new SystemID(regionServer));
		
		log.info(this.getClass(),"sending vm assigned to master:"+master);
		
		Message message = new Message(Component.viewManager, viewManager, Event.VIEWMANAGER_ASSIGNED, regionServer);
		
		sendMessageToSystemID(master, message);

		
	}
	
	
	public void viewManagerWithdrawn(String master, String viewManager){
		
			
			this.viewManager.setRegionServer(null);
			
			this.viewManager.setCondition(Condition.RUNNING);
	
			log.info(this.getClass(),"sending vm withdrawn to master:"+master);
			
			Message message = new Message(Component.viewManager, viewManager, Event.VIEWMANAGER_WITHDRAWN, "");
			
			sendMessageToSystemID(master, message);
		
	}
	
	
	public void viewManagerReassigned(String master, String regionServer, String viewManager){
		
		
		this.viewManager.setRegionServer(new SystemID(regionServer));
		
		log.info(this.getClass(),"sending vm reassigned to master:"+master);
		
		Message message = new Message(Component.viewManager, viewManager, Event.VIEWMANAGER_REASSIGNED, regionServer);
		
		sendMessageToSystemID(master, message);

		
	}
	

	
	public void viewManagerShutdown(String master, String viewManager){
		
		log.info(this.getClass(),"sending vm shutdown to master:"+master);
		
		Message message = new Message(Component.viewManager, viewManager, Event.VIEWMANAGER_SHUTDOWN, "");
		
		sendMessageToSystemID(master, message);
		
		log.info(this.getClass(),"system going down");
		
		System.exit(0);
		
	}	
	

	
	
	private String reassignRS = "";
	private long lastMeasure = new Date().getTime();
	private long lastMeasureStatistic = new Date().getTime();
	private long lastViewRecordUpdates;
	
	boolean updateProcessStarted=false;
	private long startingTime;
	private long timeSpan;
	boolean updateProcessFinished=false;

	@Override
	public void run() {

		try {
			initialize();
		} catch (Exception e) {
			
			log.error(this.getClass(), e);

		}
		
		
		while(true){
			
			try{
			
			long currentTime = new Date().getTime();
			
			
			/**send status report*/
			if((currentTime - lastMeasure) > SystemConfig.VIEWMANAGER_STATUSINTERVAL && viewManager.getRegionServer() != null){
				
//				viewManager.getRegionServer() != null
				lastMeasure = currentTime;
				
				log.info(this.getClass(),"sending status report to rs:"+viewManager.getRegionServer());
				
				Message message = new Message(Component.viewManager, viewManager.getSystemID().toString(), Event.STATUS_REPORT_VIEWMANAGER, viewManager.getProcessing().getCommitLogUpdates().get()+"");

//				SystemID rsSystemID = new SystemID(viewManager.getRegionServer());
				
				sendMessage(viewManager.getRegionServer().getIp(), viewManager.getRegionServer().getMessagePort(), message);
				
			}
			
			/**collect statistics*/
			if((currentTime - lastMeasureStatistic) > SystemConfig.VIEWMANAGER_CALCSTATISTICINTERVAL && viewManager.getRegionServer() != null){
				
				List<String> statisticValues = new ArrayList<String>();
				
				
				statisticValues.add(viewManager.getUpdatesReceived().get()+"");
				statisticValues.add(viewManager.getPreProcessing().getUpdatesPreProcessed().get()+"");
				statisticValues.add(viewManager.getProcessing().getViewRecordUpdates().get()+"");
				statisticValues.add(viewManager.getProcessing().getCommitLogUpdates().get()+"");
				statisticValues.add(viewManager.getIncomingUpdates().size()+"");
				statisticValues.add(viewManager.getPreprocessedUpdates().size()+"");
				statisticValues.add((viewManager.getProcessing().getViewRecordUpdates().get()-lastViewRecordUpdates)+"");
				statisticValues.add(timeSpan+"");
				lastViewRecordUpdates = viewManager.getProcessing().getViewRecordUpdates().get();
				
				
				StatisticLog.info(statisticValues);
				lastMeasureStatistic = currentTime;
				
			}
			
//			/**check if update process has been started*/
			if(viewManager.getUpdatesReceived().get() > 0 && !updateProcessStarted){
				
				updateProcessStarted=true;
				startingTime = currentTime;
			}
			
			/**check if update process has been finished*/
			if(!viewManager.getProcessing().getMarkers().keySet().isEmpty()){
				
				
				Set<String> keys = viewManager.getProcessing().getMarkers().keySet();
				
				for(String marker : keys){

					log.info(this.getClass(),"marker: "+marker+" received sending answer to :"+viewManager.getProcessing().getMarkers().get(marker));
					
					if(marker.contains("finish")){
						
						log.info(this.getClass(),"finish marker received, timespan=:"+timeSpan);
						timeSpan = currentTime - startingTime;
					}
					
					
					
					Message sendMarkerMessage = new Message(Component.viewManager, viewManager.getSystemID().toString(), Event.VIEWMANAGER_MARKER_RECEIVED, marker);
					
					sendMessageToSystemID(viewManager.getProcessing().getMarkers().get(marker), sendMarkerMessage);
				}
				for (String string : keys) {
					viewManager.getProcessing().getMarkers().remove(string);
				}
			
				
				
			}
			
			/**receive incoming messages*/
			String messageString = viewManager.getIncomingMessages().poll();
			
			if(messageString != null){
				
				Message message = new Message(messageString);
				
				log.message(this.getClass(),"received message: "+message);
				
//				DISPATCH TODO
				
				String component = message.getComponent();
				String operation = message.getOperation();
				String content = message.getContent();

				
				switch(component){
				
					case Component.master : 
						if(this.viewManager.getCondition().equals(Condition.RUNNING)){
							switch(operation){
								case Command.ASSIGN_VIEWMANAGER : 
									this.viewManager.setCondition(Condition.ASSIGNING);
									assignViewManager(content, this.viewManager.getSystemID().toString());
								break;	
								case Command.WITHDRAW_VIEWMANAGER : 
									this.viewManager.setCondition(Condition.WITHDRAWING);
									if(this.viewManager.getRegionServer() != null)
									withdrawViewManager(this.viewManager.getRegionServer().toString(), this.viewManager.getSystemID().toString());
									else log.info(this.getClass(),"cannot withdraw, region server not set");
								break; 
								case Command.REASSIGN_VIEWMANAGER :
									this.viewManager.setCondition(Condition.REASSIGNING);
									reassignRS = content;
									if(this.viewManager.getRegionServer() != null)
									withdrawViewManager(this.viewManager.getRegionServer().toString(), this.viewManager.getSystemID().toString());
									else log.info(this.getClass(),"cannot withdraw, region server not set");
								break;
								case Command.SHUTDOWN_VIEWMANAGER : 
									this.viewManager.setCondition(Condition.SHUTTING_DOWN);
									if(this.viewManager.getRegionServer() != null)
									withdrawViewManager(this.viewManager.getRegionServer().toString(), this.viewManager.getSystemID().toString());
									else viewManagerShutdown(viewManager.getMaster().toString(),  viewManager.getSystemID().toString());
								break;
								case Event.VIEWMANAGER_CRASHED : 
									log.info(this.getClass(),"region server crashed :"+message.getContent());

								break;	
							}
						}else{
							log.info(this.getClass(),"cannot execute command from master because condition is:"+viewManager.getCondition());
						}
					break;
					case Component.regionServer : log.info(this.getClass(),"receive message from region server");
						switch(operation){
							case Event.VIEWMANAGER_ASSIGNED : 
								if(viewManager.getCondition().equals(Condition.ASSIGNING)){
									viewManagerAssigned(viewManager.getMaster().toString(), content, viewManager.getSystemID().toString());
									viewManager.setCondition(Condition.RUNNING);
								}
								if(viewManager.getCondition().equals(Condition.REASSIGNING)){
									
									viewManagerReassigned(viewManager.getMaster().toString(), content, viewManager.getSystemID().toString());
									viewManager.setCondition(Condition.RUNNING);
									
								}					
							break;	
							case Event.VIEWMANAGER_WITHDRAWN : 
								if(viewManager.getCondition().equals(Condition.WITHDRAWING)){
									viewManagerWithdrawn(viewManager.getMaster().toString(),  viewManager.getSystemID().toString());
									viewManager.setCondition(Condition.RUNNING);
								}
								if(viewManager.getCondition().equals(Condition.REASSIGNING)){
									assignViewManager(reassignRS, viewManager.getSystemID().toString());
								}
								if(viewManager.getCondition().equals(Condition.SHUTTING_DOWN)){
									viewManagerShutdown(viewManager.getMaster().toString(),  viewManager.getSystemID().toString());
						
								}
							break;

						}
					break;				

				}
				

				
				
			}
			try {
				Thread.sleep(SystemConfig.MESSAGES_POLLINGINTERVAL);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			}
		
			}catch(Exception e){
				log.error(this.getClass(), e);
			}
	}
	}
	
	
	
	

}
