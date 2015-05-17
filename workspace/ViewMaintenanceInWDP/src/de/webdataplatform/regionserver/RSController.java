package de.webdataplatform.regionserver;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.log.StatisticLog;
import de.webdataplatform.message.Message;
import de.webdataplatform.message.MessageClient;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.system.Command;
import de.webdataplatform.system.Component;
import de.webdataplatform.system.Event;
import de.webdataplatform.view.TableService;
import de.webdataplatform.viewmanager.ViewManager;

public class RSController implements Runnable{

	
	private RegionServer regionServer;
	
	private Log log;
	
	private Map<String, Set<ViewManager>> markers=new HashMap<String, Set<ViewManager>>();
	
	
	public RSController(Log log, RegionServer regionServer) {
		super();
		this.regionServer = regionServer;
		this.log = log;
	}


	public void initialize() throws Exception{
		
		
		log.info(RegionServer.class,"Starting region server: name:"+regionServer.getSystemID().getName()+", ip:"+regionServer.getSystemID().getIp()+", message port:"+regionServer.getSystemID().getMessagePort());
		
		log.info(RegionServer.class,"connecting to zookeeper on address: "+NetworkConfig.ZOOKEEPER_QUORUM+":"+NetworkConfig.ZOOKEEPER_CLIENTPORT);
		regionServer.getZooKeeperService().startup();
		
		List<String> result = regionServer.getZooKeeperService().getChildren(SystemConfig.MASTER_ZOOKEEPERPATH);
		if(result.size() == 1){
			regionServer.setMaster(new SystemID(result.get(0)));
			log.info(RegionServer.class,"master found at: "+regionServer.getMaster());
			
		}
		
		log.info(RegionServer.class,"creating session node");
		boolean created = regionServer.getZooKeeperService().createSessionNode(SystemConfig.REGIONSERVER_ZOOKEEPERPATH+"/"+regionServer.getSystemID().toString());
		
		if(!created)throw new Exception("Zookeeper Session node could not be created");
		
		log.info(this.getClass(),"starting message server on port "+regionServer.getSystemID().getMessagePort());
		regionServer.getMessageServer().start();
		
		log.info(this.getClass(),"starting wal reader ");
		Thread t = new Thread(regionServer.getWalReader());
		
		t.start();
		
		log.info(this.getClass(),"starting update processing ");
		Thread trs = new Thread(regionServer);
		
		trs.start();
		
		
		
	}
	
	
	public void sendMessage(String ip, int port, Message message){
		
		
		MessageClient.send(log, ip, port, message);
		
		
	}
	
	
	public void sendMessageToSystemID(String systemID, Message message){
		
		
		SystemID masterID = new SystemID(systemID);
		
		sendMessage(masterID.getIp(), masterID.getMessagePort(), message);	
		
		
	}
	
	
	
	public void assignViewManager(String viewManagerId) throws InterruptedException{
		
		log.info(this.getClass(),"assigning view manager");
			
		SystemID systemID = new SystemID(viewManagerId);

		ViewManager viewManager = new ViewManager(systemID.getName(), systemID.getIp(),systemID.getUpdatePort(), systemID.getMessagePort());
		
		Queue<String> updateQueue = regionServer.getUpdateDistributor().addQueue(viewManager);
	
		regionServer.getUpdateAssigner().addViewManager(viewManager);

		if(regionServer.getUpdateAssigner().numOfVms() <= 1){
			
			log.info(this.getClass(),"num of view manager <= 1, starting sending thread instantly");
			regionServer.getUpdateDistributor().startSendingThread(viewManager);
			
		}else{
			
			log.info(this.getClass(),"num of view manager > 1, queuing markers");
			sendMarker(SystemConfig.MESSAGES_MARKERPREFIX+"assign"+viewManagerId);
		}
		
		

	}
	
//	public void assignViewManager(String viewManagerId) throws InterruptedException{
//		
//		log.info(this.getClass(),"assigning view manager");
//			
//		SystemID systemID = new SystemID(viewManagerId);
//
//		ViewManager viewManager = new ViewManager(systemID.getName(), systemID.getIp(),systemID.getUpdatePort(), systemID.getMessagePort());
//		
//		Queue<String> updateQueue = regionServer.getUpdateDistributor().addQueue(viewManager);
//	
//		regionServer.getUpdateDistributor().startSendingThread(viewManager, updateQueue);
//
//		regionServer.getUpdateAssigner().addViewManager(viewManager);
//	
//
//	}
	
	public String generateMarker(){
		
		Set<String> markerKeys = markers.keySet();
	
		Integer highestMarker=Integer.MIN_VALUE;
		for(String marker : markerKeys){
			
			Integer markerCount = Integer.parseInt(marker.replace(SystemConfig.MESSAGES_MARKERPREFIX, ""));
			if(markerCount > highestMarker){
				highestMarker = markerCount;
			}
			
		}
		if(highestMarker == Integer.MIN_VALUE)highestMarker = 0;
		
		String newMarker = SystemConfig.MESSAGES_MARKERPREFIX+(highestMarker+1);
		
		return newMarker;

	}
	public void removeMarker(ViewManager viewManager){
		
		for(String marker : markers.keySet()){
			
			if(markers.get(marker).contains(viewManager)){
				
				markers.get(marker).remove(viewManager);
				
			}
			
		}
	}
	
	public void sendMarker(String marker){
		
		
//		String marker = generateMarker();
		
		if(marker == null)marker = generateMarker();
		
		Set<ViewManager> viewManagers = regionServer.getUpdateAssigner().getViewManager();
		
		if(marker.contains("assign")){
			
			String viewManagerId = marker.split("assign")[1];
			SystemID systemID = new SystemID(viewManagerId);
			ViewManager viewManager = new ViewManager(systemID.getName(), systemID.getIp(),systemID.getUpdatePort(), systemID.getMessagePort());
			viewManagers.remove(viewManager);
		}	
		
		markers.put(marker, viewManagers);
		
		log.info(this.getClass(),"queuing markers:"+marker+"to vms:"+viewManagers);
		
		for (ViewManager viewManager : viewManagers) {
		
			
				BaseTableUpdate btu = new BaseTableUpdate(marker, regionServer.getSystemID().toString(),"region", "m1", "m1","m2","m3", new HashMap<String, String>(), new HashMap<String, String>(), new HashMap<String, String>());
	
				
				try {
					regionServer.getUpdateDistributor().queueUpdate(viewManager, btu.convertToString());
				} catch (NoQueueForViewManagerException e) {
				   log.error(this.getClass(), e);
				} 
			
		}
		
	}
	

	
	public void markerReceived(String viewManagerID, String marker){
		
		
		Set<ViewManager> viewManagers = markers.get(marker);
		
		SystemID systemID = new SystemID(viewManagerID);

		ViewManager viewManager = new ViewManager(systemID.getName(), systemID.getIp(),systemID.getUpdatePort(), systemID.getMessagePort());
		
		viewManagers.remove(viewManager);
		
		if(viewManagers.isEmpty()){
			log.info(this.getClass(),"all answers received for marker: "+marker);
			
//			if(marker.contains("finish")){
//				log.info(this.getClass(),"update process finished, informing Master");
//				
//				TableService tableService = new TableService(log);
//				
//				Map<byte[],byte[]> cols = new HashMap<byte[], byte[]>();
//				cols.put(Bytes.toBytes("rs"), Bytes.toBytes("rs"));
//				tableService.put(Bytes.toBytes("finish_markers"), Bytes.toBytes(regionServer.getSystemID().getName()), cols);
//			}
//			
			if(marker.contains("assign")){
				
				
				String viewManagerId = marker.split("assign")[1];
				
				log.info(this.getClass(),"markers received for assigning view manager:"+viewManagerId);
				
				systemID = new SystemID(viewManagerId);

				viewManager = new ViewManager(systemID.getName(), systemID.getIp(),systemID.getUpdatePort(), systemID.getMessagePort());
				
				try {
					
					log.info(this.getClass(),"starting sending thread of view manager:"+viewManagerId);
					regionServer.getUpdateDistributor().startSendingThread(viewManager);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
//
//			Message message = new Message(Component.regionServer, regionServer.getSystemID().toString(), Event.PROCESS_FINISHED, "");
//			
//			sendMessage(regionServer.getMaster().getIp(), regionServer.getMaster().getMessagePort(), message);
//			
			
		}
		
		
	}

	
	
	
	public void withdrawViewManager(String viewManagerId) throws InterruptedException{
		
		log.info(this.getClass(),"withdrawing view manager");
	
		SystemID systemID = new SystemID(viewManagerId);

		ViewManager viewManager = new ViewManager(systemID.getName(), systemID.getIp(),systemID.getUpdatePort(), systemID.getMessagePort());
		
		removeMarker(viewManager);
		
		regionServer.getUpdateAssigner().removeViewManager(viewManager);
		
		regionServer.getUpdateDistributor().stopSendingThread(viewManager);
		
		log.info(this.getClass(),"sending thread stopped");
		
		regionServer.getUpdateDistributor().emptyQueue(viewManager);
		
//		List<String> updates = regionServer.getUpdateDistributor().emptyQueue(viewManager);
//		
//		if(updates.size() > 0)log.info(this.getClass(),"last update: "+updates.get(0));
//
//		log.info(this.getClass(),"queue empty");
		
//		log.info(this.getClass(),"reassigning "+updates.size()+" updates");
//
//		for(String update : updates){
//			regionServer.getIncomingUpdates().add(new BaseTableUpdate(log, update));
//		}
		
		
//		try {
			
		regionServer.getUpdateDistributor().removeQueue(viewManager);
		
		log.info(this.getClass(),"queue removed");
//		} catch (QueueNotEmptyException e) {
//			
//			e.printStackTrace();
//		}

	}
	
	public void replayWriteAheadLog(Map<String, Long> seqNo) throws InterruptedException{
		
		log.info(this.getClass(),"replay wal");

		regionServer.getWalReader().replayWAL(seqNo);
		
	}	
	
	public void writeAheadLogReplayed() throws InterruptedException{
		
		log.info(this.getClass(),"Write Ahead Log replayed");
			
		Message message = new Message(Component.regionServer, regionServer.getSystemID().toString(), Event.WRITEAHEADLOG_REPLAYED, regionServer.getSystemID().toString());
		
		sendMessageToSystemID(regionServer.getMaster().toString(), message);
		

	}
	
	public void viewManagerAssigned(String viewManager) throws InterruptedException{
		
		log.info(this.getClass(),"view manager assigned");
			
		Message message = new Message(Component.regionServer, regionServer.getSystemID().toString(), Event.VIEWMANAGER_ASSIGNED, regionServer.getSystemID().toString());
		
		sendMessageToSystemID(viewManager, message);

	}
	
	public void viewManagerWithdrawn(String viewManager) throws InterruptedException{
		
		log.info(this.getClass(),"view manager withdrawn");
			
		Message message = new Message(Component.regionServer, regionServer.getSystemID().toString(), Event.VIEWMANAGER_WITHDRAWN, regionServer.getSystemID().toString());
		
		sendMessageToSystemID(viewManager, message);
		

	}
	
	public void crashedViewManagerWithdrawn(String viewManager) throws InterruptedException{
		
		log.info(this.getClass(),"crashed view manager withdrawn");
			
		Message message = new Message(Component.regionServer, regionServer.getSystemID().toString(), Event.CRASHED_VIEWMANAGER_WITHDRAWN, viewManager);
		
		sendMessageToSystemID(regionServer.getMaster().toString(), message);
		

	}
	
	
	private Map<String, Integer> statusReports=new HashMap<String, Integer>();
	
	private long lastMeasure = new Date().getTime();
	
	boolean updateProcessStarted=false;
	private long timeSpan;
	boolean updateProcessFinished=false;
	

	private long lastMeasureStatistic = new Date().getTime();
	private long lastUpdatesAssignedStatistics;
	
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
			
			/**send status report to master*/
			if((currentTime - lastMeasure) > SystemConfig.REGIONSERVER_STATUSINTERVAL){
				
				lastMeasure = currentTime;
				
				long report = updatesLeftSystem();
				
				report = regionServer.getWalReader().getUpdatesEnteredSystem().get() - report;
				
				log.info(this.getClass(),"sending status report to master:"+regionServer.getMaster());
				
				Message message = new Message(Component.regionServer, regionServer.getSystemID().toString(), Event.STATUS_REPORT_REGIONSERVER, report+"");
				
				sendMessage(regionServer.getMaster().getIp(), regionServer.getMaster().getMessagePort(), message);
				
			}
			
			/**collect statistics*/
			if((currentTime - lastMeasureStatistic) > SystemConfig.REGIONSERVER_CALCSTATISTICSINTERVAL){
				
				List<String> statisticValues = new ArrayList<String>();
				
				
				statisticValues.add(regionServer.getWalReader().getUpdatesRetrieved().get()+"");
				statisticValues.add(regionServer.getUpdatesAssinged().get()+"");
				statisticValues.add(regionServer.getUpdatesSent().get()+"");

				statisticValues.add(regionServer.getIncomingUpdates().size()+"");
				statisticValues.add((regionServer.getUpdatesAssinged().get()-lastUpdatesAssignedStatistics)+"");
				statisticValues.add(timeSpan+"");
				
				lastUpdatesAssignedStatistics = regionServer.getUpdatesAssinged().get();
				
				StatisticLog.info(statisticValues);
				lastMeasureStatistic = currentTime;
				
			}
			
//			/**check if update process has been started*/
//			if(regionServer.getUpdatesAssinged().get() > 0){
//				
//				updateProcessStarted=true;
//				startingTime = currentTime;
//			}

		
//			/**check if update process has been finished*/
//			if(updateProcessStarted && !updateProcessFinished && EvaluationConfig.REGIONSERVER_UPDATESTIMEOUT != 0 && (currentTime - lastMeasureUpdates) > EvaluationConfig.REGIONSERVER_UPDATESTIMEOUT){
//				
//				
//				if(lastUpdatesAssigned == regionServer.getUpdatesAssinged().get()){
//					
//					sendMarker();
//					updateProcessFinished = true;
//					timeSpan = currentTime - startingTime;
//				
//				}
//				lastMeasureUpdates = currentTime;
//				lastUpdatesAssigned = regionServer.getUpdatesAssinged().get();
//			}
//			if(!updateProcessFinished && !updateProcessStarted && EvaluationConfig.REGIONSERVER_OVERALLTIMEOUT != 0 && (currentTime - firstMeasure) > EvaluationConfig.REGIONSERVER_OVERALLTIMEOUT ){
//
//				updateProcessFinished = true;
//				
//				log.info(this.getClass(),"received no updates, update process finished, informing Master");
//
//				Message message = new Message(Component.regionServer, regionServer.getSystemID().toString(), Event.PROCESS_FINISHED, "");
//				
//				sendMessage(regionServer.getMaster().getIp(), regionServer.getMaster().getMessagePort(), message);
//			}
			
			
			String messageString = regionServer.getIncomingMessages().poll();
			
			
			/**process incoming messages*/
			if(messageString != null){
				
				Message message = new Message(messageString);
				
				log.message(this.getClass(),"receiving message: "+message);


				
//				DISPATCH TODO
				
				String component = message.getComponent();
				String operation = message.getOperation();
				
				switch(component){
				
					case Component.master : 
						switch(operation){
						case Command.WITHDRAW_CRASHED_VIEWMANAGER: 
							log.info(this.getClass(),"withdraw view manager: "+message.getContent());
							try {
								withdrawViewManager(message.getContent());
								crashedViewManagerWithdrawn(message.getContent());
							} catch (InterruptedException e) {
			
								log.error(this.getClass(), e);
							}
						break;
						case Command.REPLAY_WRITEAHEADLOG: 
							log.info(this.getClass(),"replaying write ahead log: "+message.getContent());
							try {
								
								
								String msg = message.getContent().replace("{", "").replace("}", "").replace(" ", "");
								
								log.info(this.getClass(),"replay msg: "+msg);
								Map<String, Long> seqNos = new HashMap<String, Long>();
								
								for (String keyValue : msg.split(",")) {
									
									seqNos.put(keyValue.split("=")[0], Long.parseLong(keyValue.split("=")[1]));
								}
								log.info(this.getClass(),"seqNos: "+seqNos);
								
								replayWriteAheadLog(seqNos);
								
								writeAheadLogReplayed();
								
							} catch (InterruptedException e) {
			
								log.error(this.getClass(), e);
							}
						break;
						}
					break;
					case Component.regionServer : 
					break;				
					case Component.viewManager : 
						switch(operation){
						case Event.STATUS_REPORT_VIEWMANAGER : 
							log.info(this.getClass(),"status report from "+message.getName()+":"+message.getContent());
							statusReports.put(message.getName(), Integer.parseInt(message.getContent()));
						break;	
						case Event.VIEWMANAGER_MARKER_RECEIVED : 

							log.info(this.getClass(),"marker received from "+message.getName()+":"+message.getContent());
							markerReceived(message.getName(), message.getContent());
							
						break;
						case Command.ASSIGN_VIEWMANAGER : 
							log.info(this.getClass(),"assign view manager: "+message.getName());
							try {
								assignViewManager(message.getContent());
								viewManagerAssigned(message.getContent());
							} catch (InterruptedException e) {
	
								log.error(this.getClass(), e);
							}
						break;	
						case Command.WITHDRAW_VIEWMANAGER : 
							log.info(this.getClass(),"withdraw view manager: "+message.getContent());
							try {
								withdrawViewManager(message.getContent());
								viewManagerWithdrawn(message.getContent());
							} catch (InterruptedException e) {
			
								log.error(this.getClass(), e);
							}
						break;
						}	
					
				}
				
				
			}
			try {
				Thread.sleep(SystemConfig.MESSAGES_POLLINGINTERVAL);
			} catch (InterruptedException e) {
				log.error(this.getClass(), e);
//				e.printStackTrace();
			}
			
			
			}catch(Exception e){
				log.error(this.getClass(), e);
			}
			
		}

		
	}


	public long updatesLeftSystem() {
		long report =0;
		for (String statusReportViewManager : statusReports.keySet()) {
			
			report += statusReports.get(statusReportViewManager);
			
		}
		return report;
	}



	

}
