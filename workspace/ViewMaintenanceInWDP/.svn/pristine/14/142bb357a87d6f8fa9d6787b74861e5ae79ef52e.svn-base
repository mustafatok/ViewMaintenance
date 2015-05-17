package de.webdataplatform.regionserver;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.client.Client;
import de.webdataplatform.log.Log;
import de.webdataplatform.log.StatisticLog;
import de.webdataplatform.message.Server;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.view.TableService;
import de.webdataplatform.view.ViewDefinitions;
import de.webdataplatform.viewmanager.ServerMessageHandler;
import de.webdataplatform.viewmanager.ServerMessageHandlerFactory;
import de.webdataplatform.viewmanager.ViewManager;
import de.webdataplatform.zookeeper.IZooKeeperService;
import de.webdataplatform.zookeeper.ZookeeperService;

public class RegionServer implements Runnable{

	
	private WALReader walReader;
	
	private UpdateAssigner updateAssigner;
	
	private UpdateDistributor updateDistributor;
	
	
	
	private Queue<BaseTableUpdate> incomingUpdates;
	
	private RSController rsController;
	
	
	private SystemID systemID;
	
	
	private IZooKeeperService zooKeeperService;
	
	
	private Server messageServer ;
	
	private Queue<String> incomingMessages;
	
//	private Map<String, Integer> statusReport;
			
	private SystemID master;
	
	
	private Log log;
	
	public RegionServer() {
		super();
	}

	public RegionServer(Log log, String name, String ip, int messagePort){
		
        this.incomingUpdates = new ConcurrentLinkedQueue<BaseTableUpdate>();
        updatesSent= new AtomicLong();
		systemID = new SystemID(name, ip, messagePort);
//        this.statusReport = new HashMap<String, Integer>();
        master = new SystemID();
       this.log = log;
        
	}
	
	public void initialize(){
		
//		log = new Log(systemID.getName()+".log");
        StatisticLog.name = systemID.getName();
        StatisticLog.targetDirectory = "logs/";
        
        log.info(this.getClass(),"initializing new RegionServer with config");
        log.info(this.getClass(),"name: "+systemID.getName());
        log.info(this.getClass(),"address: "+systemID.getIp());
        log.info(this.getClass(),"port: "+systemID.getMessagePort());
        log.info(this.getClass(),"updates in queue: "+incomingUpdates.size());
        
		/** establishing server for inter-component communication */
		
		incomingMessages = new ConcurrentLinkedQueue<String>();
		
		messageServer = new Server(new ServerMessageHandlerFactory(log, incomingMessages), systemID.getMessagePort());
		
//		messageServer.start(messagePort);
		
        
		/** start region server components*/
		

		ViewDefinitions getViews = new ViewDefinitions(log, "viewdefinitions");
		getViews.loadViewDefinitions();
		
		log.info(this.getClass(),"base tables with defined views: "+getViews.getBasetablesWithView());
		
		updatesAssinged = new AtomicLong();
		
		walReader = new WALReader(log, systemID.getIp(),systemID.getName(), getViews, incomingUpdates);
		
        updateAssigner = new UpdateAssigner(log, new ArrayList<ViewManager>());
        
        log.info(this.getClass(),"number of view managers on hash ring: "+updateAssigner.numOfVms());
        
        updateDistributor = new UpdateDistributor(log, systemID, updatesSent);
        
        rsController = new RSController(log, this);
        
        
        zooKeeperService = new ZookeeperService(log);
        
        log.info(this.getClass(),"initializing controller ");
        
        Thread trsm = new Thread(rsController);
        
        trsm.start();
        
	}
	

	private long lastMeasure = new Date().getTime();
	private AtomicLong updatesAssinged;
	private AtomicLong updatesSent;
	boolean finishMarkerReceived = false;
	
	@Override
	public void run() {

		
		
		while(true){
			
			try{
			
			long currentTime = new Date().getTime();
			
			if((currentTime - lastMeasure) > SystemConfig.REGIONSERVER_DISPLAYINTERVAL){
				lastMeasure = currentTime;
				log.info(this.getClass()," ---------------------");
				log.info(this.getClass(),"updatesEnteredSystem: "+walReader.getUpdatesEnteredSystem().get());
				log.info(this.getClass(),"incoming queue: "+incomingUpdates.size());
				log.info(this.getClass(),"updates assigned :"+updatesAssinged.get());
				log.info(this.getClass(),"updates sent: "+updatesSent);
				log.info(this.getClass(),"updatesLeftSystem: "+rsController.updatesLeftSystem());
				log.info(this.getClass()," ---------------------"); 
			}
			

			
//			long overalltime1 = new Date().getTime();
			
			int numOfVms = updateAssigner.numOfVms();
			if(numOfVms != 0 && updateDistributor.getMaxQueueSize() < 5000){
			
					BaseTableUpdate btupdate = incomingUpdates.poll();
					if(btupdate != null){
						
//						if(btupdate.getKey().contains("finishmarker") && !finishMarkerReceived){
//							
//							finishMarkerReceived = true;
//							log.info(this.getClass(),"finish marker received:"+btupdate.getKey());
//							log.info(this.getClass(),"sending finish marker to view managers:");
//							rsController.sendMarker(SystemConfig.MESSAGES_MARKERPREFIX+"finish");
//							
//		
//							
//						}
						
//						if(!btupdate.getKey().contains("finishmarker")){
							
							ViewManager viewManager = updateAssigner.assignUpdate(btupdate.getKey());
							updatesAssinged.incrementAndGet();

							try {
								updateDistributor.queueUpdate(viewManager, btupdate.convertToString());
								
							} catch (NoQueueForViewManagerException e) {
							
								log.error(this.getClass(),e);
							} 
//						}
							

						
					}
			}
			
//			Log.info(this.getClass(), "Overall time: "+(new Date().getTime()-overalltime1));
//			
//			if(incomingUpdates.size() == 0){
//				try {
//					Thread.sleep(0, 100000);
//				} catch (InterruptedException e) {
//	
//					e.printStackTrace();
//				}
//			}
			
			
			}catch(Exception e){
				log.error(this.getClass(), e);
			}
			
		}
		
		
		
	}


	public UpdateAssigner getUpdateAssigner() {
		return updateAssigner;
	}

	public void setUpdateAssigner(UpdateAssigner updateAssigner) {
		this.updateAssigner = updateAssigner;
	}

	public UpdateDistributor getUpdateDistributor() {
		return updateDistributor;
	}

	public void setUpdateDistributor(UpdateDistributor updateDistributor) {
		this.updateDistributor = updateDistributor;
	}

	public Queue<BaseTableUpdate> getIncomingUpdates() {
		return incomingUpdates;
	}

	public void setIncomingUpdates(Queue<BaseTableUpdate> incomingUpdates) {
		this.incomingUpdates = incomingUpdates;
	}

	public RSController getRsController() {
		return rsController;
	}

	public void setRsController(RSController rsController) {
		this.rsController = rsController;
	}

	public Server getMessageServer() {
		return messageServer;
	}

	public void setMessageServer(Server messageServer) {
		this.messageServer = messageServer;
	}

	public Queue<String> getIncomingMessages() {
		return incomingMessages;
	}

	public void setIncomingMessages(Queue<String> incomingMessages) {
		this.incomingMessages = incomingMessages;
	}




	public IZooKeeperService getZooKeeperService() {
		return zooKeeperService;
	}

	public void setZooKeeperService(IZooKeeperService zooKeeperService) {
		this.zooKeeperService = zooKeeperService;
	}

	public SystemID getSystemID() {
		return systemID;
	}

	public void setSystemID(SystemID systemID) {
		this.systemID = systemID;
	}
//
//	public Map<String, Integer> getStatusReport() {
//		return statusReport;
//	}
//
//	public void setStatusReport(Map<String, Integer> statusReport) {
//		this.statusReport = statusReport;
//	}

	public SystemID getMaster() {
		return master;
	}

	public void setMaster(SystemID master) {
		this.master = master;
	}

	public WALReader getWalReader() {
		return walReader;
	}

	public void setWalReader(WALReader walReader) {
		this.walReader = walReader;
	}

	public AtomicLong getUpdatesAssinged() {
		return updatesAssinged;
	}

	public void setUpdatesAssinged(AtomicLong updatesAssinged) {
		this.updatesAssinged = updatesAssinged;
	}



	public AtomicLong getUpdatesSent() {
		return updatesSent;
	}

	public void setUpdatesSent(AtomicLong updatesSent) {
		this.updatesSent = updatesSent;
	}

	
	
	

}
