package de.webdataplatform.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.message.Server;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.viewmanager.ServerMessageHandler;
import de.webdataplatform.viewmanager.ServerMessageHandlerFactory;
import de.webdataplatform.zookeeper.IZooKeeperService;
import de.webdataplatform.zookeeper.ZookeeperService;

public class Master {

	
	private IZooKeeperService zooKeeperService;
	

	private MasterController masterController;
	
	
	private EventProcessor eventProcessor;
	
	private LoadBalancer loadBalancer;
	
	private RecoveryManager recoveryManager;
	
	private ComponentController componentController;
	
	
	private Server messageServer;
	
	private Queue<String> incomingMessages;
	
	private SystemID systemID;
	
	private MetaData metaData;
	
	private Log log;
	
	


	
	public Master(){
		
		
	}
	
	public Master(String name, String ip, int messagePort){
	
	
		this.systemID = new SystemID(name, ip, messagePort);
		

//		if(!zooKeeperService.nodeExists("/hbase/vm")){
//			System.out.println("creating new view manager node");
//			zooKeeperService.createPersistentNode("/hbase/vm");
//		}
//		metaTable = new MetaTable();
		
	}
	
	public void initialize(){
		
		log = new Log("master.log");
		
		SystemConfig.load(log);
		NetworkConfig.load(log);
		
		log.info(this.getClass(),"name: "+systemID.getName());
		log.info(this.getClass(),"address: "+systemID.getIp());
		log.info(this.getClass(),"port: "+systemID.getMessagePort());
		log.info(this.getClass(),"message polling interval: "+SystemConfig.MESSAGES_POLLINGINTERVAL);
		
		metaData = new MetaData();
		
		incomingMessages = new ConcurrentLinkedQueue<String>();
		
		messageServer = new Server(new ServerMessageHandlerFactory(log, incomingMessages), systemID.getMessagePort());
		
		masterController = new MasterController(log, this);

		componentController = new ComponentController(log, metaData, incomingMessages);
		
		loadBalancer = new LoadBalancer(log, metaData, componentController);
		
		recoveryManager = new RecoveryManager(log, metaData, componentController, loadBalancer);

		eventProcessor = new EventProcessor(log, metaData, loadBalancer, recoveryManager);
		
		
		
		List<String> trigger = new ArrayList<String>();
		
		trigger.add(SystemConfig.REGIONSERVER_ZOOKEEPERPATH);
		
		trigger.add(SystemConfig.VIEWMANAGER_ZOOKEEPERPATH);
		
		zooKeeperService = new ZookeeperService(log, trigger, eventProcessor);
		
		Thread tcontroller = new Thread(masterController);
		
		tcontroller.start();
		
//		zooKeeperService = new ZookeeperService(zooKeeperAddress, this);
		
		
	}

	public IZooKeeperService getZooKeeperService() {
		return zooKeeperService;
	}

	public void setZooKeeperService(IZooKeeperService zooKeeperService) {
		this.zooKeeperService = zooKeeperService;
	}

	public MasterController getMasterController() {
		return masterController;
	}

	public void setMasterController(MasterController masterController) {
		this.masterController = masterController;
	}

	public LoadBalancer getLoadBalancer() {
		return loadBalancer;
	}

	public void setLoadBalancer(LoadBalancer loadBalancer) {
		this.loadBalancer = loadBalancer;
	}

	public RecoveryManager getRecoveryManager() {
		return recoveryManager;
	}

	public void setRecoveryManager(RecoveryManager recoveryManager) {
		this.recoveryManager = recoveryManager;
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




	public SystemID getSystemID() {
		return systemID;
	}

	public void setSystemID(SystemID systemID) {
		this.systemID = systemID;
	}

	public EventProcessor getEventProcessor() {
		return eventProcessor;
	}

	public void setEventProcessor(EventProcessor eventProcessor) {
		this.eventProcessor = eventProcessor;
	}

	public ComponentController getComponentController() {
		return componentController;
	}

	public void setComponentController(ComponentController componentController) {
		this.componentController = componentController;
	}

	public MetaData getMetaData() {
		return metaData;
	}

	public void setMetaData(MetaData metaData) {
		this.metaData = metaData;
	}

	public Log getLog() {
		return log;
	}

	public void setLog(Log log) {
		this.log = log;
	}


	
	
	
	
	
	/**
	@Override
	public void setTriggerRegionServer() {

		zooKeeperService.setTriggerOnChildren("/hbase/rs");
		
		
	}

	@Override
	public void setTriggerViewManager() {
		
		zooKeeperService.setTriggerOnChildren("/hbase/vm");
		
	}
	
	@Override
	public synchronized void processResult(int arg0, String znode, Object arg2, List<String> children) {
		
		
//		System.out.println("arg0: "+arg0);
//		System.out.println("arg1: "+znode);
//		System.out.println("arg2: "+arg2);
//		System.out.println("ProcessResult___________________");
//		System.out.println("children: "+children);
		
		if(znode.equals("/hbase/rs")){
			
			callRegionServerAdded();
		}
		if(znode.equals("/hbase/vm") && children != null){
			
			for(String viewManager : children){
			
				if(!viewManagers.contains(viewManager)){
					
					viewManagers.add(viewManager);
					callViewManagerAdded(viewManager);
				}
			}
			for(String viewManager : viewManagers){
				
				if(!children.contains(viewManager)){
//					System.out.println("ViewManager crashed!!!!!!!!!!!!!!:"+viewManager);
					viewManagers.remove(viewManager);
					callViewManagerCrashed(viewManager);
					
				}
				
				
				
			}
			
		}
	}*/
//
//	@Override
//	public void callRegionServerAdded() {
//		
//		
//		
//		
//	}
//
//	@Override
//	public void callViewManagerAdded(String viewManager) {
//		
//		System.out.println("ViewManager added!!!!!!!!!!!!!!:"+viewManager);
//		
////		List<String> regionServers = zooKeeperService.getChildren("/hbase/rs");
//		
//		
//		
//	}
//
//	@Override
//	public void callRegionServerRemoved() {
//		
//		
//		
//	}
//
//	@Override
//	public void callViewManagerRemoved() {
//		
//		
//	}
//
//	@Override
//	public void callRegionServerCrashed() {
//		
//		
//	}
//
//	@Override
//	public void callViewManagerCrashed(String viewManager) {
//		
//		System.out.println("ViewManager crashed!!!!!!!!!!!!!!:"+viewManager);
//		
//	}
//
//	@Override
//	public void addViewManager() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void assignViewManager() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void withdrawViewManager() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void reassignViewManager() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void removeViewManager() {
//		// TODO Auto-generated method stub
//		
//	}


	
}
