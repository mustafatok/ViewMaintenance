package de.webdataplatform.master;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;

public class MasterController implements Runnable{

	
	private Master master;
	
	private Log log;
	
	public MasterController(Log log, Master master) {
		super();
		this.master = master;
		this.log = log;
//		regionServers = new ArrayList<String>();
//		viewManagers = new ArrayList<String>();
//		assignedViewManagers = new HashMap<String, List<String>>();
	}


	public void initialize()throws Exception{
		
		log.info(this.getClass(),"connecting to zookeeper on address: "+NetworkConfig.ZOOKEEPER_QUORUM+":"+NetworkConfig.ZOOKEEPER_CLIENTPORT);
		master.getZooKeeperService().startup();
		
		if(!master.getZooKeeperService().nodeExists(SystemConfig.MASTER_ZOOKEEPERPATH)){
			
			log.info(this.getClass(),"creating new master node");
			master.getZooKeeperService().createPersistentNode(SystemConfig.MASTER_ZOOKEEPERPATH);
		}
		
		log.info(this.getClass(),"creating session node");
		boolean created = master.getZooKeeperService().createSessionNode(SystemConfig.MASTER_ZOOKEEPERPATH+"/"+master.getSystemID().toString());

		if(!created)throw new Exception("Zookeeper Session node could not be created");
		
		if(master.getZooKeeperService().nodeExists(SystemConfig.REGIONSERVER_ZOOKEEPERPATH)){
			
			log.info(this.getClass(),"deleting region server node");
			master.getZooKeeperService().deleteNode(SystemConfig.REGIONSERVER_ZOOKEEPERPATH);
			
		}
		log.info(this.getClass(),"creating new region server");
		master.getZooKeeperService().createPersistentNode(SystemConfig.REGIONSERVER_ZOOKEEPERPATH);
		master.getZooKeeperService().setTriggerOnChildren(SystemConfig.REGIONSERVER_ZOOKEEPERPATH);
		
		
		if(master.getZooKeeperService().nodeExists(SystemConfig.VIEWMANAGER_ZOOKEEPERPATH)){
			
			log.info(this.getClass(),"deleting old view manager node");
			master.getZooKeeperService().deleteNode(SystemConfig.VIEWMANAGER_ZOOKEEPERPATH);
			
		}
		log.info(this.getClass(),"creating new view manager node");
		master.getZooKeeperService().createPersistentNode(SystemConfig.VIEWMANAGER_ZOOKEEPERPATH);
		master.getZooKeeperService().setTriggerOnChildren(SystemConfig.VIEWMANAGER_ZOOKEEPERPATH);
		
		
		log.info(this.getClass(),"ip address of master: "+master.getSystemID().getIp());
		
		log.info(this.getClass(),"starting message server on port: "+master.getSystemID().getMessagePort());
		master.getMessageServer().start();
		
		log.info(this.getClass(),"starting component controller");
		Thread tcc = new Thread(master.getComponentController());
		tcc.start();

		log.info(this.getClass(),"starting load balancer");
		Thread tlb = new Thread(master.getLoadBalancer());
		tlb.start();		

		log.info(this.getClass(),"starting recovery manager");
		Thread trm = new Thread(master.getRecoveryManager());
		trm.start();		
		
	}
	


	
	
	
	@Override
	public void run() {
	
		
		try {
			initialize();

		} catch (Exception e) {
			
			log.error(this.getClass(), e);
//			e.printStackTrace();
		}
		/*
		while(true){
			
			String messageString = master.getIncomingMessages().poll();
			
			if(messageString != null){
				
				Message message = new Message(messageString);
				
				log.message(this.getClass(),"received message: "+message);
				
//				DISPATCH TODO
				
				String component = message.getComponent();
				String operation = message.getOperation();
				
//				switch(component){
//				
//
//					case Component.regionServer : 
//							switch(operation){
//								case Command.STATUS_REPORT_REGIONSERVER : 
//									log.message(this.getClass(),"status report from "+message.getName()+":"+message.getContent());
//									master.getStatusReports().put(message.getName(), Integer.parseInt(message.getContent()));
//								break;	
//							
//							}
//						break;				
//					case Component.viewManager : 
//							switch(operation){
//							case Command.VIEWMANAGER_ASSIGNED : 
//								log.message(this.getClass(),"view manager assigned "+message.getName()+", to:"+message.getContent());
//								viewManagerAssigned(message.getContent(), message.getName());
//							break;	
//							case Command.VIEWMANAGER_REASSIGNED : 
//								log.message(this.getClass(),"view manager reassigned "+message.getName()+", to:"+message.getContent());
//								viewManagerReassigned(message.getContent(), message.getName());
//							break;
//							case Command.VIEWMANAGER_WITHDRAWN : 
//								log.message(this.getClass(),"view manager withdrawn "+message.getName()+", to:"+message.getContent());
//								viewManagerWithdrawn(message.getName());
//							break;
//							case Command.VIEWMANAGER_SHUTDOWN : 
//								log.message(this.getClass(),"view manager shutdown "+message.getName());
//								viewManagerShutdown(message.getName());
//							break;							
//							}
//						break;
//				}
				
				
				
				
			}
			
			try {
				Thread.sleep(Constants.MESSAGE_POLLING_INTERVAL);
			} catch (InterruptedException e) {
	
				e.printStackTrace();
			}
			
			
			
			
		}*/
		
		
		
		
	}





}
