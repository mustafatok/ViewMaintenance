package de.webdataplatform.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.system.Command;
import de.webdataplatform.system.Event;
import de.webdataplatform.viewmanager.CommitLog;
import de.webdataplatform.viewmanager.ICommitLog;

public class RecoveryManager implements Runnable, ICommandCaller{

	private MetaData metaData;
	
	private Queue<Event> incomingEvents;	
	
	private ComponentController componentController;
	
	private LoadBalancer loadBalancer;
	
	private ICommitLog commitLog;
	
	private Log log; 
	
	public RecoveryManager(Log log, MetaData metaData, ComponentController componentController, LoadBalancer loadBalancer) {
		super();
		this.metaData = metaData;
		this.componentController = componentController;
		this.loadBalancer = loadBalancer;
		incomingEvents = new ConcurrentLinkedQueue<Event>();
		this.log = log;
		
	
		
	}
	
	
	
	@Override
	public String toString() {
		return "RecoveryManager []";
	}



	public void addEvent(Event event){
		
		incomingEvents.add(event);
		
	}
	
	
	boolean recoveryExecuted=true;
	
	@Override
	public void callbackExecuteCommand(Command command) {
		
		log.info(this.getClass(),"command:"+command+" has been executed");
		
		log.info(this.getClass(),"recovery has been performed");
		
//		if(executionResult.getCommand().getType().equals(Command.WITHDRAW_CRASHED_VIEWMANAGER)){
//		
//			log.info(this.getClass(),"performing load balancing");
//			Event event = new Event(Event.VIEWMANAGER_CRASHED, executionResult.getCommand().getViewManager(), executionResult.getCommand().getRegionServer() );
//			loadBalancer.addEvent(event);
//			
//		}
		recoveryExecuted=true;
		
	}
	
	@Override
	public void executeCommand(Command command){
		
			
			componentController.queueCommand(this, command);

		
	}
	
	
	public String lookupRSOfViewManager(String viewManager){
		
		for(String regionServer : metaData.getAssignedViewManagers().keySet()){
			
			List<String> viewManagers = metaData.getAssignedViewManagers().get(regionServer);
			if(viewManagers.contains(viewManager)){
				return regionServer;
				
			}
		}
		return null;
	}
	
	public void viewManagerCrashed(String viewManager){
		
		
		String regionServer = lookupRSOfViewManager(viewManager);
		
		SystemID systemId = new SystemID(viewManager);
		
		if(regionServer != null){
			
			commitLog = new CommitLog(log, systemId.getName());
			

			boolean logOpen = commitLog.openLogFile();
			Map<String, Long> highestSequenceNos=null;
			
			if(logOpen){
				
				highestSequenceNos = commitLog.readHighestSeqNos();
				if(highestSequenceNos.keySet().size() == 0){
					log.info(this.getClass(),"commit log empty, nothing to replay");
				}else{
					
					log.info(this.getClass(),"last entries of commit log:"+highestSequenceNos);			
					
				}
				
			}else{
				log.info(this.getClass(),"no complete recovery possible, entries may be missing");
			}
			
			
			
			log.info(this.getClass(),"inform region server:"+regionServer+" that view manager: "+viewManager+" has been crashed");
			
			Command command = new Command(Command.WITHDRAW_CRASHED_VIEWMANAGER, viewManager, regionServer);
			executeCommand(command);
			
			if(logOpen && highestSequenceNos.keySet().size() != 0){
					
				command = new Command(Command.REPLAY_WRITEAHEADLOG, viewManager, regionServer, highestSequenceNos.toString());
				executeCommand(command);
			}
			
		}else{
			log.info(this.getClass(),"no region server assigned, doing nothing");
		}
		

		
	}
	
	public void regionServerCrashed(String regionServer){
		
		
		
		
		List<String> affectedViewManagers = metaData.getAssignedViewManagers().get(regionServer);
		
		if(affectedViewManagers != null && affectedViewManagers.size() > 0){
			
			for(String viewManager : affectedViewManagers){
				
				log.info(this.getClass(),"view manager:"+viewManager+" should be assigned to : "+regionServer);
				log.info(this.getClass(),"inform view manager:"+viewManager+" that region server: "+regionServer+" has been crashed");
				
				Command command = new Command(Event.REGIONSERVER_CRASHED, viewManager, regionServer);
				
				executeCommand(command);
			}
			return;
		}

		
	}	
	
	@Override
	public void run() {
		
		while(true){
			
			try{
			
						Event event = incomingEvents.poll();
						
						if(event != null){
							
//							if(recoveryExecuted){
//								
//								recoveryExecuted=false;
						
							log.info(this.getClass(),"received event: "+event.getType());
							
							
							
			//				DISPATCH TODO
							
							switch(event.getType()){
								case Event.VIEWMANAGER_CRASHED : 
									log.info(this.getClass(),"view manager crashed "+event.getViewManager());
									
									viewManagerCrashed(event.getViewManager());
								break;	
								case Event.REGIONSERVER_CRASHED : 
									log.info(this.getClass(),"region server crashed "+event.getViewManager());
									regionServerCrashed(event.getViewManager());
								break;	
							}
							
//						}
						
					
					}
			
			}catch(Exception e){
				
				log.error(this.getClass(), e);
			}
			
			try {
				Thread.sleep(SystemConfig.MESSAGES_POLLINGINTERVAL);
			} catch (InterruptedException e) {
	
				e.printStackTrace();
			}
			
			
		}	
		
		
		
	}
	
	
}
