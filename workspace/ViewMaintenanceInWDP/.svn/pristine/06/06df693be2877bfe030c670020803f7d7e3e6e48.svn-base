package de.webdataplatform.master;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.system.Command;
import de.webdataplatform.system.Event;

public class LoadBalancer implements ILoadBalancer, Runnable, ICommandCaller{

	
	
	private ComponentController componentController;
	
	private Queue<Event> incomingEvents;	
	
	private MetaData metaData;
	
	private Log log;
	
//	private boolean executingCommand;
	
	
	
//	public synchronized boolean isExecutingCommand() {
//		return executingCommand;
//	}
//
//
//	public synchronized void setExecutingCommand(boolean executingCommand) {
//		this.executingCommand = executingCommand;
//	}


	public LoadBalancer(Log log, MetaData metaData, ComponentController componentController) {
		super();

		this.log = log;
		this.componentController = componentController;
		this.metaData = metaData;
		incomingEvents = new ConcurrentLinkedQueue<Event>();
	}
	
	
	public synchronized void addEvent(Event event){
		
		incomingEvents.add(event);
		
	}
	
	
	
	@Override
	public String toString() {
		return "LoadBalancer []";
	}


	public List<String> lookupFreeViewManager(){
		
		List<String> freeViewManagers = new ArrayList<String>();
		
		for(String viewManager : metaData.getViewManagers()){
			
		
			if(!metaData.getAssignedViewManagers().containsValue(viewManager)){
				freeViewManagers.add(viewManager);
				
			}
		}
		return freeViewManagers;
	}
	
	
	boolean loadBalancingExecuted=true;
	@Override
	public void callbackExecuteCommand(Command command) {
		
		loadBalancingExecuted=true;
		log.info(this.getClass(),"command has been executed");
//		setExecutingCommand(false);
	}
	
	@Override
	public synchronized void executeCommand(Command command) throws Exception{
		
//		List<Command> commands = new ArrayList<Command>();
//		
//		commands.add(command);
		
		
		
//		int x = 0;
		
//		while(!lastCommandExecuted){
//			
//			lastCommandExecuted=false;
			componentController.queueCommand(this, command);
//
//			try {
//				Thread.sleep(Constants.MESSAGE_POLLING_INTERVAL);
//			} catch (InterruptedException e) {
//	
//				e.printStackTrace();
//			}
//			x++;
//			
//			if(x == 10000)throw new CommandNotExecutedException("command could not be executed: "+command);
//		}

//		setExecutingCommand(true);
		
		
	}
	

	public void viewManagerIncreased(String viewManager){

		
		String chosenRegionServer = findRegionServerNeedsVM();
		
			
		if(chosenRegionServer == null){
			log.info(this.getClass(),"load balancer could not find answer");
			loadBalancingExecuted=true;
			return;
		}
		
		log.info(this.getClass(),"view manager:"+viewManager+" should be assigned to : "+chosenRegionServer);
		
		Command command = new Command(Command.ASSIGN_VIEWMANAGER, viewManager, chosenRegionServer);
		
		try {
			executeCommand(command);
		} catch (Exception e) {

			log.error(this.getClass(), e);
		}
		
		
	}



	
	
	
	
	/*public void viewManagerDecreased(String viewManager, String removedFromRS){

//		String regionServer = lookupRSOfViewManager(viewManager);
		
		log.info(this.getClass(),"vm was removed from: "+removedFromRS);
		
		if(metaData.getAssignedViewManagers().get(removedFromRS) == null || metaData.getAssignedViewManagers().get(removedFromRS).size() == 0){
			
			regionServerIncreased(removedFromRS);
		}
		
	}*/
	
/*	public void regionServerIncreased(String regionServer){
		
		
		
//		Searching for free view managers
		
		List<String> freeViewManagers = lookupFreeViewManager();
		
		if(freeViewManagers.size() > 0){
			for(String viewManager : freeViewManagers){
				log.info(this.getClass(),"view manager:"+viewManager+" should be assigned to : "+regionServer);
				
				Command command = new Command(Command.ASSIGN_VIEWMANAGER, viewManager, regionServer);
				
				try {
					executeCommand(command);
				} catch (Exception e) {

					log.error(this.getClass(), e);
				}
			}
			return;
		}
		
//		Searching for region servers which dispense with view managers
		
		String chosenRegionServer = findRegionServerDispenseWithVM();
		
		if(chosenRegionServer == null){
			log.info(this.getClass(),"load balancer could not find answer");
			loadBalancingExecuted=true;
			return;
		}
		
		List<String> viewManagers = metaData.getAssignedViewManagers().get(chosenRegionServer);
	
		String viewManager = viewManagers.get(0);
		
		log.info(this.getClass(),"view manager:"+viewManager+" should be reassigned from:"+chosenRegionServer+" to : "+regionServer);
		
		Command command = new Command(Command.REASSIGN_VIEWMANAGER, viewManager, regionServer);
		
		try {
			executeCommand(command);
		} catch (Exception e) {

			log.error(this.getClass(), e);
		}
		
	}*/

	private String findRegionServerNeedsVM() {

		if(metaData.getRegionServers().size() == 0){
			log.info(this.getClass(),"no region servers available, waiting...");
			return null;
		}
		
//		FIRST ROUND
		String chosenRegionServer=null;
		
		List<String> regionServersZeroVM = getRegionServers(0, EQUAL);
//		log.info(this.getClass(),"region server with 0 view managers "+regionServersZeroVM);
		regionServersZeroVM = getRegionServerByLoad(regionServersZeroVM, HEAVIEST_LOAD_WITHOUT_ZERO);
//		log.info(this.getClass(),"region server with 0 view managers and load != 0"+regionServersZeroVM);
		if(regionServersZeroVM.size() > 0){
			int zahl = (int)(Math.random() * regionServersZeroVM.size());
			chosenRegionServer = regionServersZeroVM.get(zahl);
			return chosenRegionServer;
		}
		
//		SECOND ROUND
		List<String> regionServersByLoad = getRegionServerByLoad(metaData.getRegionServers(), HEAVIEST_LOAD);
//		log.info(this.getClass(),"region server with heaviest load "+regionServersByLoad);
		regionServersByLoad = getRegionServerByVmCount(regionServersByLoad, LEAST_VMCOUNT);
//		log.info(this.getClass(),"region server with heaviest load and least vms "+regionServersByLoad);
		if(regionServersByLoad.size() > 0){
			int zahl = (int)(Math.random() * regionServersByLoad.size());
			chosenRegionServer = regionServersByLoad.get(zahl);
			return chosenRegionServer;
		}
		return null;
	}
	
	
	
	private String findRegionServerDispenseWithVM() {
		
		if(metaData.getRegionServers().size() == 0){
			log.info(this.getClass(),"no region servers available, waiting...");
			return null;
		}
		
//		FIRST ROUND
		String chosenRegionServer=null;

		List<String> regionServers = getRegionServers(2, GREATER_OR_EQUAL);
		
		if(regionServers.size() == 0){
			log.info(this.getClass(),"no free view managers, doing nothing");
			return null;
		}

		List<String> regionServersByLoad = getRegionServerByLoad(regionServers, LEAST_LOAD);
		regionServersByLoad = getRegionServerByVmCount(regionServersByLoad, HEAVIEST_VMCOUNT);
		int zahl = (int)(Math.random() * regionServersByLoad.size());
		chosenRegionServer = regionServersByLoad.get(zahl);
		
		
		return chosenRegionServer;
	}
	
	
	public void regionServerDecreased(String regionServer){
		
		List<String> viewManagers = metaData.getAssignedViewManagers().get(regionServer);
		
		for(String viewManager : viewManagers){
			
			viewManagerIncreased(viewManager);
			
		}
		
		
		
	}
	
	public void balanceLoad(){
		
		log.info(this.getClass(),"-----------------------------");
		
//		List<String> leastLoadedServers = getRegionServerByLoad(metaData.getRegionServers(), LEAST_LOAD);
//		List<String> maxLoadedServers = getRegionServerByLoad(metaData.getRegionServers(), HEAVIEST_LOAD);
		
		String leastLoadedServer = findRegionServerDispenseWithVM();
		String maxLoadedServer = findRegionServerNeedsVM();

		log.info(this.getClass(),"leastLoadedServer:"+leastLoadedServer);
		log.info(this.getClass(),"maxLoadedServer:"+maxLoadedServer);
		
		if(leastLoadedServer != null && maxLoadedServer != null && !leastLoadedServer.equals(maxLoadedServer)){
			
			List<String> leastVms = metaData.getAssignedViewManagers().get(leastLoadedServer);
			List<String> maxVms = metaData.getAssignedViewManagers().get(maxLoadedServer);
			
			float leastLoad = metaData.getStatusReports().get(leastLoadedServer);
			float maxLoad = metaData.getStatusReports().get(maxLoadedServer);
			
	
			
			float currentLoad = (maxLoad/maxVms.size())+(leastLoad/leastVms.size());
			float possibleLoad = (maxLoad/(maxVms.size()+1))+(leastLoad/(leastVms.size()-1));
			
			log.info(this.getClass(),"currentLoad: "+currentLoad);
			log.info(this.getClass(),"possibleLoad: "+possibleLoad);
			
		}else{
			log.info(this.getClass(),"no loaded servers found ");
		}
		
		log.info(this.getClass(),"exercise load balancing");
//		log.info(this.getClass(),"least loaded servers:"+leastLoadedServers);
		
		
		loadBalancingExecuted=true;
		
		log.info(this.getClass(),"-----------------------------");
		
	}

	
	
	public final static String EQUAL="equal";
	public final static String SMALLER_OR_EQUAL="smallerOrEqual";
	public final static String GREATER_OR_EQUAL="greaterOrEqual";
	
	public List<String> getRegionServers(int viewManagerCount, String operator){
		
		List<String> regionServers = new ArrayList<String>();
		
		for(String regionServer : metaData.getRegionServers()){
			
			List<String> assignedViewManagers = metaData.getAssignedViewManagers().get(regionServer);
			
			if(assignedViewManagers == null && operator.equals(EQUAL) && viewManagerCount == 0){
				
				regionServers.add(regionServer);
			}
			
			if(assignedViewManagers != null && operator.equals(EQUAL) && assignedViewManagers.size() ==  viewManagerCount){
				
				regionServers.add(regionServer);
			}
			if(assignedViewManagers != null && operator.equals(SMALLER_OR_EQUAL) && assignedViewManagers.size() <=  viewManagerCount){
				
				regionServers.add(regionServer);
			}			
			if(assignedViewManagers != null && operator.equals(GREATER_OR_EQUAL) && assignedViewManagers.size() >=  viewManagerCount){
				
				regionServers.add(regionServer);
			}				
			
		}
		
		return regionServers;
		
	}
	
	public final static String HEAVIEST_VMCOUNT="heaviestVmCount";
	public final static String LEAST_VMCOUNT="leastVmCount";
	
	public List<String> getRegionServerByVmCount(List<String> regionServers, String operator){
		
		float compareValue=0f;
		
		if(operator.equals(HEAVIEST_VMCOUNT))compareValue=-1f;
		if(operator.equals(LEAST_VMCOUNT))compareValue=Float.MAX_VALUE;
		
		List<String> chosenRegionServers=new ArrayList<String>();
		
		for(String regionServer : regionServers){
			
			float vmPerViewManager;
			
			
			if(metaData.getAssignedViewManagers().get(regionServer) == null || metaData.getAssignedViewManagers().get(regionServer).size() == 0){
				vmPerViewManager = 0;

			}else vmPerViewManager = metaData.getAssignedViewManagers().get(regionServer).size();
			
			
			log.info(this.getClass(),regionServer+": vmPerViewManager: "+vmPerViewManager);

			if(operator.equals(HEAVIEST_VMCOUNT)){ 
				
				if(vmPerViewManager == compareValue){
					chosenRegionServers.add(regionServer);
				}
				if(vmPerViewManager > compareValue){
					
					chosenRegionServers=new ArrayList<String>();
					compareValue = vmPerViewManager;
					chosenRegionServers.add(regionServer);
					
				}
			}
			if(operator.equals(LEAST_VMCOUNT)){
				
				if(vmPerViewManager == compareValue){
					chosenRegionServers.add(regionServer);
				}
				if(vmPerViewManager < compareValue){
					
					chosenRegionServers=new ArrayList<String>();
					compareValue = vmPerViewManager;
					chosenRegionServers.add(regionServer);
					
				}
			}
			
		}
			
		
		return chosenRegionServers;
		
		
	}	
	
	public final static String HEAVIEST_LOAD="heaviestLoad";
	public final static String HEAVIEST_LOAD_WITHOUT_ZERO="heaviestLoadWithoutZero";
	public final static String LEAST_LOAD="leastLoad";
	public final static String LEAST_LOAD_WITHOUT_ZERO="leastLoadWithoutZero";
	
	public List<String> getRegionServerByLoad(List<String> regionServers, String operator){
		
		float compareValue=0f;
		
		if(operator.equals(HEAVIEST_LOAD)||operator.equals(HEAVIEST_LOAD_WITHOUT_ZERO))compareValue=-1f;
		if(operator.equals(LEAST_LOAD)||operator.equals(LEAST_LOAD_WITHOUT_ZERO))compareValue=Float.MAX_VALUE;
		
		List<String> chosenRegionServers=new ArrayList<String>();
		
		for(String regionServer : regionServers){
			
			float loadPerViewManager;
			
			
			if(metaData.getAssignedViewManagers().get(regionServer) == null || metaData.getAssignedViewManagers().get(regionServer).size() == 0){
				
				if(metaData.getStatusReports().get(regionServer) != null){
					
					loadPerViewManager = metaData.getStatusReports().get(regionServer);
				}else loadPerViewManager = 0;
			}else{
				
				if(metaData.getStatusReports().get(regionServer) == null)loadPerViewManager=0;
				else loadPerViewManager = metaData.getStatusReports().get(regionServer)/metaData.getAssignedViewManagers().get(regionServer).size();
			}
			
			log.info(this.getClass(),regionServer+": loadPerViewManager: "+loadPerViewManager);

			if(operator.equals(HEAVIEST_LOAD)){ 
				
				if(loadPerViewManager == compareValue){
					chosenRegionServers.add(regionServer);
				}
				if(loadPerViewManager > compareValue){
					
					chosenRegionServers=new ArrayList<String>();
					compareValue = loadPerViewManager;
					chosenRegionServers.add(regionServer);
					
				}
			}
			if(operator.equals(HEAVIEST_LOAD_WITHOUT_ZERO)){ 
				
				if(loadPerViewManager == compareValue){
					chosenRegionServers.add(regionServer);
				}
				if(loadPerViewManager > compareValue && loadPerViewManager != 0){
					
					chosenRegionServers=new ArrayList<String>();
					compareValue = loadPerViewManager;
					chosenRegionServers.add(regionServer);
					
				}
			}
			if(operator.equals(LEAST_LOAD)){
				
				if(loadPerViewManager == compareValue){
					chosenRegionServers.add(regionServer);
				}
				if(loadPerViewManager < compareValue){
					
					chosenRegionServers=new ArrayList<String>();
					compareValue = loadPerViewManager;
					chosenRegionServers.add(regionServer);
					
				}
			}
			if(operator.equals(LEAST_LOAD_WITHOUT_ZERO)){
				
				if(loadPerViewManager == compareValue){
					chosenRegionServers.add(regionServer);
				}
				if(loadPerViewManager < compareValue && loadPerViewManager != 0){
					
					chosenRegionServers=new ArrayList<String>();
					compareValue = loadPerViewManager;
					chosenRegionServers.add(regionServer);
					
				}
			}
			
		}
			
		
		return chosenRegionServers;
		
		
	}


	private long lastMeasure = new Date().getTime();
	
	
	@Override
	public void run() {
		
		while(true){

			try{
				
						
//					long currentTime = new Date().getTime();
					
//					if((currentTime - lastMeasure) > 5000){
//						
//						lastMeasure = currentTime;
//						
//						
//						Event loadBalancingJob = new Event(Event.BALANCE_LOAD, "");
//						this.incomingEvents.add(loadBalancingJob);
//						
//		
//					}	
					if(loadBalancingExecuted){	
						
						Event event = incomingEvents.poll();
//						log.info(this.getClass(),"event: "+event);
//						if(event == null){
//							loadBalancingExecuted=true;
//						}
						
						if(event != null){
							
								loadBalancingExecuted=false;
								log.info(this.getClass(),"received event: "+event.getType());
							
								switch(event.getType()){
									case Event.VIEWMANAGER_ADDED : 
										log.info(this.getClass(),"view manager added "+event.getViewManager());
										viewManagerIncreased(event.getViewManager());
									break;
									case Event.VIEWMANAGER_CRASHED : 
										log.info(this.getClass(),"view manager crashed "+event.getViewManager());
//										viewManagerDecreased(event.getViewManager(), event.getRegionServer());
										loadBalancingExecuted=true;
									break;	
									case Event.VIEWMANAGER_SHUTDOWN : 
										log.info(this.getClass(),"view manager removed "+event.getViewManager());
//										viewManagerDecreased(event.getViewManager(), event.getRegionServer());
										loadBalancingExecuted=true;
									break;	
									case Event.REGIONSERVER_ADDED : 
										log.info(this.getClass(),"region server added "+event.getViewManager());
//										regionServerIncreased(event.getViewManager());
										loadBalancingExecuted=true;
									break;
									case Event.REGIONSERVER_CRASHED : 
										log.info(this.getClass(),"region server crashed "+event.getViewManager());
										regionServerDecreased(event.getViewManager());
									break;	
									case Event.REGIONSERVER_SHUTDOWN : 
										log.info(this.getClass(),"region server removed "+event.getViewManager());
										regionServerDecreased(event.getViewManager());
									break;	
									case Event.BALANCE_LOAD : 
										log.info(this.getClass(),"load balancing "+event.getViewManager());
										balanceLoad();
									break;	
								}
							
							
						}
						
			
					}
					
					try {
						Thread.sleep(SystemConfig.MESSAGES_POLLINGINTERVAL);
					} catch (InterruptedException e) {
						
						e.printStackTrace();
					}
			}catch(Exception e){
				
				log.error(this.getClass(), e);
			}
			
		}	
		
		
		
	}


	
	


}
