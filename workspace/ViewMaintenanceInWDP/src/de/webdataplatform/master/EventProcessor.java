package de.webdataplatform.master;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.system.Event;

public class EventProcessor implements Runnable, ChildrenCallback{

	

	private MetaData metaData;
	
	private ILoadBalancer loadBalancer;
	
	private RecoveryManager recoveryManager;
	
	private Log log;
	
	
	public EventProcessor(Log log, MetaData metaData, ILoadBalancer loadBalancer, RecoveryManager recoveryManager) {
		super();
		this.loadBalancer = loadBalancer;
		this.recoveryManager = recoveryManager;
		this.metaData = metaData;
		this.log = log;
	}


//	EVENTS
	
	
	public String lookupRSOfViewManager(String viewManager){
		
		for(String regionServer : metaData.getAssignedViewManagers().keySet()){
			
			List<String> viewManagers = metaData.getAssignedViewManagers().get(regionServer);
			if(viewManagers.contains(viewManager)){
				return regionServer;
				
			}
		}
		return null;
	}
	
	
	public void viewManagerAdded(String viewManager){
		
		log.info(this.getClass(),"view manager has been added: "+viewManager);
		metaData.getViewManagers().add(viewManager);
		Event loadBalancingJob = new Event(Event.VIEWMANAGER_ADDED, viewManager);
		loadBalancer.addEvent(loadBalancingJob);
		
	}
	
	public void viewManagerShutdown(String viewManager){
		
		log.info(this.getClass(),"view manager has been shut down: "+viewManager);
		metaData.getViewManagers().remove(viewManager);
		Event event = new Event(Event.VIEWMANAGER_SHUTDOWN, viewManager, metaData.getVmRemoved().get(viewManager));
		metaData.getVmRemoved().remove(viewManager);
		loadBalancer.addEvent(event);
		
	}
	
	public void viewManagerCrashed(String viewManager){
		
		log.info(this.getClass(),"view manager has been crashed: "+viewManager);
		metaData.getViewManagers().remove(viewManager);
		Event event = new Event(Event.VIEWMANAGER_CRASHED, viewManager, lookupRSOfViewManager(viewManager));
//		loadBalancer.addEvent(event);
		recoveryManager.addEvent(event);
		
	}

	
	public void regionServerAdded(String regionServer){
		
		log.info(this.getClass(),"region server has been added: "+regionServer);
		metaData.getRegionServers().add(regionServer);
		Event event = new Event(Event.REGIONSERVER_ADDED, "", regionServer);
		loadBalancer.addEvent(event);
		
	}
	
	public void regionServerShutdown(String regionServer){
		
		log.info(this.getClass(),"region server has been shut down: "+regionServer);
		metaData.getRegionServers().remove(regionServer);
		metaData.getRsRemoved().remove(regionServer);
		Event loadBalancingJob = new Event(Event.REGIONSERVER_SHUTDOWN, "", regionServer);
		loadBalancer.addEvent(loadBalancingJob);
	}
	
	public void regionServerCrashed(String regionServer){
		
		log.info(this.getClass(),"region server has been crashed: "+regionServer);
		metaData.getRegionServers().remove(regionServer);
		Event loadBalancingJob = new Event(Event.REGIONSERVER_CRASHED, "", regionServer);
//		loadBalancer.addEvent(loadBalancingJob);
		recoveryManager.addEvent(loadBalancingJob);
	}
	


	
//	ZOOKEEPER EVENT DISPATCHER
	
	@Override
	public void processResult(int arg0, String arg1, Object arg2, List<String> arg3) {
		

		String znode = arg1;
		List<String> children = arg3;
		
		log.info(this.getClass(), "children: "+children);
		
			
		List<String> regionServerAdded = new ArrayList<String>();
		List<String> regionServerRemoved = new ArrayList<String>();
		List<String> regionServerCrashed = new ArrayList<String>();
		List<String> viewManagerAdded = new ArrayList<String>();
		List<String> viewManagerRemoved = new ArrayList<String>();
		List<String> viewManagerCrashed = new ArrayList<String>();

		
		
			if(znode.equals(SystemConfig.REGIONSERVER_ZOOKEEPERPATH)&& children != null){
				
					for(String regionServer : children){
						
						if(!metaData.getRegionServers().contains(regionServer)){
							
							regionServerAdded.add(regionServer);

						}
					}
					
					for(String regionServer : metaData.getRegionServers()){
						
						if(!children.contains(regionServer)){
							
							if(metaData.getRsRemoved().contains(regionServer)){
								regionServerRemoved.add(regionServer);
							}else{
								
								regionServerCrashed.add(regionServer);
							}


							
						}
						
					}
					
				
			}
			for (String rsAdded : regionServerAdded) regionServerAdded(rsAdded);
			for (String rsShutdown : regionServerRemoved) regionServerShutdown(rsShutdown);
			for (String rsCrashed : regionServerCrashed) regionServerCrashed(rsCrashed);
			
			if(znode.equals(SystemConfig.VIEWMANAGER_ZOOKEEPERPATH) && children != null){
				
					for(String viewManager : children){
					
						if(!metaData.getViewManagers().contains(viewManager)){
							
							viewManagerAdded.add(viewManager);
						}
					}
					
					
					
					for(String viewManager : metaData.getViewManagers()){
						
						if(!children.contains(viewManager)){

							if(metaData.getVmRemoved().containsKey(viewManager)){
								viewManagerRemoved.add(viewManager);
								
							}else{
								
								viewManagerCrashed.add(viewManager);
							}
						}
					}
					
				
			}
			
			for (String vmAdded : viewManagerAdded) viewManagerAdded(vmAdded);
			for (String vmShutdown : viewManagerRemoved) viewManagerShutdown(vmShutdown);
			for (String vmCrashed : viewManagerCrashed) viewManagerCrashed(vmCrashed);
//		}
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	
}
