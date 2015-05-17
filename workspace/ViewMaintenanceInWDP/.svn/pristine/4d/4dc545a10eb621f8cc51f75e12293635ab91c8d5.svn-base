package de.webdataplatform.regionserver;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.message.UpdateClient;
import de.webdataplatform.viewmanager.IViewManager;
import de.webdataplatform.viewmanager.ViewManager;

public class UpdateDistributor{

	

	
	private Map<IViewManager, Queue<String>> updateQueues= new ConcurrentHashMap<IViewManager, Queue<String>>();
	
	private Map<IViewManager, UpdateClient> updateThreads = new ConcurrentHashMap<IViewManager, UpdateClient>();
	
	private SystemID rsNetworkAddress;
	
	private AtomicLong updatesSent;
	
	private Log log;
	
	public UpdateDistributor(Log log, SystemID rsNetworkAddress, AtomicLong updatesSent){

		this.updatesSent = updatesSent;
		this.rsNetworkAddress = rsNetworkAddress;
		this.log = log;
		
	}
	
	
	
	public int getMaxQueueSize(){
		
		int maxSize = 0;
		
		for(IViewManager vm : updateQueues.keySet()){
			
			int size = updateQueues.get(vm).size();
			
			if(size > maxSize)maxSize = size;
		}
		
		return maxSize;
		
	}
	
	
	public Queue<String> addQueue(ViewManager viewManager){
		
		Queue<String> updateQueue = new ConcurrentLinkedQueue<String>();
		
		updateQueues.put(viewManager, updateQueue);
		
		return updateQueue;
	}
	
	
//	public List<String> emptyQueue(ViewManager viewManager){
		public void emptyQueue(ViewManager viewManager){
		
		Queue<String> updateQueue = updateQueues.get(viewManager);
		
		updateQueue.clear();
		
//		List<String> result = new ArrayList<String>();
//		
//		log.info(this.getClass(),"update queue size "+updateQueue.size()+"");
//		
//		if(updateQueue != null){
//			
//			String element = updateQueue.poll();
//			
//			while(element != null){
//				
//				
//				
//				result.add(element);
//				element = updateQueue.poll();
//				
//			}
//		}
//		return result;

	}
	
//	public void removeQueue(ViewManager viewManager)throws QueueNotEmptyException{
	public void removeQueue(ViewManager viewManager){
		
		Queue<String> updateQueue = updateQueues.get(viewManager);
		
		if(updateQueue == null)return;
		
//		if(updateQueue.size() != 0)throw new QueueNotEmptyException("Size of queue is:"+updateQueue.size()+", should be zero");
		
		updateQueues.remove(viewManager);
		
	}
	
	public boolean startSendingThread(ViewManager viewManager) throws InterruptedException{
		
		UpdateClient updateClient = new UpdateClient(log, rsNetworkAddress, viewManager, updateQueues.get(viewManager), updatesSent);
		
//		Thread t = new Thread(updateClient);

		updateThreads.put(viewManager, updateClient);
		
//		Log.info(this.getClass(),"starting thread for "+viewManager);
		
		updateClient.start();
			

		return true;
		
	}
	
	public boolean stopSendingThread(ViewManager viewManager) throws InterruptedException{
		
		UpdateClient updateClient = updateThreads.get(viewManager);
		
		updateClient.terminate();
		
		updateClient.join();
			

		return true;
		
	}
	
	
	public void queueUpdate(ViewManager viewManager, String update) throws NoQueueForViewManagerException{
		
//		if(!updateQueues.containsKey(viewManager)){
			
//			Log.info(this.getClass(),"connecting to new view manager:"+viewManager.getVMName()+", "+viewManager.getSystemID().getIp()+", "+viewManager.getSystemID().getUpdatePort());
			
//			Queue<BaseTableUpdate> updateQueue = new ConcurrentLinkedQueue<BaseTableUpdate>();
//			
//			updateQueue.add(update);
//			
//			updateQueues.put(viewManager, updateQueue);
			
			
			
//			UpdateClient updateClient = new UpdateClient(rsNetworkAddress, viewManager, updateQueue, updatesSent);
//			
////			Thread t = new Thread(updateClient);
//
//			updateThreads.put(viewManager, updateClient);
//			
////			Log.info(this.getClass(),"starting thread for "+viewManager);
//			
//			updateClient.start();
			


			
//		}else{
			
//			if(updateThreads.get(viewManager)== null || !updateThreads.get(viewManager).isAlive())throw new SendingThreadNotRunningException("sending thread is not running");
//			{
				
//				Log.info(this.getClass(),"Sending Thread for "+viewManager+" not alive, trying to restart");
//				
//				UpdateClient updateClient = new UpdateClient(rsNetworkAddress, viewManager, updateQueues.get(viewManager), updatesSent);
//				
////				Thread t = new Thread(updateClient);
//
//				updateThreads.put(viewManager, updateClient);
//				
//				updateClient.start();
//			}
//			
			
			Queue<String> updateQueue = updateQueues.get(viewManager);
			
			if(updateQueue == null)throw new NoQueueForViewManagerException("No queue for view manager:"+viewManager);
			
			updateQueue.add(update);
			
			log.update(this.getClass(),"put update to queue "+viewManager+": size: "+updateQueues.get(viewManager).size());
			
			


//		}
		
		
		
		
		
	}


	
	
	

}
