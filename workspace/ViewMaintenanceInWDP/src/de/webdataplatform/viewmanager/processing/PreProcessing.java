package de.webdataplatform.viewmanager.processing;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.view.TableService;
import de.webdataplatform.view.ViewDefinitions;

public class PreProcessing implements Runnable{


	private Queue<String> incomingQueue;
	
	private Queue<BaseTableUpdate> outgoingQueue;
	
//	private Queue<String> statusQueue;
	
	private ViewDefinitions getViews;
	
	private AnalyzeLoad analyzeLoad;

	private Log log;
	
	private long btuLastTimestamp=123l;
	private BaseTableUpdate btuLast=null;
	private String keyLast="";
	
	public PreProcessing(Log log, Queue<String> incomingQueue, Queue<BaseTableUpdate> outgoingQueue){
		this.incomingQueue = incomingQueue;
		this.outgoingQueue = outgoingQueue;
//		this.statusQueue = statusQueue;
		this.log = log;
		updatesReceived = new AtomicLong();
		getViews = new ViewDefinitions(log, "viewdefinitions");
		getViews.loadViewDefinitions();
		updatesPreProcessed = new AtomicLong();
//		analyzeLoad = new AnalyzeLoad(statusQueue);
	}
	
	private long lastMeasure= new Date().getTime();
	
	private AtomicLong updatesReceived;
	private AtomicLong updatesPreProcessed;


	
	
	@Override
	public void run() {
		
		while(true){

			String update="";
			try{
			
			
			long currentTime = new Date().getTime();
			if((currentTime - lastMeasure) > SystemConfig.VIEWMANAGER_DISPLAYINTERVAL){
				lastMeasure = currentTime;
				log.info(this.getClass()," ---------------------");
				log.info(this.getClass()," input queue size: "+incomingQueue.size());
				log.info(this.getClass()," updates received: "+updatesReceived.get());
//				log.info(this.getClass()," load currently at "+outgoingQueue.size());
				log.info(this.getClass()," ---------------------");
			}
			
			
			if(outgoingQueue.size() < 5000){
				update = incomingQueue.poll();
				if(update != null){
					
					if(SystemConfig.LOGGING_LOGUPDATES)log.info(this.getClass(),"pre-processing update: "+update);
					BaseTableUpdate baseTableUpdate = new BaseTableUpdate(log, update);
					
					
					
					if(baseTableUpdate.getBaseTable().startsWith(SystemConfig.MESSAGES_MARKERPREFIX)){
	//					log.info(this.getClass(),"marker received, putting to map: "+baseTableUpdate);
						outgoingQueue.add(baseTableUpdate);
	//					markers.put(baseTableUpdate.getBaseTable(), baseTableUpdate.getRegionServer());
						continue;
					}

//					if(btuLastTimestamp == Long.parseLong(baseTableUpdate.getTimestamp())&& keyLast.equals(baseTableUpdate.getKey())){
//						log.info(this.getClass(), "Equal timestamp received:"+btuLastTimestamp+", old btu:"+btuLast);
//						log.info(this.getClass(), "Equal timestamp received:"+btuLastTimestamp+", new btu:"+baseTableUpdate);
//						
//					}
//					btuLastTimestamp = Long.parseLong(baseTableUpdate.getTimestamp()); 
//					btuLast = baseTableUpdate;
//					keyLast = baseTableUpdate.getKey();
//					if(baseTableUpdate.getColumns().containsKey("finishMarker")){
//	
////						TableService tableService = new TableService(log);
////						
////						Map<byte[],byte[]> cols = new HashMap<byte[], byte[]>();
////						cols.put(Bytes.toBytes("client"), Bytes.toBytes("client"));
////						tableService.put(Bytes.toBytes("finish_markers"), Bytes.toBytes(baseTableUpdate.getKey().split(":")[1]), cols);
//						
//						log.info(this.getClass(),"finish marker received");
//						
//						outgoingQueue.add(baseTableUpdate);
//						continue;
//					}
					
					updatesReceived.incrementAndGet();
	
					
	//				outgoingQueue.add(baseTableUpdate);
			
					List<BaseTableUpdate> baseTableViewUpdates =getViews.process(baseTableUpdate);				
					
					
					
					if(baseTableViewUpdates != null){
						for(BaseTableUpdate baseTableViewUpdate : baseTableViewUpdates){
							outgoingQueue.add(baseTableViewUpdate);
							log.update(this.getClass(), "baseTableUpdate pre-processed"+baseTableViewUpdate.toString());
						}
					}
					
					updatesPreProcessed.incrementAndGet();
					
	//				analyzeLoad.process(outgoingQueue.size());
				}
			
			}
//			if(outgoingQueue.size() > 5000){
//				try {
//					Thread.sleep(Constants.UPDATE_POLLING_INTERVAL);
//				} catch (InterruptedException e) {
//			
//					e.printStackTrace();
//				}
//			}
			
			}catch(Exception e){
				log.error(this.getClass(), e);
				log.info(this.getClass(),"Exception caught for update: "+update);
			}
		}
		
	}




	public AtomicLong getUpdatesReceived() {
		return updatesReceived;
	}



	public void setUpdatesReceived(AtomicLong updatesReceived) {
		this.updatesReceived = updatesReceived;
	}



	public AtomicLong getUpdatesPreProcessed() {
		return updatesPreProcessed;
	}



	public void setUpdatesPreProcessed(AtomicLong updatesPreProcessed) {
		this.updatesPreProcessed = updatesPreProcessed;
	}


	
	
	

}
