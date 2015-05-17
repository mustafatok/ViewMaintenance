package de.webdataplatform.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.view.ViewDefinitions;

public class WALReader implements Runnable {


	private Queue<BaseTableUpdate> btupdates;
	
	private ViewDefinitions viewDefinition;
	
	private String serverAddress;
	
	private String rsName;


	
	private Log log;
	
	public Map<String, Long> highestKeys=new HashMap<String,Long>();
	public Long updatesCounted;

	private AtomicLong updatesRetrieved;
	private AtomicLong updatesEnteredSystem;

	
	private AtomicLong updatesIterated=new AtomicLong();
	
	private Map<Path, Long> logPositions=new HashMap<Path, Long>();
	
	
	public WALReader(Log log, String serverAddress, String rsName,  ViewDefinitions viewDefinition, Queue<BaseTableUpdate> btupdates){
		
		this.rsName = rsName;
		this.serverAddress = serverAddress;
		this.btupdates = btupdates;
		this.viewDefinition = viewDefinition;
		this.updatesRetrieved = new AtomicLong();
//		this.updatesEnteredSystem = updatesEnteredSystem;
		this.updatesEnteredSystem = new AtomicLong();
		
		this.log = log;
	}
	
	
	
	public void replayWAL(Map<String, Long> replayKeys){
		
		log.wal(this.getClass(),"highestKeys:"+highestKeys);
		log.wal(this.getClass(),"Replaying from keys:"+replayKeys);
		log.wal(this.getClass(),"logPositions:"+logPositions);
		
		logPositions=new HashMap<Path, Long>();
		
		highestKeys.putAll(replayKeys);
		log.wal(this.getClass(),"result:"+highestKeys);

	}
	
//	private Configuration configuration;
	

	
	private FileSystem fs;
	
	public FileSystem getFileSystem(){
		

		if(fs == null){
	
			try {
				fs = FileSystem.get(NetworkConfig.getHadoopConfiguration());
			} catch (IOException e1) {
				
				  log.error(this.getClass(),e1);
			}
		}
		return fs;
	}
	
	
	
	@Override
	public void run() {

//		try{
//			Thread.sleep(EvaluationConfig.REGIONSERVER_READWALDELAY);
//		}catch(Exception e){
//			
//		}
			String walDirectory = "/hbase/WALs/";
    	
			log.wal(this.getClass(), "update iterated: "+updatesIterated);
			log.wal(this.getClass(), "polling WAL with interval:"+SystemConfig.REGIONSERVER_WALPOLLINGINTERVAL);
			log.wal(this.getClass(), "view definitions: "+viewDefinition.getViewDefinitions());
			while(true){
				try{
					

					log.wal(this.getClass(), "WAL directory: "+walDirectory);
					log.wal(this.getClass(), "searching WAL Files for: "+serverAddress);
					List<String> directoryPath = searchLogDirectory(walDirectory, serverAddress);

					
					if(directoryPath.size() > 0){
						
						log.wal(this.getClass(), "WAL Files FOUND");
						for (String string : directoryPath) {
							log.wal(this.getClass(), string);
						}
						searchNewEntries(directoryPath);
					}else{
						
						log.wal(this.getClass(), "WAL Files NOT FOUND");
						
					}
//		
					log.info(this.getClass(), "update iterated: "+updatesIterated);
					log.info(this.getClass(), "----------------------------------------------------");
					try {
						Thread.sleep(SystemConfig.REGIONSERVER_WALPOLLINGINTERVAL);
					} catch (InterruptedException e) {
					
						e.printStackTrace();
					}
				
				}catch(Exception e){
					
					log.error(this.getClass(), e);
				}
				
			}
		
		
		
	}
	
	public List<String> searchLogDirectory(String pathString, String serverName){
		

		List<String> result = new ArrayList<String>();
		
		
//		String pathString = "/hbase/.oldlogs/";
		Path path = new Path(pathString);

        try{

            FileStatus[] status = getFileSystem().listStatus(path);  
   
            
            for (int i=0;i<status.length;i++){
                if(status[i].getPath().getName().contains(serverName) && !status[i].getPath().getName().contains("splitting") && !status[i].getPath().getName().contains("meta")){
                
//                	log.info(this.getClass(), "wal reader has found log directory: "+status[i].getPath().getName());
                	result.add(status[i].getPath().toUri().toString());
                }

            } 
            

            

	    }catch(Exception e){
	            log.error(this.getClass(), e);
	    }
        
        return result;
		
	}
	

	

	public void searchNewEntries(List<String> pathStrings){
		
		
		for (String path : pathStrings) {
			searchDirectory(path);
		}
		
	}
	
	
	public void searchDirectory(String pathString){
		
		
//		String pathString = "/hbase/.oldlogs/";
		log.wal(this.getClass(), "------------------------------------------------------------");
		log.wal(this.getClass(), "Searching directory: "+pathString);
		log.wal(this.getClass(), "------------------------------------------------------------");
		
		Path path = new Path(pathString);

		
        try{

            FileStatus[] status = getFileSystem().listStatus(path);  
   
//            boolean queueFull = false;
            
            for (int x=0;x<status.length;x++){
            	
                
            	
                Path pathFile = status[x].getPath();
                
                System.out.println(pathFile);
                
                if(!pathFile.toString().contains("meta")){
		                
		                log.wal(this.getClass(), "------------------------------");
		                log.wal(this.getClass(), "Searching file: "+pathFile);
		                log.wal(this.getClass(), "------------------------------");
		//                log.info(this.getClass(), "pathFile: "+pathFile);
		
		                
		                
		                Configuration configuration = NetworkConfig.getHBaseConfiguration(log);
		                
		                Reader writeAheadLog = HLogFactory.createReader(fs, pathFile, configuration);
		                
		        		if(logPositions.get(pathFile)!= null){
		        			
		//        			System.out.println("already read log");
		//        			System.out.println("jumping to position: "+logPositions.get(pathFile));
		        			writeAheadLog.seek(logPositions.get(pathFile));
		        			
		        			
		        		}
		                
			                
			                try {
			                      HLog.Entry entry;
			                      while ((entry = writeAheadLog.next()) != null) {
			                    	  
			                    	    updatesIterated.incrementAndGet();
			                    	    
				                        HLogKey key = entry.getKey();
				                        WALEdit edit = entry.getEdit();
				                        
//				                        edit
				                        
				                        log.wal(this.getClass(), "-----------");
		//		                        log.info(this.getClass(), "pathFile: "+pathFile);
		//		                        log.info(this.getClass(),"highestKey: "+highestKey);
				                        
//				                        Map<String, Object> txn = key.toStringMap();
				                        	
				                        	
				                        log.wal(this.getClass(),"key.getLogSeqNum(): "+key.getLogSeqNum());
				                        log.wal(this.getClass(),"write time: "+key.getWriteTime());
				                        log.wal(this.getClass(),"nonce: "+key.toStringMap());
				                        
				                        String regionName = Bytes.toString(key.getEncodedRegionName());
				                        log.wal(this.getClass(),"region: "+regionName);
//				                        log.info(this.getClass(),"sequence: "+txn.get("sequence"));
		//		                        log.info(this.getClass(),"entry: "+entry);
				                        log.wal(this.getClass(),"tablename: "+Bytes.toString(key.getTablename().getName()));
				                        log.wal(this.getClass(),"key: "+Bytes.toString(edit.getKeyValues().get(0).getRow()));
				                        
				                        Long highestKey = highestKeys.get(regionName);
				                        
				            			if(highestKey == null || highestKey < key.getLogSeqNum()){
		//		            				
					            			log.wal(this.getClass(),"processing btu");
					            				
					            			logPositions.put(pathFile, writeAheadLog.getPosition());
					                        
					            			log.wal(this.getClass(),"condition: "+viewDefinition.getBasetablesWithView().contains(Bytes.toString(key.getTablename().getName())));
					            			
					            			if(viewDefinition.getBasetablesWithView().contains(Bytes.toString(key.getTablename().getName()))){
				
				
			        							if(updatesEnteredSystem != null){
			        								updatesEnteredSystem.set(updatesEnteredSystem.get()+viewDefinition.numOfAssignedViewTables(Bytes.toString(key.getTablename().getName())));
			        								
			        							}	

					        					BaseTableUpdate btu = retrieveUpdate(key, edit);
					    		    			if(btu != null){
					    		    					log.wal(this.getClass(),"inserting btu to queue");
					        		    				btupdates.add(btu);
					        		    				highestKeys.put(regionName, key.getLogSeqNum());
					        		    				
					    		    			}
				        		    				
					        				}
			        					
			        				
			        				}
				                        
			
			                        
			                      }
			                } finally {
			                  writeAheadLog.close();
			                }
                }

            } 
            
	    }catch(Exception e){

	    	log.error(this.getClass(), e);
	    }
        

		
	}





	private BaseTableUpdate retrieveUpdate(HLogKey key, WALEdit edit) {

		
		String rowKey = "";
		String type = "";
		
		Map<String, String> columns = new HashMap<String, String>();
		Map<String, String> oldColumns = new HashMap<String, String>();
		Map<String, String> colFamilies = new HashMap<String, String>();
		
		for(KeyValue keyValue : edit.getKeyValues()){
			
			
			rowKey = Bytes.toString(keyValue.getRow());
			
			String qualifier = Bytes.toString(keyValue.getQualifier());
			String value = Bytes.toString(keyValue.getValue());
			String colFam = Bytes.toString(keyValue.getFamily());
			
			if(qualifier.startsWith("old_")){
				
				oldColumns.put(qualifier.replace("old_", ""), value);
			}else{
				
				columns.put(qualifier, value);
				colFamilies.put(qualifier, colFam);
			}
			
			
//        						System.out.println("qualifier: "+qualifier);
//        						System.out.println("value: "+value);
//        						System.out.println("------------------: "+value);
			
//        						if(qualifier.equals("aggregationKey"))aggregationKey = value;
//        						if(qualifier.equals("aggregationValue"))aggregationValue = value;
			
			type = KeyValue.Type.codeToType(keyValue.getType()).toString();
		
			
			
		}

//        					log.info(this.getClass(), "columns: "+columns);
		
//		log.info(this.getClass(), "rowKey: "+rowKey);
		
		
		if(!rowKey.equals("METAROW")){
			
			String regionName = Bytes.toString(key.getEncodedRegionName());
			
			BaseTableUpdate btu = new BaseTableUpdate(Bytes.toString(key.getTablename().getName()), rsName, regionName, key.getLogSeqNum()+"",key.getWriteTime()+"", rowKey, type, columns, oldColumns, colFamilies);
			
			log.wal(this.getClass(), "btu: "+btu);
//			log.info(this.getClass(), "btu: "+btu);
//			log.info(this.getClass(), "conver-btu: "+btu.convertToString());
//			log.info(this.getClass(), "--------------------------------------");
			
			if(updatesRetrieved != null)updatesRetrieved.incrementAndGet();
//	        				System.out.println("putting key:"+btu+" to queue");
			
			
			
			return btu;
		}

		return null;
	}



	public AtomicLong getUpdatesEnteredSystem() {
		return updatesEnteredSystem;
	}



	public void setUpdatesEnteredSystem(AtomicLong updatesEnteredSystem) {
		this.updatesEnteredSystem = updatesEnteredSystem;
	}



	public AtomicLong getUpdatesRetrieved() {
		return updatesRetrieved;
	}



	public void setUpdatesRetrieved(AtomicLong updatesRetrieved) {
		this.updatesRetrieved = updatesRetrieved;
	}
	


	

	

	
	
	
	
	

	

}
