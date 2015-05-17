package de.webdataplatform.viewmanager.processing;


public class CopyOfProcessing implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

//
//
//
//	private Queue<BaseTableUpdate> incomingQueue;
//	
//
//	
//	
////	
////	private QueryBaseTable queryBaseTable;
////	
////	private CalculateResult calculateResult;
////	
////	private UpdateView updateView;
////	
//	private ICommitLog commitLog;
//	
//	
//	private String vmName;
//	
//	private ConcurrentHashMap<String, String> markers;
//	
//	
//	private BaseTableService baseTableService;
//
//	private TableService viewTableService;
//
//
//	private Log log;
//	
//
//	
//	public CopyOfProcessing(Log log, Queue<BaseTableUpdate> incomingQueue, String vmName){
//		this.incomingQueue = incomingQueue;
//		this.vmName = vmName;
//		this.commitLog = new CommitLog(log, vmName);
//		viewRecordUpdates = new AtomicLong();
//		commitLogUpdates = new AtomicLong();
//		markers = new ConcurrentHashMap<String, String>();
//		this.log = log;
//		
//		baseTableService = new BaseTableService(log);
//		viewTableService = new TableService(log);
//
//	}
//	
//	private long lastMeasure;
////	private AtomicLong insertCount;
////	private AtomicLong updateCount;
////	private AtomicLong deletionCount;
//	private AtomicLong viewRecordUpdates;
//	private AtomicLong commitLogUpdates;
//	
//	@Override
//	public void run() {
//		
//
//		
//		while(true){
//			
//			try{
//			
//			long currentTime = new Date().getTime();
//			if((currentTime - lastMeasure) > SystemConfig.VIEWMANAGER_DISPLAYINTERVAL){
//				lastMeasure = currentTime;
//				log.info(this.getClass()," ---------------------");
//				log.info(this.getClass()," load currently at "+incomingQueue.size());
//				log.info(this.getClass()," updates processed "+viewRecordUpdates.get());
//				log.info(this.getClass()," ---------------------");
//			}
//			BaseTableUpdate update = incomingQueue.poll();
//			
//			if(update != null){
//				
//				if(SystemConfig.LOGGING_LOGUPDATES)log.info(this.getClass(),"processing update: "+update);
//				
//				try{
//					
//					if(update.getBaseTable().startsWith(SystemConfig.MESSAGES_MARKERPREFIX)){
//						log.info(this.getClass(),"marker received, putting to map: "+update);
//						markers.put(update.getBaseTable(), update.getRegionServer());
//						continue;
//					}			
//					
//					
////					long processingTime = new Date().getTime();
//					process(update);
////					log.info(this.getClass(), "processing time: "+update.getType()+": "+(new Date().getTime() - processingTime));
//
//					viewRecordUpdates.incrementAndGet();
//					
////					log.info(this.getClass(),"writing update to commit log: "+update);
//					
//					long walTime = new Date().getTime();
//					if(SystemConfig.FAULTTOLERANCE_COMMITLOG){
//		
//						commitLog.writeToLogFile(vmName, update);
//						commitLogUpdates.incrementAndGet();
//					}
////					log.info(this.getClass(), "walTime time: "+(new Date().getTime() - walTime));
//					
//				}catch(Exception e){
//					
//					log.info(this.getClass(), "errornous update:"+update.convertToString());
//					log.error(this.getClass(), e);
//		
//				}
//				
//				
//			}
//			
//		
//			if(incomingQueue.size() == 0){
//			try {
//				Thread.sleep(SystemConfig.VIEWMANAGER_UPDATEPOLLINGINTERVAL);
//			} catch (InterruptedException e) {
//		
//				e.printStackTrace();
//			}
//			}
//			
//			}catch(Exception e){
//				log.error(this.getClass(), e);
//			}
//		}
//		
//	}
//	
//
//	
//	
//	
//	
//	/*
//	 * (non-Javadoc)
//	 * @see de.webdataplatform.viewmanager.IViewManager#pollUpdates()
//	 */
//	public void process(BaseTableUpdate btu){
//		
////		log.info(this.getClass(), "Processing update: "+btu);
//		
//		String viewDefinition = btu.getViewDefinition();
//		
//
//					String viewType = viewDefinition.split(",")[0];
//					ViewMode viewMode = ViewMode.valueOf(viewType);
//		
//					
//					if(viewMode.equals(ViewMode.AGGREGATION_SUM) 
//					   || viewMode.equals(ViewMode.AGGREGATION_COUNT)
//					   || viewMode.equals(ViewMode.AGGREGATION_MIN)
//					   || viewMode.equals(ViewMode.AGGREGATION_MAX)){
//						
//						propagateUpdateAggregation(btu);
//
//					}
//
//					
//					if(viewMode.equals(ViewMode.SELECTION)){
//						propagateUpdateSelection(btu);
//					}
//					
//					
//					if(viewMode.equals(ViewMode.INDEX)){
//						propagateUpdateIndex(btu);
//						
//					}
//					
//					if(viewMode.equals(ViewMode.JOIN)){
//						propagateUpdateJoin(btu);
//						
//					}
//					
//					/** Produce duplicates for testing reasons*/
////					System.out.println(getVMName()+": Propagting duplication"+update);
////					propagateUpdate(update.getKey(), update.getOldColumn(), update.getNewColumn(), update.getEid());
//					
//
////			}else{
////			
////			}
////		}
//		
//	}
//	
//	
//	/*
//	 * (non-Javadoc)
//	 * @see de.webdataplatform.viewmanager.IViewManager#propagateUpdate(java.lang.String, de.webdataplatform.storage.BaseTableColumn, de.webdataplatform.storage.BaseTableColumn, long)
//	 */
//	public void propagateUpdateAggregation(BaseTableUpdate btu){
//		
//
//		
//		if(btu.getType().equals("Put")){
//
//			if(!btu.getOldColumns().isEmpty()){
////				log.info(this.getClass(), "----------------------Propagating Update-----------------------------");
////				log.info(this.getClass(), "--------------------------Propagating Delete-----------------------------");
////				long deleteTime = new Date().getTime();
//				propagateDeleteAggregation(btu);
////				log.info(this.getClass(), "deleteTime:"+(new Date().getTime() - deleteTime));
//			}else{
//			}
////			log.info(this.getClass(), "--------------------------Propagating Insert----------------------------");
//			
////			long insertTime = new Date().getTime();
//			propagateInsertAggregation(btu);
////			log.info(this.getClass(), "insertTime:"+(new Date().getTime() - insertTime));
//				
//		}
////		
//		if(btu.getType().equals("Delete") || btu.getType().equals("DeleteColumn") || btu.getType().equals("DeleteFamily")){
//
////			log.info(this.getClass(), "--------------------------Propagating Delete-----------------------------");
////			long deleteTime = new Date().getTime();
//			propagateDeleteAggregation(btu);
////			log.info(this.getClass(), "deletionTime:"+(new Date().getTime() - deleteTime));	
//		}
//
//		
//		
//		
//	}
//	
//	
//	
//	/*
//	 * (non-Javadoc)
//	 * @see de.webdataplatform.viewmanager.IViewManager#propagateInsert(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
//	 */
//	public void propagateInsertAggregation(BaseTableUpdate btu) {
//
//		
//		CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
//		
//		String viewType = cAV.getType();
//		ViewMode viewMode = ViewMode.valueOf(viewType);
//		
//		String keyName = cAV.getAggregationKey();
//		String valueName = cAV.getAggregationValue();
//		String key  = btu.getColumns().get(keyName);  
//		Integer deltaValue = Integer.parseInt(btu.getColumns().get(valueName));
//		
//		
////		String key = btu.getKey();
////		String valueName = btu.getViewDefinition().split(",")[1];
////		
////		String viewTableName = btu.getViewTable();
////		String regionServer = btu.getRegionServer();
////		String seqNo = btu.getSeqNo();
//		
//		
////		CreateIndexView cIV = CreateIndexView.parse(btu.getViewDefinition());
////		String key = btu.getColumns().get(cIV.getIndexKey());
////		String primKey = btu.getKey();
////		
////		log.info(this.getClass(),"index key:"+key);
////		
////		String valueName = btu.getViewDefinition().split(",")[1];
////		
////		String viewTableName = btu.getViewTable();
////		String regionServer = btu.getRegionServer();
////		String seqNo = btu.getSeqNo();
////		String value  = btu.getColumns().get(cIV.getIndexKey());  
//		
//		
//		
//		String viewTableName = btu.getViewTable();
//		String regionServer = btu.getRegionServer();
//		String seqNo = btu.getSeqNo();
//		
////		log.info(this.getClass(), "base table update insert: keyName="+keyName+", valueName="+valueName+", key="+key+", deltalValue="+deltaValue+", viewType="+viewType+", viewMode="+viewMode+", viewTableName="+viewTableName+", regionServer="+regionServer+", seqNo="+seqNo);
//		
//		
////		log.info(this.getClass(), "key"+key+", deltaValue:"+deltaValue+", viewTableName:"+viewTableName+", viewMode:"+viewMode);
//
//		
//		boolean succeed = false;
//
//		do{
//
//			boolean hasProcessed=false;
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				viewTableService.get(viewTableName, key, valueName);
//				hasProcessed = viewTableService.getWithSignature(viewTableName, key, valueName, regionServer+seqNo);
//			}
//			else viewTableService.get(viewTableName, key, valueName);
//			
//			
////			Retrieving Values
//			Integer result=null;
//			Integer value=null;
//			Result response=null;
//			
//			if(viewTableService.getResult() != null){
//				
//				response = viewTableService.getResult();
//				value = Integer.parseInt(Bytes.toString(response.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes(valueName))));
//				
////				log.info(this.getClass(), "INSERT key: "+key+", value:"+value+", deltaValue:"+deltaValue);
//				
//			}
////			else{
////				log.info(this.getClass(), "key: "+key+", value:null, deltaValue:"+deltaValue);
////			}
//			
//
//			
//			if(response != null){
//
//				if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//					if(hasProcessed){
//						log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
//						break;
//					}
//				}
//				
//				
//				if(viewMode.equals(ViewMode.AGGREGATION_SUM))result = value + deltaValue;
//				if(viewMode.equals(ViewMode.AGGREGATION_COUNT))result = value + 1;
//				if(viewMode.equals(ViewMode.AGGREGATION_MIN)){
//						if(deltaValue < value)result = deltaValue;
//						else result = value;
//				}
//				if(viewMode.equals(ViewMode.AGGREGATION_MAX)){
//					if(deltaValue > value)result = deltaValue;
//					else result = value;
//				}				
//
//				
//			}else{
//					
//				result = deltaValue;
//				if(viewMode.equals(ViewMode.AGGREGATION_COUNT))result = 1;
//			
//			}
//			
////			log.info(this.getClass(), "result: "+result);
////			if(viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX))
//				
//			
//		   Map<String, String> columns=new HashMap<String, String>();
//		   columns.put(valueName,result+"");
//			
//		   if(SystemConfig.FAULTTOLERANCE_TESTANDSET){
//				if(SystemConfig.FAULTTOLERANCE_SIGNATURES)succeed = viewTableService.checkAndPutWithSignature(viewTableName, key, valueName, (value==null)?null:value+"", columns, btu.getRegionServer()+btu.getSeqNo());
//				else succeed = viewTableService.checkAndPut(viewTableName, key, valueName, value+"", columns);
//			}
//			else{
//				viewTableService.put(viewTableName, key, columns);
//				succeed = true;
//			}
//				
//			if(!succeed)System.out.println("Update failed");
//			 
//			
//		}while(!succeed);
//		
//	}
//	
//	public void propagateDeleteAggregation(BaseTableUpdate btu) {
//
//		CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
//		
//		String viewType = cAV.getType();
//		ViewMode viewMode = ViewMode.valueOf(viewType);
//		
//		String keyName = cAV.getAggregationKey();
//		String valueName = cAV.getAggregationValue();
//		
//		String key  = btu.getOldColumns().get(keyName);  
//		Integer deltaValue = Integer.parseInt(btu.getOldColumns().get(valueName));
//		
//		String viewTableName = btu.getViewTable();
//		String baseTableName = btu.getBaseTable();
//		String regionServer = btu.getRegionServer();
//		String seqNo = btu.getSeqNo();
//		
//		
//		
//		
////		log.info(this.getClass(), "base table update insert: keyName="+keyName+", valueName="+valueName+", key="+key+", deltalValue="+deltaValue+", viewType="+viewType+", viewMode="+viewMode+", viewTableName="+viewTableName+", regionServer="+regionServer+", seqNo="+seqNo);
//
//		
//		boolean succeed = false;
//
//		do{
//		
//			boolean hasProcessed=false;
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				viewTableService.get(viewTableName, key, valueName);
//				hasProcessed = viewTableService.getWithSignature(viewTableName, key, valueName, regionServer+seqNo);
//			}
//			else viewTableService.get(viewTableName, key, valueName);
//			
//			Integer value=null;
//			Result response=null;
//			
//			if(viewTableService.getResult() != null){
//				
//				response = viewTableService.getResult();
//				value = Integer.parseInt(Bytes.toString(response.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes(valueName))));
//
//
//				
//			}
//
//			
//			Integer result=null;
//
//				if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//					if(hasProcessed){
//						log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
//						break;
//					}
//				}
//				
//				
//				if(viewMode.equals(ViewMode.AGGREGATION_SUM))result = value - deltaValue;
//				if(viewMode.equals(ViewMode.AGGREGATION_COUNT))result = value - 1;
//				if(viewMode.equals(ViewMode.AGGREGATION_MIN)){
//					
//						if(deltaValue == value){
//							
//							Integer scanResult = baseTableService.scanMinimum(baseTableName, keyName, valueName, key);
//							
//							if(scanResult == null)result = 0;
//							else result = scanResult;
//							
//						}else{
//							break;
//						}
//				}
//				if(viewMode.equals(ViewMode.AGGREGATION_MAX)){
//						if(deltaValue == value){
//							
//							Integer scanResult = baseTableService.scanMaximum(baseTableName, keyName, valueName, key);
//							
//							if(scanResult == null)result = 0;
//							else result = scanResult;
//						}else{
//							break;
//						}
//				}	
//				
//				
//					
//					if(result != 0){
//	
//					   Map<String, String> columns=new HashMap<String, String>();
//					   columns.put(valueName, result+"");
//							
//					   if(SystemConfig.FAULTTOLERANCE_TESTANDSET){
//								if(SystemConfig.FAULTTOLERANCE_SIGNATURES)succeed = viewTableService.checkAndPutWithSignature(viewTableName, key, valueName, (value==null)?null:value+"", columns, btu.getRegionServer()+btu.getSeqNo());
//								else succeed = viewTableService.checkAndPut(viewTableName, key, valueName, (value==null)?null:value+"", columns);
//					   }else{
//							viewTableService.put(viewTableName, key, columns);
//							succeed = true;
//						}
//					}else{
//						
//						if(SystemConfig.FAULTTOLERANCE_TESTANDSET){
////							succeed = viewTableService.checkAndDelete(viewTableName, key, valueName, Bytes.toBytes(value));
//							if(SystemConfig.FAULTTOLERANCE_SIGNATURES)succeed = viewTableService.checkAndDeleteWithSignature(viewTableName, key, valueName, (value==null)?null:value+"", btu.getRegionServer()+btu.getSeqNo());
//							else succeed = viewTableService.checkAndDelete(viewTableName, key, valueName, (value==null)?null:value+"");
//						}
//						else{
//							
//							Set<String> colQual = new HashSet<String>();
//							colQual.add(valueName);
//							viewTableService.delete(viewTableName, key, colQual);
//							succeed = true;
//						}
//	
//					}
//					viewTableService.get(viewTableName, key, valueName);
////				}
//				
////			}else succeed = true;
//			
//			
//
//			if(!succeed)System.out.println("Update failed");
//			 
//			
//		}while(!succeed);
//		
//	}
//	
//
//	
////////////////////////////////////////////SELECTION/////////////////////////////////////////////////////////////////////	
//	
//	
//	
//	
//	
//	
//	public void propagateUpdateSelection(BaseTableUpdate btu){
//		
//		
//		
//		if(btu.getType().equals("Put")){
//		
//			CreateSelectionView cSV = CreateSelectionView.parse(btu.getViewDefinition());
//			
//			
//			
//			String valueName = cSV.getSelectionColumn();
//			String operand = cSV.getSelectionOperation();
//			Integer selectValue = Integer.parseInt(cSV.getSelectionValue());
//			Integer value  = Integer.parseInt(btu.getColumns().get(valueName));  
//	//		log.info(this.getClass(), "base table update selection: valueName="+valueName+", operand="+operand+", conditionalValue="+selectValue+", value="+value);
//			
//			
//			
//			boolean isMatching=false;
//			
//			switch(operand){
//			case ">" : isMatching = (value > selectValue);
//				break;
//			case "<" : isMatching = (value < selectValue);
//				break;
//			case "=" : isMatching = (value == selectValue);
//				break;
//			
//			
//			}
//			
////			log.info(this.getClass(), "isMatching: "+isMatching);
//			
//			
//			if(isMatching){
//			
//		
//					
//					propagateInsertSelection(btu);
//							
//	
//			}else{
//					// value updated, check if last value has to be deleted from view
//					if(btu.getOldColumns().keySet().size() != 0){
//						
//						Integer valueOld  = Integer.parseInt(btu.getOldColumns().get(valueName));  
//						boolean isLastMatching=false;
//						switch(operand){
//						case ">" : isLastMatching = (valueOld > selectValue);
//							break;
//						case "<" : isLastMatching = (valueOld < selectValue);
//							break;
//						case "=" : isLastMatching = (valueOld == selectValue);
//							break;
//						
//						
//						}
//						
//						if(isLastMatching)propagateDeleteSelection(btu);
//					}
//			}
//		
//		}
//		
//		if(btu.getType().equals("Delete") || btu.getType().equals("DeleteColumn") || btu.getType().equals("DeleteFamily")){
//			
//			propagateDeleteSelection(btu);
//			return;	
//		}
//		
//	}
//
//
//	public void propagateInsertSelection(BaseTableUpdate btu){
//		
//		String key = btu.getKey();
//		String valueName = btu.getViewDefinition().split(",")[1];
//		
//		String viewTableName = btu.getViewTable();
//		String regionServer = btu.getRegionServer();
//		String seqNo = btu.getSeqNo();
//		
//		
//		boolean succeed = false;
////		boolean isMatching = baseTableRecord.isMatching(400, IProjectionRecord.EQUAL);				
//
//		do{
//		
//			boolean hasProcessed=false;
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				hasProcessed = viewTableService.getWithSignature(viewTableName, key, valueName, regionServer+seqNo);
//			}
//			else viewTableService.get(viewTableName, key, valueName);
//			
////			Retrieving Values
//
////			Checking signature, if already processed
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				if(hasProcessed){
//					log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
//					break;
//				}
//			}
//		   
//			
////			Putting new values
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES)viewTableService.putWithSignature(viewTableName, key,  btu.getColumns(), btu.getRegionServer()+btu.getSeqNo());
//			else viewTableService.put(viewTableName, key, btu.getColumns());
//			succeed = true;
//			   
//	
//
//
//			if(!succeed)System.out.println("Update failed");
//			 
//			
//		}while(!succeed);
//		
//	}	
//	
//	
//
//	public void propagateDeleteSelection(BaseTableUpdate btu){
//		
//		
//		String key = btu.getKey();
//		String valueName = btu.getViewDefinition().split(",")[1];
//		
//		String viewTableName = btu.getViewTable();
//		String regionServer = btu.getRegionServer();
//		String seqNo = btu.getSeqNo();
//		
//
//		
//		boolean succeed = false;
////		boolean isMatching = baseTableRecord.isMatching(400, IProjectionRecord.EQUAL);				
//
//		do{
//		
//			boolean hasProcessed=false;
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				hasProcessed = viewTableService.getWithSignature(viewTableName, key, valueName, regionServer+seqNo);
//			}
//			else viewTableService.get(viewTableName, key, valueName);
//			
//
//			
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				if(hasProcessed){
//					log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
//					break;
//				}
//			}
//			
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES)viewTableService.deleteWithSignature(viewTableName, key, btu.getColumns().keySet(),  btu.getRegionServer()+btu.getSeqNo());
//			else{
//				
//				Set<String> colQual = new HashSet<String>();
//				colQual.add(valueName);
//				viewTableService.delete(viewTableName, key, colQual);
//			}
//
//			succeed = true;   
//
//			if(!succeed)System.out.println("Update failed");
//			
//			
//		}while(!succeed);
//		
//	}	
//	
//	
//	
//	
////////////////////////////////////////////INDEX/////////////////////////////////////////////////////////////////////		
//	
//	
//	public void propagateUpdateIndex(BaseTableUpdate btu){
//		
//		
//		log.info(this.getClass(),"propagating index");
//		
//		if(btu.getType().equals("Put")){
//			
//			propagateInsertIndex(btu);
//							
//		}
//		
////		if(btu.getType().equals("Delete") || btu.getType().equals("DeleteColumn") || btu.getType().equals("DeleteFamily")){
////			
////			propagateDeleteIndex(btu);
////			return;	
////		}
//		
//	}
//
//	public void propagateInsertIndex(BaseTableUpdate btu){
//		
//		CreateIndexView cIV = CreateIndexView.parse(btu.getViewDefinition());
//		String key = btu.getColumns().get(cIV.getIndexKey());
//		String primKey = btu.getKey();
//		
//		log.info(this.getClass(),"index key:"+key);
//		
//		String valueName = btu.getViewDefinition().split(",")[1];
//		
//		String viewTableName = btu.getViewTable();
//		String regionServer = btu.getRegionServer();
//		String seqNo = btu.getSeqNo();
//		
//		
//	
//		String value  = btu.getColumns().get(cIV.getIndexKey());  
//		
//		
//		boolean succeed = false;
////		boolean isMatching = baseTableRecord.isMatching(400, IProjectionRecord.EQUAL);				
//
//
//		do{
//
//			boolean hasProcessed=false;
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				viewTableService.get(viewTableName, key, valueName);
//				hasProcessed = viewTableService.getWithSignature(viewTableName, key, valueName, regionServer+seqNo);
//			}
//			else viewTableService.get(viewTableName, key, valueName);
//			
//			
////			Retrieving Values
//			Integer result=null;
//
//			Result response=null;
//			
//			if(viewTableService.getResult() != null){
//				
//				response = viewTableService.getResult();
//				
////				log.info(this.getClass(), "INSERT key: "+key+", value:"+value+", deltaValue:"+deltaValue);
//				
//			}
//			log.info(this.getClass(),"response:"+response);
//
//			
//		   Map<String, String> columns=new HashMap<String, String>();
//		   columns.put(primKey,"sonstwas");
//		   
//		   log.info(this.getClass(),"columns:"+columns);
//			
////		   if(SystemConfig.FAULTTOLERANCE_TESTANDSET){
////				if(SystemConfig.FAULTTOLERANCE_SIGNATURES)succeed = viewTableService.checkAndPutWithSignature(viewTableName, key, valueName, (value==null)?null:value+"", columns, btu.getRegionServer()+btu.getSeqNo());
////				else succeed = viewTableService.checkAndPut(viewTableName, key, valueName, value+"", columns);
////			}
////			else{
//				
//				
//			viewTableService.put(viewTableName, key, columns);
//			succeed = true;
////			}
//				
//			if(!succeed)System.out.println("Update failed");
//			 
//			
//		}while(!succeed);
//		
//		
//	}		
///*
//	public void propagateDeleteIndex(BaseTableUpdate btu){
//		
//		
//		String key = btu.getKey();
//		String valueName = btu.getViewDefinition().split(",")[1];
//		
//		String viewTableName = btu.getViewTable();
//		String regionServer = btu.getRegionServer();
//		String seqNo = btu.getSeqNo();
//		
//
//		
//		boolean succeed = false;
////		boolean isMatching = baseTableRecord.isMatching(400, IProjectionRecord.EQUAL);				
//
//		do{
//		
//			boolean hasProcessed=false;
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				hasProcessed = viewTableService.getWithSignature(viewTableName, key, valueName, regionServer+seqNo);
//			}
//			else viewTableService.get(viewTableName, key, valueName);
//			
//
//			
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				if(hasProcessed){
//					log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
//					break;
//				}
//			}
//			
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES)viewTableService.deleteWithSignature(viewTableName, key, btu.getColumns().keySet(),  btu.getRegionServer()+btu.getSeqNo());
//			else{
//				
//				Set<String> colQual = new HashSet<String>();
//				colQual.add(valueName);
//				viewTableService.delete(viewTableName, key, colQual);
//			}
//
//			succeed = true;   
//
//			if(!succeed)System.out.println("Update failed");
//			
//			
//		}while(!succeed);
//		
//	}	*/
//	
//		
//	
/////////////////////////////////////////////////////////JOIN///////////////////////////////////////////////////////////////////////////////	
//
//	
//	public void propagateUpdateJoin(BaseTableUpdate btu){
//		
//		
//
//		
//		
////		String joinKey = btu.getColumns().get(joinKeyName); 
//		
////		log.update(this.getClass(), "base table update selection: tablename="+btu.getBaseTable()+", leftSide="+leftSide+", rightSide="+rightSide+"isLeftSide="+isLeftSide+", joinKeyName="+joinKeyName+", joinKey="+joinKey);
//
//		if(btu.getType().equals("Put")){
//			
////			propagateInsertJoin(btu);
//				
//		}
//
////		if(btu.getType().equals("Delete") || btu.getType().equals("DeleteColumn") || btu.getType().equals("DeleteFamily")){
////
////			propagateDeleteJoin(btu);
////				
////		}
//			
//
//				
//
//
//		
//		
//		
//		
//	}
//	
//
//	public void propagateInsertJoin(BaseTableUpdate btu){
//		
//
//		
////		String joinKey = btu.getColumns().get(joinKeyName); 
//
//		
//		List<JoinTablePair> joinTablePairs = JoinTablePair.parse(btu.getViewDefinition());
//		
//		
//
//		
//		boolean succeed = false;
//
//		do{
//		
//			boolean hasProcessed=false;
//
////			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
////				hasProcessed = viewTableService.getWithSignature(viewTableName, key, joinKeyName, regionServer+seqNo);
////			}
////			else viewTableService.get(viewTableName, key, joinKeyName);
////			
////			
//////			CHECKING SIGNATURE
////			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
////				if(hasProcessed){
////					log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
////					break;
////				}
////			}
//			
////			TABLE QUERY
//			
//			Result result=null;
//			
////			List<JoinTablePair> selectedPairs = JoinTablePair.getJoinTables(joinTablePairs, btu.getBaseTable());	
//				
////			computeJoinRecords(BaseTableUpdate btu)
//			
//			boolean findMatchingRow = true;
//
//			
//			
//			succeed = true;	
//			
//			if(findMatchingRow){
//				log.info(this.getClass(),"MATCHING FOUND:");
//				
//				
//				
////				log.update(this.getClass(), "join has matched row btu:"+btu+", result"+result);
//				
////				if(SystemConfig.FAULTTOLERANCE_SIGNATURES)viewTableService.putWithSignature(viewTableName, key, joinColumns, btu.getRegionServer()+btu.getSeqNo());
////				else viewTableService.put(viewTableName, key, joinColumns);
////				succeed = true;
//				
//			}else{
//				log.info(this.getClass(), "no matching row found for btu:"+btu);
//				succeed = true;
//			}
//				
//			
//			
//			
//
//			if(!succeed)System.out.println("Update failed");
//			 
//			
//		}while(!succeed);
//		
//	}		
//
//	
//	
//	
//	
//	/**
//	 * Compute join records of a base table update, that has been applied to a certain table. Computing
//	 * the join record includes resolving the join definition and computing the matching rows
//	 * @param btu
//	 */
//	public JoinNode computeJoinRecords(BaseTableUpdate btu){
//		
//		
//		List<JoinTablePair> joinTablePairs = computeJoinOrder(btu.getBaseTable(), btu.getViewDefinition());
//		
//		
//		
//		JoinNode rootNode = new JoinNode(new MatchingRow(btu.getBaseTable(), btu.getKey(), btu.getColumns()), new ArrayList<JoinNode>());
//		List<JoinNode> selectedNodes = new ArrayList<JoinNode>();
//		selectedNodes.add(rootNode);
//		
//		for (JoinTablePair joinTablePair : joinTablePairs) {
//			
//			List<JoinNode> nextSelectedNodes = new ArrayList<JoinNode>();
//			for(JoinNode selectedNode: selectedNodes){
//				
//				
//				List<MatchingRow> results = computeJoinPair(selectedNode.getMatchingRow(), joinTablePair);
//				
////				System.out.println("size: "+results.size());
////				System.out.println("size: "+results);
//				
//				if(results.size() != 0){
//					
//					for (MatchingRow matchingRow : results) {
//						JoinNode joinNode = new JoinNode(matchingRow, new ArrayList<JoinNode>());
//						selectedNode.addJoinNode(joinNode);
//						nextSelectedNodes.add(joinNode);
//					}
//
//				}
//			}
////			System.out.println("selec: "+nextSelectedNodes.size());
//			if(nextSelectedNodes.size() == 0){
////				System.out.println("No Join possible");
////				return null;
//				return null;
//				
//			}else{
//				
//				selectedNodes = nextSelectedNodes;
//			}
//
//			
//		}
//		
//		return rootNode;
//		
////		for (MatchingRow matchingRow : rootNode.returnMatchingRow(completeRecords, selectedRecord)) {
////			System.out.print(matchingRow.getPrimaryKeyString()+"_");
////		}
//		
//		
//	}
//	
//	
//
//	
//	
//	/**
//	 * 
//	 * Given the updatedTable and the view definition of the join, this procedure computes the correct
//	 * order in which the tables have to be joined. Start point is the updated base table.
//	 * @param updatedTable
//	 * @param viewDefinition
//	 * @return
//	 */
//	public List<JoinTablePair> computeJoinOrder(String updatedTable, String viewDefinition){
//		
//		
//		List<String> joinTablesToCompute = new ArrayList<String>();
//		Set<String> computedJoinTables = new HashSet<String>();
//		
//		String selectedTable = updatedTable;
//
//			
//		List<JoinTablePair> result = new ArrayList<JoinTablePair>();
//			
//		
//		do{
//			
////			System.out.println("computing joins for basetable: "+selectedTable);
//			
//			
//			List<JoinTablePair> jTPsSelectedTable = JoinTablePair.getJoinTables(JoinTablePair.parse(viewDefinition), selectedTable);
//			
//			
//			for (JoinTablePair joinTablePair : jTPsSelectedTable) {
//					
//				
//					if(!joinTablePair.getLeftTable().getTableName().equals(selectedTable) && !computedJoinTables.contains(joinTablePair.getLeftTable().getTableName()))
//						joinTablesToCompute.add(joinTablePair.getLeftTable().getTableName());
//					if(!joinTablePair.getRightTable().getTableName().equals(selectedTable) && !computedJoinTables.contains(joinTablePair.getRightTable().getTableName()))
//						joinTablesToCompute.add(joinTablePair.getRightTable().getTableName());
//					
//					if(!computedJoinTables.contains(joinTablePair.getLeftTable().getTableName()) && !computedJoinTables.contains(joinTablePair.getRightTable().getTableName())){
//
////						System.out.println("computing join table pair: "+joinTablePair);
//						result.add(joinTablePair);	
//			
//					}
//			}
//			
//			computedJoinTables.add(selectedTable);
//			selectedTable = null;
//			
//			if(joinTablesToCompute.size() > 0){
//				selectedTable = joinTablesToCompute.get(0);
//				joinTablesToCompute.remove(0);
//			}
//			
//		}while(selectedTable != null);
//		
//		return result;
//	}
//
//	
//	
//
//	
//	
//	/**
//	 * Compute joining rows between one row of table A and table B in a join pair. The methode takes 
//	 * the join key of the left table and the definition of the right table as input and
//	 * produces the matching rows as output. 
//	 * @param leftTableName
//	 * @param joinTablePair
//	 * @return
//	 */
//
//	public List<MatchingRow> computeJoinPair(MatchingRow leftTableRow, JoinTablePair joinTablePair){
//		
////		System.out.println("computing join pair: "+joinTablePair);
//		
//		JoinTable leftTable= getLeftTable(leftTableRow, joinTablePair);
//		JoinTable rightTable= getRightTable(leftTableRow, joinTablePair);
//		String joinKey =  determineJoinKey(leftTableRow, leftTable);
//		
////		System.out.println("left table= "+leftTable);
////		System.out.println("right table="+rightTable);
////		System.out.println("matchingRows: "+leftTableRow);
////		System.out.println("joinKey: "+joinKey);
//		
//		
//		return computeJoinRow(joinKey, rightTable);
//
//
//	}
//	
//	
//	public JoinTable getLeftTable(MatchingRow leftTableRow, JoinTablePair joinTablePair){
//	
//		if(joinTablePair.getLeftTable().getTableName().equals(leftTableRow.getTableNameString()))
//		  return joinTablePair.getLeftTable();
//		else return joinTablePair.getRightTable();
//	
//	}
//	
//	public JoinTable getRightTable(MatchingRow leftTableRow, JoinTablePair joinTablePair){
//		if(joinTablePair.getLeftTable().getTableName().equals(leftTableRow.getTableNameString()))
//			  return joinTablePair.getRightTable();
//			else return joinTablePair.getLeftTable();
//	}
//	
//	
//	public String determineJoinKey(MatchingRow matchingRow, JoinTable leftTable){
//		
//		String joinKey;
//		
//		if(leftTable.getJoinKey().equals("primaryKey")){
//			
//			   joinKey = Bytes.toString(matchingRow.getPrimaryKey());
//		}else{
//			   
//			   joinKey = BytesUtil.convertMapBack(matchingRow.getColumns()).get(leftTable.getJoinKey());
//			
//		}
//		return joinKey;
//	}
//	
//	
//
//
//	public List<MatchingRow>  computeJoinRow(String joinKey, JoinTable rightTable){
//		
//		List<Result> results = queryRightTable(joinKey, rightTable);
//		
////		System.out.println("result of query: "+results);
//			
//		List<MatchingRow> matchingRows = transformResults(rightTable.getTableName(), results);
//		
//
//		return matchingRows;
//	}
//
//	/**
//	 * Query right table to determine the matching rows in the a given relation
//	 * @param rightTable
//	 * @param joinKey
//	 * @return
//	 */
//	
//	private List<Result> queryRightTable(String joinKey, JoinTable rightTable) {
//		
//		
////		System.out.println("query right table: "+rightTable+" with key: "+joinKey);
//		List<Result> results = new ArrayList<Result>();
//		try {
//			
//			if(rightTable.getJoinKey().equals("primaryKey")){
////				System.out.println("joinKey: "+joinKey);
//				Result result = baseTableService.get(rightTable.getTableName(), joinKey);
//				
//				if(!result.isEmpty()){
//
//					results.add(result);
//				}
//					
//				
//			}else{
//				
//				results = baseTableService.scanValue(rightTable.getTableName(), rightTable.getJoinKey(), joinKey);
//				
//			}
//			
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//		return results;
//	}
//
//	/**
//	 * Translate results to a list of MatchingRow objects
//	 * @param results
//	 * @return
//	 */
//	private List<MatchingRow> transformResults(String tableName, List<Result> results) {
//		
////		System.out.println("results:"+results);
//		
//		List<MatchingRow> matchingRows = new ArrayList<MatchingRow>();
//		
//		if(results.size() > 0){
//			
//			for (Result result : results) {
//				
//				
//				MatchingRow matchingRow = new MatchingRow(Bytes.toBytes(tableName),  result.getRow(), result.getFamilyMap(Bytes.toBytes("colfam1")));
//				
//				matchingRows.add(matchingRow);
//				
//			}
//			
//		}
//		
//		return matchingRows;
//	}
//	
//	
//	
//	
//
//
//
//	
//	
//	public void propagateDeleteJoin(BaseTableUpdate btu){
//		
//		String key = btu.getKey();
//		String joinKeyName = btu.getViewDefinition().split(",")[3];
//
//		log.info(this.getClass(), "DELETE: key="+key);
//		
//		String viewTableName = btu.getViewTable();
//		String regionServer = btu.getRegionServer();
//		String seqNo = btu.getSeqNo();
//		
//		
//		boolean succeed = false;
//
//		do{
//		
//			boolean hasProcessed=false;
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				hasProcessed = viewTableService.getWithSignature(viewTableName, key, joinKeyName, regionServer+seqNo);
//			}
//			else viewTableService.get(viewTableName, key, joinKeyName);
//			
//			
////			CHECKING SIGNATURE
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
//				if(hasProcessed){
//					log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
//					break;
//				}
//			}
//			
//
//			if(SystemConfig.FAULTTOLERANCE_SIGNATURES)viewTableService.deleteWithSignature(viewTableName, key, btu.getColumns().keySet(),  btu.getRegionServer()+btu.getSeqNo());
//			else viewTableService.delete(viewTableName, key, btu.getColumns().keySet());
//			
//
//			succeed = true;   
//			
//			
//			
//			if(!succeed)System.out.println("Update failed");
//
//			
//		}while(!succeed);
//		
//	}
//	
//
////	
////	
////public void propagateUpdateJoin(BaseTableUpdate btu){
////		
////		
////		
////		String leftSide = btu.getViewDefinition().split(",")[1];
////		String rightSide = btu.getViewDefinition().split(",")[2];
////		String joinKeyName = btu.getViewDefinition().split(",")[3];
////		
////		boolean isLeftSide = btu.getBaseTable().equals(leftSide);
////		
////		String joinKey = btu.getColumns().get(joinKeyName); 
////		
////		log.update(this.getClass(), "base table update selection: tablename="+btu.getBaseTable()+", leftSide="+leftSide+", rightSide="+rightSide+"isLeftSide="+isLeftSide+", joinKeyName="+joinKeyName+", joinKey="+joinKey);
////		
////		
////		
////		if(isLeftSide){
////			
////			log.update(this.getClass(), "propagating leftside of join");
////
////			if(btu.getType().equals("Put")){
////				
////				propagateInsertJoin(btu);
////					
////			}
////	
////			if(btu.getType().equals("Delete") || btu.getType().equals("DeleteColumn") || btu.getType().equals("DeleteFamily")){
////	
////				propagateDeleteJoin(btu);
////					
////			}
////			
////
////				
////		}
////
////
////		
////		
////		
////		
////	}
////
////
////	public void propagateInsertJoin(BaseTableUpdate btu){
////		
////		String key = btu.getKey();
////		String leftSide = btu.getViewDefinition().split(",")[1];
////		String rightSide = btu.getViewDefinition().split(",")[2];
////		String joinKeyName = btu.getViewDefinition().split(",")[3];
////		
////		boolean isLeftSide = btu.getBaseTable().equals(leftSide);
////		
////		String joinKey = btu.getColumns().get(joinKeyName); 
////
////		
////		String viewTableName = btu.getViewTable();
////		String regionServer = btu.getRegionServer();
////		String seqNo = btu.getSeqNo();
////		
////		
////		boolean succeed = false;
////
////		do{
////		
////			boolean hasProcessed=false;
////
////			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
////				hasProcessed = viewTableService.getWithSignature(viewTableName, key, joinKeyName, regionServer+seqNo);
////			}
////			else viewTableService.get(viewTableName, key, joinKeyName);
////			
////			
//////			CHECKING SIGNATURE
////			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
////				if(hasProcessed){
////					log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
////					break;
////				}
////			}
////			
//////			TABLE QUERY
////			
////			Result result=null;
////			
////				
////				
////			try {
////				result = baseTableService.get(rightSide, joinKey);
////			} catch (RemoteException e) {
////
////				log.error(this.getClass(), e);
////			}
////				
////			
////			if(!result.isEmpty()){
////				
////				log.update(this.getClass(), "join has matched row btu:"+btu+", result"+result);
//////				Putting new values
////				
////				Map<String, String> joinColumns = new HashMap<String, String>();
////				
////				for (String columnKey : btu.getColumns().keySet()) {
////					joinColumns.put(columnKey, btu.getColumns().get(columnKey));
////				}
////				for (KeyValue keyValue : result.list()){
////					
////					joinColumns.put(Bytes.toString(keyValue.getQualifier()), Bytes.toString(keyValue.getValue()));
////				}
////				
////				
////				if(SystemConfig.FAULTTOLERANCE_SIGNATURES)viewTableService.putWithSignature(viewTableName, key, joinColumns, btu.getRegionServer()+btu.getSeqNo());
////				else viewTableService.put(viewTableName, key, joinColumns);
////				succeed = true;
////				
////			}else{
////				log.info(this.getClass(), "no matching row found for btu:"+btu);
////				succeed = true;
////			}
////			
////
////			if(!succeed)System.out.println("Update failed");
////			 
////			
////		}while(!succeed);
////		
////	}		
////	
////
////	
////	public void propagateDeleteJoin(BaseTableUpdate btu){
////		
////		String key = btu.getKey();
////		String joinKeyName = btu.getViewDefinition().split(",")[3];
////
////		log.info(this.getClass(), "DELETE: key="+key);
////		
////		String viewTableName = btu.getViewTable();
////		String regionServer = btu.getRegionServer();
////		String seqNo = btu.getSeqNo();
////		
////		
////		boolean succeed = false;
////
////		do{
////		
////			boolean hasProcessed=false;
////
////			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
////				hasProcessed = viewTableService.getWithSignature(viewTableName, key, joinKeyName, regionServer+seqNo);
////			}
////			else viewTableService.get(viewTableName, key, joinKeyName);
////			
////			
//////			CHECKING SIGNATURE
////			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
////				if(hasProcessed){
////					log.info(this.getClass(),"Update key="+key+", regionServer="+regionServer+", seqNo="+seqNo+" rejected --> It has already been processed");
////					break;
////				}
////			}
////			
////
////			if(SystemConfig.FAULTTOLERANCE_SIGNATURES)viewTableService.deleteWithSignature(viewTableName, key, btu.getColumns().keySet(),  btu.getRegionServer()+btu.getSeqNo());
////			else viewTableService.delete(viewTableName, key, btu.getColumns().keySet());
////			
////
////			succeed = true;   
////			
////			
////			
////			if(!succeed)System.out.println("Update failed");
////
////			
////		}while(!succeed);
////		
////	}
//
//	public ICommitLog getCommitLog() {
//		return commitLog;
//	}
//
//	public void setCommitLog(ICommitLog commitLog) {
//		this.commitLog = commitLog;
//	}
//
//	public AtomicLong getViewRecordUpdates() {
//		return viewRecordUpdates;
//	}
//
//	public void setViewRecordUpdates(AtomicLong viewRecordUpdates) {
//		this.viewRecordUpdates = viewRecordUpdates;
//	}
//
//	public AtomicLong getCommitLogUpdates() {
//		return commitLogUpdates;
//	}
//
//	public void setCommitLogUpdates(AtomicLong commitLogUpdates) {
//		this.commitLogUpdates = commitLogUpdates;
//	}
//
//	public ConcurrentHashMap<String, String> getMarkers() {
//		return markers;
//	}
//
//	public void setMarkers(ConcurrentHashMap<String, String> markers) {
//		this.markers = markers;
//	}
//
//	
//	
//
//
//
//	
//	
//	
	
	
}
