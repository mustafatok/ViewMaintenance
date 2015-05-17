package de.webdataplatform.viewmanager.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.BytesUtil;
import de.webdataplatform.settings.CreateAggregationView;
import de.webdataplatform.settings.CreateDeltaView;
import de.webdataplatform.settings.CreateIndexView;
import de.webdataplatform.settings.CreateReverseJoinView;
import de.webdataplatform.settings.CreateJoinView;
import de.webdataplatform.settings.CreateSelectionView;
import de.webdataplatform.settings.JoinTable;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.view.OperationMode;
import de.webdataplatform.view.TableService;
import de.webdataplatform.view.ViewMode;
import de.webdataplatform.viewmanager.CommitLog;
import de.webdataplatform.viewmanager.ICommitLog;

public class Processing implements Runnable{




	private Queue<BaseTableUpdate> incomingQueue;
	

	
	
//	
//	private QueryBaseTable queryBaseTable;
//	
//	private CalculateResult calculateResult;
//	
//	private UpdateView updateView;
//	
	private ICommitLog commitLog;
	
	
	private String vmName;
	
	private ConcurrentHashMap<String, String> markers;
	
	

	private TableService tableService;


	private Log log;
	

	
	public Processing(Log log, Queue<BaseTableUpdate> incomingQueue, String vmName){
		this.incomingQueue = incomingQueue;
		this.vmName = vmName;
		this.commitLog = new CommitLog(log, vmName);
		viewRecordUpdates = new AtomicLong();
		commitLogUpdates = new AtomicLong();
		markers = new ConcurrentHashMap<String, String>();
		this.log = log;
		
		tableService = new TableService(log);

	}
	
	private long lastMeasure;
//	private AtomicLong insertCount;
//	private AtomicLong updateCount;
//	private AtomicLong deletionCount;
	private AtomicLong viewRecordUpdates;
	private AtomicLong commitLogUpdates;
	
	@Override
	public void run() {
		

		
		while(true){
			
			try{
			
			long currentTime = new Date().getTime();
			if((currentTime - lastMeasure) > SystemConfig.VIEWMANAGER_DISPLAYINTERVAL){
				lastMeasure = currentTime;
				log.info(this.getClass()," ---------------------");
				log.info(this.getClass()," load currently at "+incomingQueue.size());
				log.info(this.getClass()," updates processed "+viewRecordUpdates.get());
				log.info(this.getClass()," ---------------------");
			}
//			long pollingTime = new Date().getTime();
			BaseTableUpdate update = incomingQueue.poll();
//			log.performance(this.getClass(), "polling time: "+(new Date().getTime() - pollingTime));
			
			if(update != null){
				if(SystemConfig.LOGGING_LOGUPDATES)log.info(this.getClass(),"processing update: "+update);
				log.performance(this.getClass(), "----------------------------");
				try{
						
					if(update.getBaseTable().startsWith(SystemConfig.MESSAGES_MARKERPREFIX)){
						log.info(this.getClass(),"marker received, putting to map: "+update);
						markers.put(update.getBaseTable(), update.getRegionServer());
						continue;
					}	
					if(!update.getViewTable().equals("zfinish_marker") && update.getKey().contains("finishmarker")){

						tableService.put(Bytes.toBytes(update.getViewTable()), Bytes.toBytes(update.getKey()), Bytes.toBytes("colfam1"), BytesUtil.convertMap(update.getColumns()), null);
						continue;
					}
					if(update.getViewTable().equals("zfinish_marker")){

						if(update.getKey().contains("finishmarker")){
							tableService.put(Bytes.toBytes("finish_markers"), Bytes.toBytes(update.getKey()), Bytes.toBytes("colfam1"), BytesUtil.convertMap(update.getColumns()), null);
						}
						continue;
					}
					
					long processingTime = new Date().getTime();
					process(update);
					log.performance(this.getClass(), "processing time: "+(new Date().getTime() - processingTime));

					viewRecordUpdates.incrementAndGet();
					
//					log.info(this.getClass(),"writing update to commit log: "+update);
					
					long walTime = new Date().getTime();
//					if(SystemConfig.FAULTTOLERANCE_COMMITLOG){
		
						//Commit Table
						
//						commitLog.append(vmName, update);
//					}
					
					
//					if(update.getType().equals("Put")){
//						Map<byte[], byte[]> commitRecord = BytesUtil.convertMap(update.getColumns());
//						tableService.put(Bytes.toBytes(update.getBaseTable()+"_commit"), Bytes.toBytes(update.getKey()), commitRecord);
//					}
//					
//					if(update.getType().equals("Delete") || update.getType().equals("DeleteColumn") || update.getType().equals("DeleteFamily")){
//						
//						List<byte[]> columns = BytesUtil.convertMapToList(BytesUtil.convertMap(update.getColumns()));
//						tableService.delete(Bytes.toBytes(update.getBaseTable()+"_commit"), Bytes.toBytes(update.getKey()), columns);
//					}
//						
//						
					

					
					commitLogUpdates.incrementAndGet();
					log.performance(this.getClass(), "walTime time: "+(new Date().getTime() - walTime));
					
				}catch(Exception e){
					
					log.info(this.getClass(), "errornous update:"+update.convertToString());
					log.error(this.getClass(), e);
		
				}
				
			}
			
		
			if(incomingQueue.size() == 0){
			try {
				Thread.sleep(SystemConfig.VIEWMANAGER_UPDATEPOLLINGINTERVAL);
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			}
			}
			
			}catch(Exception e){
				log.error(this.getClass(), e);
			}
		}
		
	}
	

	
	
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#pollUpdates()
	 */
	public void process(BaseTableUpdate btu){

					propagateUpdate(btu);
					

		
	}
	
	

	

	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateUpdate(java.lang.String, de.webdataplatform.storage.BaseTableColumn, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateUpdate(BaseTableUpdate btu){
		

		String region = btu.getRegion();
		String seqNo = btu.getSeqNo();
		String signature = region+"_"+seqNo;
		
//		setOldBaseRecord(btu);
		
		
		btu.setColumns(btu.getColumnsWithoutSignatures());
		btu.setOldColumns(btu.getOldColumnsWithoutSignatures());
		
		log.updates(this.getClass(), "btu: "+btu);
		log.performance(this.getClass(), "btu: "+btu);
		
//		if(btu.getColumns().isEmpty() && btu.getOldColumns().isEmpty())return;

		
		if(btu.getViewMode().equals(ViewMode.AGGREGATION_COUNT) || 
				btu.getViewMode().equals(ViewMode.AGGREGATION_SUM) ||
				btu.getViewMode().equals(ViewMode.AGGREGATION_MIN) ||
				btu.getViewMode().equals(ViewMode.AGGREGATION_MAX) ||
				btu.getViewMode().equals(ViewMode.INDEX) ||
				btu.getViewMode().equals(ViewMode.REVERSE_JOIN)){
			
			
			btu.setColumns(removeNullValues(btu.getColumns()));
			btu.setOldColumns(removeNullValues(btu.getOldColumns()));
			
			if(btu.getType().equals("Put")){
				if(btu.getColumns().isEmpty() && !btu.getOldColumns().isEmpty()){
					btu.setType("Delete");
					
				}
			}
			
		}
		if(btu.getViewMode().equals(ViewMode.JOIN)){
			
//				log.info(this.getClass(), "Nullvalues:"+containsNullValues(btu.getColumns()));
				if(containsNullValues(btu.getColumns())){
					btu.setType("Delete");
//					log.info(this.getClass(), "Nullvalues:"+btu.getColumns());
					
				}
			
		}
			
		if(btu.getType().equals("Put")){

			if(!btu.getOldColumns().isEmpty()){
				long deletionTime = new Date().getTime();
				propagate(btu, OperationMode.DELETE, signature+"_1");
				log.performance(this.getClass(), "deletionTime time: "+(new Date().getTime() - deletionTime));
				
				long insertionTime = new Date().getTime();
				propagate(btu, OperationMode.INSERT, signature+"_2");
				log.performance(this.getClass(), "insertionTime time: "+(new Date().getTime() - insertionTime));
				
			}else{
				
				long insertionTime = new Date().getTime();
				propagate(btu, OperationMode.INSERT, signature);
				log.performance(this.getClass(), "insertionTime time: "+(new Date().getTime() - insertionTime));
			}
			
				
		}
	
		if(btu.getType().equals("Delete") || btu.getType().equals("DeleteColumn") || btu.getType().equals("DeleteFamily")){

			propagate(btu, OperationMode.DELETE, signature);
		}
			
		
		

		
		
		
//		if(btu.getPropagationMode().equals(PropagationMode.INSERT))propagate(btu, PropagationMode.INSERT, signature);
//		
//		if(btu.getPropagationMode().equals(PropagationMode.UPDATE)){
//			propagate(btu, PropagationMode.DELETE, signature+"_1");
//			propagate(btu, PropagationMode.INSERT, signature+"_2");
//		}
//		if(btu.getPropagationMode().equals(PropagationMode.DELETE))propagate(btu, PropagationMode.DELETE, signature);
		

		log.updates(this.getClass(), "-------------------------------");
		
		
		
	}
	
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateInsert(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagate(BaseTableUpdate btu, OperationMode propagationMode, String signature) {

		ViewMode viewMode = ViewMode.valueOf(btu.getViewDefinition().split(",")[0]);
//		log.info(this.getClass(), "Calculating "+propagationMode.toString()+" "+viewMode.toString());

		String key="";
		List<String> joinKeys;
//		String checkQualifier="";
//		String checkValue="";

		
				
		List<byte[]> getColumns = new ArrayList<byte[]>();
		List<String> colFams = null;
		
		
		
		long getKeyTime = new Date().getTime();	
		key = getViewRecordKey(btu, propagationMode);

		if(viewMode.equals(ViewMode.JOIN)){
			
			joinKeys = new ArrayList<String>();
			
		}
		
		log.performance(this.getClass(), "key:"+key);
//		log.info(this.getClass(), "key:"+key);
		log.performance(this.getClass(), "getKey time: "+(new Date().getTime() - getKeyTime));
		
		getColumns = getViewRecordColumns(btu);
		colFams = getViewRecordColFam(btu);
		
//		log.info(this.getClass(), "colFam:"+colFam);
//		getList.add(Bytes.toBytes(signature));
		
//		log.info(this.getClass(), "getList:"+BytesUtil.listToString(getColumns));
//		log.info(this.getClass(), "ViewTable:"+btu.getViewTable());
		
		Boolean succeed = false;
		Result oldViewRecord=null;


		do{
			
			long retrivalTime = new Date().getTime();
			oldViewRecord = retrieveViewRecord(btu, key, BytesUtil.convertList(colFams), getColumns, Bytes.toBytes(signature));
//			log.info(this.getClass(), "oldViewRecord:"+BytesUtil.convertMapBack(oldViewRecord));
			log.performance(this.getClass(), "retrivalTime time: "+(new Date().getTime() - retrivalTime));
			
//			log.info(this.getClass(), "oldViewRecord: "+BytesUtil.convertMapBack(oldViewRecord));
			
			boolean hasProcessed;
			if(oldViewRecord == null)hasProcessed = false;
			else hasProcessed = BytesUtil.mapContains(Bytes.toBytes(signature), oldViewRecord.getFamilyMap(Bytes.toBytes("sigfam1")));
			
			
//			System.out.println("oldViewRecord:"+BytesUtil.mapToString(oldViewRecord));
//			System.out.println("hasProcessed: "+hasProcessed);

				
			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
				if(hasProcessed){
					log.info(this.getClass(),"Update key="+key+", signature="+signature+" rejected --> It has already been processed");
					break;
				}
			}
				
			long updateView = new Date().getTime();	
			succeed = updateView(btu, propagationMode, key, colFams, oldViewRecord, signature);
			log.performance(this.getClass(), "updateView time: "+(new Date().getTime() - updateView));
			
			
		   
			if(succeed == null)break;	
			if(!succeed)log.info(this.getClass(), "Concurrent Update failed");
			 
			
		}while(!succeed);
		
	}
	
	private List<String> getViewRecordColFam(BaseTableUpdate btu) {
		
		ViewMode viewMode = btu.getViewMode();
		List<String> colFams = new ArrayList<String>();
		
		
		if(!viewMode.equals(ViewMode.REVERSE_JOIN)){
			
			colFams.add("colfam1");
		}
		
		if(viewMode.equals(ViewMode.REVERSE_JOIN)){

			String baseTable = btu.getBaseTable();
//			String colFamBaseRecord = btu.getColumnFamily();
			
					
					
			CreateReverseJoinView cRJV = CreateReverseJoinView.parse(btu.getViewDefinition());
			
			for (JoinTable joinTable : cRJV.getJoinTables()) {
				
				if(!btu.getBaseTable().equals(joinTable.getTableName())){
					colFams.add(joinTable.getTableName()+"fam1");
				}
				
			}
			
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getLeftTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getRightTable().getTableName()+"fam1";
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getRightTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getLeftTable().getTableName()+"fam1";
//			colFam = Bytes.toBytes(btu.getBaseTable());
			
		}
		
//		log.info(this.getClass(), "colFams:"+colFams);
		return colFams;
	}
	
//	private String getBaseRecordColFam(BaseTableUpdate btu) {
//		
//		ViewMode viewMode = btu.getViewMode();
//		String colFam = null;
//
//		colFam = "colfam1";
//		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
//			String baseTable = btu.getBaseTable();
//			CreateReverseJoinView cJP = CreateReverseJoinView.parse(btu.getViewDefinition());
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getLeftTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getLeftTable().getTableName()+"fam1";
//			if(baseTable.equals(cJP.getJoinPairs().get(0).getRightTable().getTableName()))colFam = cJP.getJoinPairs().get(0).getRightTable().getTableName()+"fam1";
//		
//		}
//		
//		return colFam;
//	}
	
	

	private String getViewRecordKey(BaseTableUpdate btu, OperationMode propagationMode) {
		
		String key = "";
		
		ViewMode viewMode = btu.getViewMode();
//		System.out.println("Calculating "+propagationMode.toString()+" "+viewMode.toString());


		Map<String, String> columns=null;
		
		if(propagationMode.equals(OperationMode.INSERT))columns = btu.getColumns(); 
		if(propagationMode.equals(OperationMode.DELETE))columns = btu.getOldColumns(); 
		
		

		if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
		
			CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
			
			key = columns.get(cAV.getAggregationKey());
			
//			getList.add(Bytes.toBytes(cAV.getAggregationValue()));
			
		}
		if(viewMode.equals(ViewMode.SELECTION)){
			
			key = btu.getKey();
			

		}
		
		if(viewMode.equals(ViewMode.INDEX)){
			CreateIndexView cIV = CreateIndexView.parse(btu.getViewDefinition());
			key = columns.get(cIV.getIndexKey());
	
		}
		
		if(viewMode.equals(ViewMode.DELTA)){
			key = btu.getKey();
	
		}
		
		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
			
			CreateReverseJoinView cJPV = CreateReverseJoinView.parse(btu.getViewDefinition());
			key = columns.get(cJPV.getJoinTables().get(0).getJoinKey());

		
		}
		if(viewMode.equals(ViewMode.JOIN)){
			
			
		}
		

		return key;
	}



	
	private List<byte[]> getViewRecordColumns(BaseTableUpdate btu) {
		
		ViewMode viewMode = btu.getViewMode();
		List<byte[]> getList = new ArrayList<byte[]>();
		
		if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
			
			CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
			getList.add(Bytes.toBytes(cAV.getAggregationValue()));
			

			
		}
		if(viewMode.equals(ViewMode.DELTA)){
			
			CreateDeltaView cDV = CreateDeltaView.parse(btu.getViewDefinition());
			
			for(String key : cDV.getColumns()){
				
				getList.add(Bytes.toBytes(key+"_new"));
			}
			
		}
		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
			getList=null;
		}
		
		return getList;
	}
	




	public Result retrieveViewRecord(BaseTableUpdate btu, String viewRecordKey, List<byte[]> colFams, List<byte[]> cols, byte[] signature){
		
		
		if((viewRecordKey == null || viewRecordKey.equals(""))){
			
			return Result.EMPTY_RESULT;
		}

		
		return tableService.get(Bytes.toBytes(btu.getViewTable()), Bytes.toBytes(viewRecordKey), colFams, cols, signature);
	}

	
	private Boolean updateView(BaseTableUpdate btu, OperationMode propagationMode, String viewRecordKey, List<String> colFams, Result oldVM, String signature){
		
		ViewMode viewMode = btu.getViewMode();
		String viewTableName = btu.getViewTable();
		Boolean succeed=false;
		Map<String, String> columns=null;
		Map<byte[], byte[]> newViewRecord = new HashMap<byte[], byte[]>();
		List<byte[]> deleteViewRecord = new ArrayList<byte[]>();

		
		columns = btu.getColumns(); 
		
//		if(propagationMode.equals(PropagationMode.INSERT))columns = btu.getColumns(); 
//		if(propagationMode.equals(PropagationMode.DELETE))columns = btu.getOldColumns(); 
		
//		log.info(this.getClass(), "columns: "+columns);
		
		
		if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
			
			Map<byte[], byte[]> oldViewRecord=null;
			
			if(oldVM != null)oldViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(0)));
			
			CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
			log.updates(this.getClass(), "cAV: "+cAV);
			
			String keyName = cAV.getAggregationKey();
			String valueName = cAV.getAggregationValue();
			
			
			Long oldValue = null;
			if(oldViewRecord != null){
				if(oldViewRecord.get(Bytes.toBytes(cAV.getAggregationValue())) != null){
					oldValue = Long.parseLong(Bytes.toString(oldViewRecord.get(Bytes.toBytes(cAV.getAggregationValue()))));
				}
			}else{
				oldValue = 0l;
			}
			Long deltaValue=null;
			if(propagationMode.equals(OperationMode.INSERT))deltaValue=Long.parseLong(columns.get(cAV.getAggregationValue()));
			if(propagationMode.equals(OperationMode.DELETE))deltaValue=Long.parseLong(btu.getOldColumns().get(cAV.getAggregationValue()));
			
			Long result = null;
			
			if(viewMode.equals(ViewMode.AGGREGATION_SUM)){
				
				if(propagationMode.equals(OperationMode.INSERT))result = oldValue + deltaValue;
				if(propagationMode.equals(OperationMode.DELETE))result = oldValue - deltaValue;
				
			}
			if(viewMode.equals(ViewMode.AGGREGATION_COUNT)){
				if(propagationMode.equals(OperationMode.INSERT))result = oldValue + 1;
				if(propagationMode.equals(OperationMode.DELETE))result = oldValue - 1;
			}
			if(viewMode.equals(ViewMode.AGGREGATION_MIN)){
				
				if(propagationMode.equals(OperationMode.INSERT)){
					
					newViewRecord.put(Bytes.toBytes(btu.getKey()), Bytes.toBytes(String.valueOf(deltaValue)));
					if(oldValue == 0)oldValue = Long.MAX_VALUE;
					if(deltaValue < oldValue){
//						log.info(this.getClass(), "oldvalue: "+oldValue);
//						log.info(this.getClass(), "deltaValue: "+deltaValue);
						result = deltaValue;
					}
					else result = oldValue;
				}
				if(propagationMode.equals(OperationMode.DELETE)){
					
					deleteViewRecord.add(Bytes.toBytes(String.valueOf(btu.getKey())));
					deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, deleteViewRecord, signature);
					deleteViewRecord = new ArrayList<byte[]>();
					
					log.updates(this.getClass(), "deltaValue: "+deltaValue);
					log.updates(this.getClass(), "oldValue: "+oldValue);
					log.updates(this.getClass(), "equals: "+deltaValue.equals(oldValue));
					if(deltaValue.equals(oldValue)){
						
//						log.info(this.getClass(), "Minimum deleted");
						
						Map<String, String> res = BytesUtil.convertMapBack(tableService.get(Bytes.toBytes(btu.getViewTable()), Bytes.toBytes(viewRecordKey), BytesUtil.convertList(colFams), new ArrayList<byte[]>(), null).getFamilyMap(Bytes.toBytes(colFams.get(0))));
						res.remove(btu.getKey());
						res.remove(valueName);
//						log.info(this.getClass(), "res: "+res);
						
						Long smallestValue=Long.MAX_VALUE;
					
						for (String bs : res.keySet()) {
							
							try{
								Long val = Long.parseLong(res.get(bs));
								if(val <= smallestValue)smallestValue =val;
							
							}catch(Exception e){
								
							}
							
						}
						if(smallestValue == Long.MAX_VALUE)smallestValue = 0l;
						result = smallestValue;
						
//						log.info(this.getClass(), "new smallest value: "+smallestValue);

						
					}else{
						return null;
					}
				}
					
					
			}
			if(viewMode.equals(ViewMode.AGGREGATION_MAX)){
				
				if(propagationMode.equals(OperationMode.INSERT)){
					newViewRecord.put(Bytes.toBytes(btu.getKey()), Bytes.toBytes(String.valueOf(deltaValue)));
					if(oldValue == 0)oldValue = Long.MIN_VALUE;
	
						if(deltaValue > oldValue)result = deltaValue;
						else result = oldValue;
					
				}
				if(propagationMode.equals(OperationMode.DELETE)){
					
					deleteViewRecord.add(Bytes.toBytes(String.valueOf(btu.getKey())));
					deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, deleteViewRecord, signature);
					deleteViewRecord = new ArrayList<byte[]>();
//					System.out.println("deltaValue: "+deltaValue);
//					System.out.println("oldValue: "+oldValue);
					if(deltaValue.equals(oldValue)){
						
//						log.info(this.getClass(), "Maximum deleted");
						
						Map<String, String> res = BytesUtil.convertMapBack(tableService.get(Bytes.toBytes(btu.getViewTable()), Bytes.toBytes(viewRecordKey), BytesUtil.convertList(colFams), new ArrayList<byte[]>(),null).getFamilyMap(Bytes.toBytes(colFams.get(0))));
						res.remove(btu.getKey());
						res.remove(valueName);
//						log.info(this.getClass(), "res: "+res);
						
						
						Long biggestValue=Long.MIN_VALUE;
						
						for (String bs : res.keySet()) {
							
							try{
								Long val = Long.parseLong(res.get(bs));
								if(val >= biggestValue)biggestValue =val;
							
							}catch(Exception e){
								
							}
							
						}
//						log.info(this.getClass(), "new biggest value: "+biggestValue);
						
						if(biggestValue == Long.MIN_VALUE)biggestValue = 0l;
						result = biggestValue;
						
						
					}else{
						return null;
					}
				}	
			}

			
			
			//UPDATE FOR COUNT,SUM,MIN,MAX

			String checkQualifier = cAV.getAggregationValue();
			String checkValue = null;
			if(oldViewRecord != null){
				checkValue = Bytes.toString(oldViewRecord.get(Bytes.toBytes(cAV.getAggregationValue())));
			}
			if(result != 0){
				
				newViewRecord.put(Bytes.toBytes(valueName), Bytes.toBytes(String.valueOf(result)));
				succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), checkQualifier, checkValue, newViewRecord, signature);
			}else{
				
				deleteViewRecord.add(Bytes.toBytes(valueName));
				succeed = deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), checkQualifier, checkValue, deleteViewRecord, signature);
			}
			
		}
		
		
		if(viewMode.equals(ViewMode.SELECTION)){
			
			CreateSelectionView cSV = CreateSelectionView.parse(btu.getViewDefinition());
			
			String valueName = cSV.getSelectionColumn();
			String operand = cSV.getSelectionOperation();
			Integer selectValue = Integer.parseInt(cSV.getSelectionValue());
			Integer value= Integer.parseInt(columns.get(valueName));
	//		log.info(this.getClass(), "base table update selection: valueName="+valueName+", operand="+operand+", conditionalValue="+selectValue+", value="+value);
			
			
			
			boolean isMatching=false;
			
			switch(operand){
			case ">" : isMatching = (value > selectValue);
				break;
			case "<" : isMatching = (value < selectValue);
				break;
			case "=" : isMatching = (value.equals(selectValue));
				break;
			
			
			}
			if(!isMatching){
//				System.out.println("selection condition not met");
				return null;
			}

			
			if(propagationMode.equals(OperationMode.INSERT)){
				
				newViewRecord = BytesUtil.convertMap(columns);
				succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, newViewRecord, signature);
			}	
			if(propagationMode.equals(OperationMode.DELETE)){
				
				deleteViewRecord.addAll(BytesUtil.convertMap(columns).keySet());
				succeed = deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, deleteViewRecord, signature);
			}

		}

		
		if(viewMode.equals(ViewMode.DELTA)){
			
			CreateDeltaView cDV = CreateDeltaView.parse(btu.getViewDefinition());
//			log.info(this.getClass(), "cDV: "+cDV);
			Map<byte[], byte[]> oldViewRecord=null;
			if(oldVM != null)oldViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFams.get(0)));
	
			if(propagationMode.equals(OperationMode.INSERT)){
				
				
				if(oldViewRecord == null || oldViewRecord.keySet() == null || oldViewRecord.keySet().size() == 0){
					
					
					for (String key : columns.keySet()) {				
						newViewRecord.put( Bytes.toBytes(key+"_old") , null);
						newViewRecord.put( Bytes.toBytes(key+"_new") , Bytes.toBytes(columns.get(key)));
					}
					
				}else{
					
					for (byte[] key : oldViewRecord.keySet()) {				
						newViewRecord.put( Bytes.toBytes(Bytes.toString(key).replace("_new", "")+"_old") , oldViewRecord.get(key));
					}
					
					for (String key : columns.keySet()) {
						newViewRecord.put( Bytes.toBytes(key+"_new") , Bytes.toBytes(columns.get(key)));
					}
					
					
				}
				
				
			}	
			if(propagationMode.equals(OperationMode.DELETE)){
				
				for (byte[] key : oldViewRecord.keySet()) {				
					newViewRecord.put( Bytes.toBytes(Bytes.toString(key).replace("_new", "")+"_old") , oldViewRecord.get(key));
					newViewRecord.put( Bytes.toBytes(Bytes.toString(key)) , null);
				}
				

			}
//			log.info(this.getClass(), "viewRecordKey: "+viewRecordKey);
//			log.info(this.getClass(), "newViewRecord: "+BytesUtil.mapToString(newViewRecord));
			succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, newViewRecord, signature);
		}
		
		if(viewMode.equals(ViewMode.INDEX)){
			
			
			if(propagationMode.equals(OperationMode.INSERT)){
				
				newViewRecord.put(Bytes.toBytes(String.valueOf(btu.getKey())), Bytes.toBytes(""));
				succeed = insertToViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, newViewRecord, signature);
			}	
			if(propagationMode.equals(OperationMode.DELETE)){
				
				deleteViewRecord.add(Bytes.toBytes(String.valueOf(btu.getKey())));
				succeed = deleteFromViewTable(viewTableName, viewRecordKey, colFams.get(0), null, null, deleteViewRecord, signature);
			}
		}
		
		if(viewMode.equals(ViewMode.REVERSE_JOIN)){
			
				
			Put put = new Put(Bytes.toBytes(viewRecordKey));
			
			for (String colFam : colFams) {
				
				Map<byte[], byte[]> oldViewRecord=null;
				if(oldVM != null)oldViewRecord = oldVM.getFamilyMap(Bytes.toBytes(colFam));
				
				if(oldViewRecord != null){
					
					for (byte[] bs : oldViewRecord.keySet()) {
	//					log.info(this.getClass(), "oldJPRecord"+Bytes.toString(oldViewRecord.get(bs))+"");
						if(oldViewRecord.get(bs)!= null && !Bytes.toString(oldViewRecord.get(bs)).equals("") && !Bytes.toString(oldViewRecord.get(bs)).equals(" "))
							put.add(Bytes.toBytes(colFam), bs, oldViewRecord.get(bs));
						
					}
					
					
				}
			}
			
			String valueString="";
			
			if(propagationMode.equals(OperationMode.INSERT)){
				
				for(String colKey : btu.getColumns().keySet()){
					valueString+=colKey+"|"+btu.getColumns().get(colKey)+"|";
				}
				put.add(Bytes.toBytes(btu.getBaseTable()+"fam1"), Bytes.toBytes(btu.getKey()), Bytes.toBytes(valueString));
//				log.info(this.getClass(), "INSERT: colfam["+getBaseRecordColFam(btu)+"], col["+btu.getKey()+"], value["+valueString+"]");
			}	 
			if(propagationMode.equals(OperationMode.DELETE)){
				
				put.add(Bytes.toBytes(btu.getBaseTable()+"fam1"), Bytes.toBytes(btu.getKey()), null);
//				log.info(this.getClass(), "DELETE: colfam["+getBaseRecordColFam(btu)+"], col["+btu.getKey()+"], value[null]");

			}					
				
//			log.info(this.getClass(), "put: "+put.toString());
			succeed = insertToViewTable(viewTableName, viewRecordKey, btu.getBaseTable()+"fam1", null, null, put, signature);

		}
		if(viewMode.equals(ViewMode.JOIN)){
			
			CreateJoinView cJV = CreateJoinView.parse(btu.getViewDefinition());

//			log.updates(this.getClass(), "cJV: "+cJV);
			
//			Map<String, String> cols = removeNullValues(btu.getColumns());
//			log.info(this.getClass(), "cols: "+btu.getColumns());
			Map<String, Map<String, String>> joinKeys = constructJoinKeys(cJV, btu.getColFamilies(), btu.getColumns());
			

			if(propagationMode.equals(OperationMode.INSERT) && btu.getColumns().size() > 1){
				
				
				for (String joinKey : joinKeys.keySet()) {
					
					newViewRecord = BytesUtil.convertMap(joinKeys.get(joinKey));
//					log.info(this.getClass(), "insertJoinKey: "+joinKey);
//					log.info(this.getClass(), "valueMap: "+joinKeys.get(joinKey));
					succeed = insertToViewTable(viewTableName, joinKey, colFams.get(0), null, null, newViewRecord, signature);
				}
				
				
			}
			if(propagationMode.equals(OperationMode.DELETE) && btu.getColumns().size() > 1){
				
			
				
				for (String joinKey : joinKeys.keySet()) {
					
					newViewRecord = BytesUtil.convertMap(columns);
//					log.info(this.getClass(), "deleteJoinKey: "+joinKey);
//					log.info(this.getClass(), "cols: "+joinKeys.get(joinKey).keySet());
					for (String key : joinKeys.get(joinKey).keySet()) {
						
						deleteViewRecord.add(Bytes.toBytes(key));
					}
//					log.info(this.getClass(), "colList: "+BytesUtil.listToString(deleteViewRecord));
					succeed = deleteFromViewTable(viewTableName, joinKey, colFams.get(0), null, null, deleteViewRecord, signature);
					deleteViewRecord = new ArrayList<byte[]>();
				}
				
				
			}
			
			
			
			return true;
		}
			

		return succeed;
	}
	

	public boolean containsNullValues(Map<String, String> map){
		
		for (String key : map.keySet()) {
			
			if(map.get(key) == null || map.get(key).equals("") || map.get(key).equals(" ") || map.get(key).equals("null"))return true;
		}
		
		return false;
		
	}
	
	public Map<String, String> removeNullValues(Map<String, String> map){
		
		Map<String, String> result = new HashMap<String, String>();
		
		for (String key : map.keySet()) {
			
			if(map.get(key) != null  && !map.get(key).equals("") && !map.get(key).equals(" "))result.put(key, map.get(key));
		}
		
		return result;
		
	}

	
	public Map<String, Map<String, String>> constructJoinKeys(CreateJoinView cJV, Map<String, String> colFams, Map<String, String> cols){
		
		
		Map<String, Map<String, String>> resultKeys = new HashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> tempKeys = new HashMap<String, Map<String, String>>();
		
		for (String keyPart : cJV.getCompositeKey()) {
			
			for (String colKey : cols.keySet()) {
				
				if(keyPart.equals(colFams.get(colKey))){
					
					if(resultKeys.size() == 0){
						  
						Map<String, String> valueMap = new HashMap<String, String>();
						valueMap.put(colKey, cols.get(colKey));
						tempKeys.put(colKey, valueMap);
						  
					}else{
						for (String resultKey : resultKeys.keySet()) {
							Map<String, String> valueMap = new HashMap<String, String>();
							valueMap.put(colKey, cols.get(colKey));	
							valueMap.putAll(resultKeys.get(resultKey));
							tempKeys.put(resultKey+="_"+colKey, valueMap);
						}
					}
				}
				
			}
			resultKeys = new HashMap<String, Map<String,String>>();
			if(tempKeys.size() == 0)return new HashMap<String, Map<String,String>>();
			resultKeys.putAll(tempKeys);
			tempKeys = new HashMap<String, Map<String,String>>();
			
		}
		return resultKeys;
	}
	
	
	private boolean insertToViewTable(String viewTableName, String key, String columnFamily, String checkQualifier, String checkValue, Map<byte[], byte[]> newViewRecord, String signature) {
		
		Put put = new Put(Bytes.toBytes(key));
		
		for (byte[] columnQualifier : newViewRecord.keySet()) {
			byte[] value = newViewRecord.get(columnQualifier);
			put.add(Bytes.toBytes(columnFamily), columnQualifier, value);	
		}
		return insertToViewTable(viewTableName, key, columnFamily, checkQualifier, checkValue, put,signature);
	}



	private boolean insertToViewTable(String viewTableName, String key, String columnFamily, String checkQualifier, String checkValue, Put put, String signature) {
		
		
		
		if(!SystemConfig.FAULTTOLERANCE_SIGNATURES)signature=null;
		log.updates(this.getClass(), "inserting into view table :"+viewTableName+", key: "+key+", signature: "+signature+", checkQualifier:"+checkQualifier+"checkValue:"+checkValue+" columns: "+put.toString());

		boolean succeed;
		if(SystemConfig.FAULTTOLERANCE_TESTANDSET && checkQualifier != null){
			   
			byte[] checkVal=null;
			if(checkValue != null)checkVal = Bytes.toBytes(checkValue);
			
			succeed = tableService.checkAndPut(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), Bytes.toBytes(checkQualifier), checkVal, put,  (signature != null)?Bytes.toBytes(signature):null);

		}
		else{
			tableService.put(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), put, (signature != null)?Bytes.toBytes(signature):null);
			succeed = true;
		}
		return succeed;
	}
	
	private boolean deleteFromViewTable(String viewTableName, String key, String columnFamily, String checkQualifier, String checkValue, List<byte[]> deleteViewRecord, String signature) {
		
		log.updates(this.getClass(), "deleting from view table :"+viewTableName+", key: "+key+", signature: "+signature+", checkQualifier:"+checkQualifier+"checkValue:"+checkValue+" columns: "+BytesUtil.listToString(deleteViewRecord));

		boolean succeed=false;
		if(SystemConfig.FAULTTOLERANCE_TESTANDSET && checkQualifier != null){
			   
			byte[] checkVal=null;
			if(checkValue != null)checkVal = Bytes.toBytes(checkValue);
			
			
			if(SystemConfig.FAULTTOLERANCE_SIGNATURES)
			succeed = tableService.checkAndDeleteWithSignature(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), Bytes.toBytes(checkQualifier), checkVal, deleteViewRecord, Bytes.toBytes(signature));
			else{
				succeed = tableService.checkAndDelete(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), Bytes.toBytes(checkQualifier), checkVal, deleteViewRecord);
			}

		}
		else{
			
			if(SystemConfig.FAULTTOLERANCE_SIGNATURES){
				tableService.deleteWithSignature(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), deleteViewRecord, Bytes.toBytes(signature));
				succeed = true;
			}else{
				tableService.delete(Bytes.toBytes(viewTableName), Bytes.toBytes(key), Bytes.toBytes(columnFamily), deleteViewRecord);
				succeed = true;
			}
		}
		return succeed;
	}
	

	
	
	

	
	





	public ICommitLog getCommitLog() {
		return commitLog;
	}

	public void setCommitLog(ICommitLog commitLog) {
		this.commitLog = commitLog;
	}

	public AtomicLong getViewRecordUpdates() {
		return viewRecordUpdates;
	}

	public void setViewRecordUpdates(AtomicLong viewRecordUpdates) {
		this.viewRecordUpdates = viewRecordUpdates;
	}

	public AtomicLong getCommitLogUpdates() {
		return commitLogUpdates;
	}

	public void setCommitLogUpdates(AtomicLong commitLogUpdates) {
		this.commitLogUpdates = commitLogUpdates;
	}

	public ConcurrentHashMap<String, String> getMarkers() {
		return markers;
	}

	public void setMarkers(ConcurrentHashMap<String, String> markers) {
		this.markers = markers;
	}

	
	



	
	
	
	
	
}
