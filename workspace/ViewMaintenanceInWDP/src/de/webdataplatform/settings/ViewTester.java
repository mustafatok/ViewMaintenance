package de.webdataplatform.settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

import de.webdataplatform.log.Log;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.test.Sum;
import de.webdataplatform.test.Sum.SumRequest;
import de.webdataplatform.test.Sum.SumResponse;
import de.webdataplatform.test.Sum.SumService;
import de.webdataplatform.view.Join;
import de.webdataplatform.view.TableService;
import de.webdataplatform.view.ViewMode;

public class ViewTester {

	
	
	
//	static Map<String, Long> view = new HashMap<String, Long>();
//	static Map<String, List<String>> viewIndex = new HashMap<String, List<String>>();
	private Log log;
	private TableService tableService;
	
	public ViewTester(Log log, TableService tableService){
		this.log = log;
		this.tableService = tableService;
	}
	
	
	public void check(String baseTableName, String viewTableName, String viewDefinition) {
		
		Configuration conf = NetworkConfig.getHBaseConfiguration(log);

		
		try {

			
			ViewMode viewMode = ViewMode.valueOf(viewDefinition.split(",")[0]);
			log.info(this.getClass(),"checking view: "+viewTableName);
			log.info(this.getClass(),"on baseTable: "+baseTableName);
			log.info(this.getClass(),"viewMode: "+viewMode);
		
			Map<String, List<String>> view = calculateView(baseTableName, viewDefinition, conf);
		
			log.info(this.getClass(),"calculated view: "+view);
			
			boolean isEqual =false;
			

//			if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX) || viewMode.equals(ViewMode.SELECTION)){
				
			isEqual = compareViews(viewTableName, viewDefinition, view, conf);

//			}
//			
//
//			if(viewMode.equals(ViewMode.INDEX)){
//		
//				isEqual = compareViews(viewTableName, viewDefinition, view, conf);
//			}
//			
			
			if(isEqual)log.info(this.getClass(),"View "+viewTableName+" has been calculated correctly");
			else log.info(this.getClass(),"View "+viewTableName+" is INCORRECT");
			
			log.info(this.getClass(),"executing server-side scan");
			serverSideScan(baseTableName, conf); 
			
			log.info(this.getClass(),"-----------------------------");
			
			
		} catch (IOException e) {
			log.error(this.getClass(), e);
		}
	}

	private boolean compareViews(String viewTableName, String viewDefinition, Map<String, List<String>> calculatedView, Configuration conf) throws IOException {
		
		boolean isViewEqual = true;
		
		ViewMode viewMode = ViewMode.valueOf(viewDefinition.split(",")[0]);
		
		HTable table;
		table = new HTable(conf, viewTableName);

		int count= 0;
		long overallTime= 0;
		
		for (String key : calculatedView.keySet()) {
			
			boolean isRecordEqual = true;
			Long valueFromView=null;
			
			if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
				
				Long calculatedValue = Long.parseLong(calculatedView.get(key).get(0));
				
//				CreateAggregationView cAV = CreateAggregationView.parse(btu.getViewDefinition());
//			
//			if(createViewTable instanceof CreateAggregationView){
				
				CreateAggregationView cAV = CreateAggregationView.parse(viewDefinition);
				
				
				long startTime = new Date().getTime();
				count++;
				
				Get get = new Get(Bytes.toBytes(key));
				get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(cAV.getAggregationValue()));
				
				Result result = table.get(get);
				
				long accessTime = new Date().getTime() - startTime;
				overallTime += accessTime;
				
				
				if(!result.isEmpty())valueFromView = Long.parseLong(Bytes.toString(result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes(cAV.getAggregationValue()))));
				else isRecordEqual = false;
				
				if(calculatedValue == null && valueFromView != null)isRecordEqual = false;
				if(calculatedValue != null && valueFromView == null)isRecordEqual = false;
				
				if(calculatedValue != null && valueFromView != null){
					if(!calculatedValue.equals(valueFromView))isRecordEqual = false;
				}

			}
			if(viewMode.equals(ViewMode.SELECTION)){
				
				CreateSelectionView cSV = CreateSelectionView.parse(viewDefinition);
				
				long startTime = new Date().getTime();
				count++;
				
				Get get = new Get(Bytes.toBytes(key));
				get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(cSV.getSelectionColumn()));
				
				Result result = table.get(get);
				
				overallTime += (new Date().getTime() - startTime);
				
				Long calculatedValue = Long.parseLong(calculatedView.get(key).get(0));
				
				if(!result.isEmpty())valueFromView = Long.parseLong(Bytes.toString(result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes(cSV.getSelectionColumn()))));
				else isRecordEqual = false;
				
//				System.out.println("valueFromView:"+valueFromView);
//				System.out.println("calculatedValue:"+calculatedValue);
				
				if(calculatedValue == null && valueFromView != null)isRecordEqual = false;
				if(calculatedValue != null && valueFromView == null)isRecordEqual = false;
				
				if(calculatedValue != null && valueFromView != null){
					if(!calculatedValue.equals(valueFromView))isRecordEqual = false;
				}

			}
			
			if(viewMode.equals(ViewMode.INDEX)){
				
					
				Get get = new Get(Bytes.toBytes(key));

				
				Result result = table.get(get);
				for(String col : calculatedView.get(key)){
					
					if(result.isEmpty()){
						isRecordEqual = false;
					}else{
						if(!result.containsColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(col)))isRecordEqual = false;
					}
				}
				


			}
			
			if(viewMode.equals(ViewMode.JOIN)){
				
				
				Get get = new Get(Bytes.toBytes(key));

				
				Result result = table.get(get);
				for(String col : calculatedView.get(key)){
					
					if(result.isEmpty()){
						isRecordEqual = false;
					}else{
						if(!result.containsColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(col)))isRecordEqual = false;
					}
					
				}
//				


			}

			if(!isRecordEqual){
				
				log.info(this.getClass(), "The following key/value was not found correctly:"+key+"/"+calculatedView.get(key));
				isViewEqual = false;
			}
		}
		table.close();
		
		
		if(count != 0){
			log.info(this.getClass(), "Average access time to view:"+(overallTime/count));
			
		}

		
		
		return isViewEqual;
	}
	
	

	
	private Map<String, List<String>> calculateView(String baseTableName, String viewDefinition, Configuration conf) throws IOException {
		
			ViewMode viewMode = ViewMode.valueOf(viewDefinition.split(",")[0]);
		
			HTable table;
			table = new HTable(conf, baseTableName);
	
			Scan scan1 = new Scan();
			ResultScanner scanner1 = table.getScanner(scan1);
		
			Map<String, List<String>> viewIndex = new HashMap<String, List<String>>();
			
			long startTime = new Date().getTime();
			
			int count= 0;
			long overallTime= 0;
			
			log.info(this.getClass(), "start scan");
			
			
			for (Result res : scanner1) {
			
	//					System.out.println(Bytes.toString(res.getRow()));
				
					if(Bytes.toString(res.getRow()).contains("finishmarker"))continue;
						
					if(viewMode.equals(ViewMode.AGGREGATION_SUM) || viewMode.equals(ViewMode.AGGREGATION_COUNT) || viewMode.equals(ViewMode.AGGREGATION_MIN) || viewMode.equals(ViewMode.AGGREGATION_MAX)){
							
//						System.out.println("getFamilyMap:"+BytesUtil.convertMapBack(res.getFamilyMap(Bytes.toBytes("colfam1"))));
						
							CreateAggregationView cAV = CreateAggregationView.parse(viewDefinition);
							KeyValue aggKey = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(cAV.getAggregationKey()));
//							System.out.println("aggKey:"+aggKey);
							
//							System.out.println("aggKey:"+Bytes.toString(aggKey.getValue()));
							
							KeyValue aggValue = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(cAV.getAggregationValue()));
//							System.out.println("aggValue:"+Bytes.toString(aggValue.getValue()));
							
						
							List<String> values = viewIndex.get(Bytes.toString(aggKey.getValue()));
							
	//						System.out.println("values: "+values);
							if(values == null){
								values = new ArrayList<String>();
								if(viewMode.equals(ViewMode.AGGREGATION_MIN))values.add(String.valueOf(Long.MAX_VALUE));
								else values.add("0");
							}
							
							Long value = Long.parseLong(values.get(0));
							if(value == null){
								value = 0l;
							}
							if(viewMode.equals(ViewMode.AGGREGATION_SUM))value += Long.parseLong(Bytes.toString(aggValue.getValue()));
							if(viewMode.equals(ViewMode.AGGREGATION_COUNT))value += 1;
							if(viewMode.equals(ViewMode.AGGREGATION_MIN)){
								
								if(Long.parseLong(Bytes.toString(aggValue.getValue())) < value){

									value = Long.parseLong(Bytes.toString(aggValue.getValue()));
									
								}
							}
							if(viewMode.equals(ViewMode.AGGREGATION_MAX))if(Long.parseLong(Bytes.toString(aggValue.getValue())) > value)value = Long.parseLong(Bytes.toString(aggValue.getValue()));
							
							values.set(0, String.valueOf(value));
	//						System.out.println("cAV.getType():"+cAV.getType());
							viewIndex.put(Bytes.toString(aggKey.getValue()), values);
		//							System.out.println(Bytes.toString(aggValue.getValue()));
						}
						
					if(viewMode.equals(ViewMode.SELECTION)){
							
							CreateSelectionView cSV = CreateSelectionView.parse(viewDefinition);
							KeyValue selectionValue = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(cSV.getSelectionColumn()));
							
							Long value = Long.parseLong(Bytes.toString(selectionValue.getValue()));
	
							long startTime2 = new Date().getTime();
							count++;
							
							Get get = new Get(res.getRow());
							get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(cSV.getSelectionColumn()));
							
							table.get(get);
							
							overallTime += (new Date().getTime() - startTime2);
							
							
							boolean isMatching=false;
							
							switch(cSV.getSelectionOperation()){
							case ">" : isMatching = (value > Long.parseLong(cSV.getSelectionValue()));
								break;
							case "<" : isMatching = (value < Long.parseLong(cSV.getSelectionValue()));
								break;
							case "=" : isMatching = (value == Long.parseLong(cSV.getSelectionValue()));
								break;
							
							}
							
							if(isMatching){
								
								List<String> values = new ArrayList<String>();
								
								values.add(value+"");
								viewIndex.put(Bytes.toString(res.getRow()), values);
							}
							
		//							System.out.println(Bytes.toString(aggValue.getValue()));
						}
						
					if(viewMode.equals(ViewMode.INDEX)){
							
							CreateIndexView cIV = CreateIndexView.parse(viewDefinition);
							KeyValue indexKey = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(cIV.getIndexKey()));
	//						System.out.println("cIV.getIndexKey(): "+cIV.getIndexKey());
	//						System.out.println("Bytes.toString(indexKey.getValue()): "+Bytes.toString(indexKey.getValue()));
							
							List<String> values = viewIndex.get(Bytes.toString(indexKey.getValue()));
							
	//						System.out.println("values: "+values);
							if(values == null){
								values = new ArrayList<String>();
	
							}
							values.add(Bytes.toString(res.getRow()));
							viewIndex.put(Bytes.toString(indexKey.getValue()), values);
					}
					
					if(viewMode.equals(ViewMode.JOIN)){
						
						
						/*
	
						BaseTableUpdate btu = new BaseTableUpdate();
						btu.setBaseTable(baseTableName);
						btu.setViewDefinition(viewDefinition);
						btu.setKey(Bytes.toString(res.getRow()));
						btu.setColumns(BytesUtil.convertMapBack(res.getFamilyMap(Bytes.toBytes("colfam1"))));
//						System.out.println("sslj: "+btu);
						
						CreateReverseJoinView cJV = CreateReverseJoinView.parse(viewDefinition);
						
						Join join = new Join(cJV, tableService, log);
						
						JoinNode rootNode = join.computeJoinRecords(btu);

						if(rootNode == null){
							System.out.println("no matchting row found for join");
							
						}else{
						
								
								List<List<MatchingRow>> resultJoin = new ArrayList<List<MatchingRow>>();
								rootNode.returnMatchingRow(resultJoin, null);
		//						
		//						
								for (List<MatchingRow> list : resultJoin) {
									
									String viewRecordKey = join.getPrimaryKey(list);
									List<String> viewRecordColumns = BytesUtil.convertListBack(BytesUtil.convertMapToList(join.getColumns(list)));
		
									List<String> values = viewIndex.get(viewRecordKey);
									
									if(values == null){
										values = new ArrayList<String>();
			
									}
									values.addAll(viewRecordColumns);
									viewIndex.put(viewRecordKey, values);
									
								}
						}
						*/
					
						
						
//						CreateIndexView cIV = CreateIndexView.parse(viewDefinition);
//						KeyValue indexKey = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(cIV.getIndexKey()));
//	//					System.out.println("cIV.getIndexKey(): "+cIV.getIndexKey());
//	//					System.out.println("Bytes.toString(indexKey.getValue()): "+Bytes.toString(indexKey.getValue()));
//						
//						List<String> values = viewIndex.get(Bytes.toString(indexKey.getValue()));
//						
//	//					System.out.println("values: "+values);
//						if(values == null){
//							values = new ArrayList<String>();
//	
//						}
//						values.add(Bytes.toString(res.getRow()));
//						viewIndex.put(Bytes.toString(indexKey.getValue()), values);
				}	
					
			}
			if(count != 0){
				log.info(this.getClass(), "Average access time to base table:"+(overallTime/count));
				
			}
			
			log.info(this.getClass(), "scan executed in :"+(new Date().getTime() - startTime)+" ms");
			
			scanner1.close();
			table.close();
			
			return viewIndex;

	}
	
	public void serverSideScan(String baseTable, Configuration conf) {
		

		HConnection connection;
		HTableInterface table=null;
		try {
			connection = HConnectionManager.createConnection(conf);
			table = connection.getTable(baseTable);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		long startTime = new Date().getTime();
		
		final SumRequest request = SumRequest.newBuilder().setFamily("colfam1").setColumn("gross").build();
		try {
		Map<byte[], List<Sum.KeyValue>> results = table.coprocessorService (SumService.class, null, null, new Batch.Call<SumService, List<Sum.KeyValue>>() {
		    @Override
		        public List<Sum.KeyValue> call(SumService aggregate) throws IOException {
		    		BlockingRpcCallback rpcCallback = new BlockingRpcCallback();
		            aggregate.getSum(null, request, rpcCallback);
		            SumResponse response = (SumResponse)rpcCallback.get();
		            return response.getSumMapList();
		        }
		    });
		    

		
		Map<String, Long> aggregationMap=new HashMap<String, Long>();
		for (byte[] key : results.keySet()) {
			log.info(this.getClass(), Bytes.toString(key));
		}
		
		log.info(this.getClass(), "server-side scan time:" + (new Date().getTime() - startTime));
		
		for (List<Sum.KeyValue> sum : results.values()) {
			for (Sum.KeyValue keyValue : sum) {
				if(aggregationMap.containsKey(keyValue.getKey())){
					Long val = aggregationMap.get(keyValue.getKey());
					aggregationMap.put(keyValue.getKey(), val+keyValue.getValue());
				}else{
					aggregationMap.put(keyValue.getKey(), keyValue.getValue());
				}
				aggregationMap.put(keyValue.getKey(), keyValue.getValue());
			}
		 }
		log.info(this.getClass(), "Map = " + aggregationMap);
		    
		    
		} catch (ServiceException e) {
		e.printStackTrace();
		} catch (Throwable e) {
		    e.printStackTrace();
		}

	}
	

}
