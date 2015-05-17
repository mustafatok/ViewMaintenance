package de.webdataplatform.view;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.client.Client;
import de.webdataplatform.log.Log;
import de.webdataplatform.settings.CreateAggregationView;
import de.webdataplatform.settings.CreateDeltaView;
import de.webdataplatform.settings.CreateIndexView;
import de.webdataplatform.settings.CreateReverseJoinView;
import de.webdataplatform.settings.CreateJoinView;
import de.webdataplatform.settings.CreateSelectionView;
import de.webdataplatform.settings.ICreateTable;
import de.webdataplatform.settings.JoinTablePair;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.storage.BaseTableUpdate;

public class ViewDefinitions {


	private Map<String, List<String>> viewAssignments;
	
	private Map<String, String> viewDefinitions;
	
	private TableService tableService;
	
	
	private Log log; 
	
	private String viewDefTableName;
	
	
	public ViewDefinitions(Log log, String viewDefTableName) {

		this.log = log;
		
		
		
		this.viewDefTableName = viewDefTableName;
		viewAssignments = new HashMap<String, List<String>>();
		viewDefinitions= new HashMap<String, String>();
		
		
//		List<String> views = new ArrayList<String>();
//		views.add("viewtable1");
//		views.add("viewtable2");
//
//
//		viewAssignments.put("basetable1", views);
//		
//		viewDefinitions.put("viewtable1", "AGGREGATION_COUNT,aggregationKey,aggregationValue");
//		viewDefinitions.put("viewtable2", "AGGREGATION_SUM,aggregationKey,aggregationValue");

		
		
	}
	

//	public static void main(String[] args) {
//		
//		ViewDefinitions getViews = new ViewDefinitions(null);
//		
//		getViews.loadViewDefinitions();
//	}
	
	public void generateTable() throws Exception{
		
		
		Client client = new Client("c1", log);

		client.recreateSimpleTable(viewDefTableName);
		
		client.recreateSimpleTable("finish_markers");


		
		
	}
	
	
	public void generateViewDefinitions(List<ICreateTable> createViewTables) throws Exception{
		
		log.info(this.getClass(),"-----------------------");
		log.info(this.getClass(),"fill view definition table");
		log.info(this.getClass(),"-----------------------");
		

//		Put put2 = new Put(Bytes.toBytes("basetable_joinleft"));
//		Put put3 = new Put(Bytes.toBytes("basetable_joinright"));

		Configuration conf = NetworkConfig.getHBaseConfiguration(log);
		
		HTable table = new HTable(conf, viewDefTableName);
		
		
		log.info(this.getClass(), "creating view definitions");

		for (ICreateTable createViewTable : createViewTables) {


			if(createViewTable instanceof CreateAggregationView){
				
				CreateAggregationView cAV = (CreateAggregationView)createViewTable;
				
				Put put = new Put(Bytes.toBytes(cAV.getBasetable()));
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(cAV.getName()), Bytes.toBytes(cAV.getViewDefinition()));

				table.put(put);
			}
			
			if(createViewTable instanceof CreateSelectionView){
				
				CreateSelectionView cSV = (CreateSelectionView)createViewTable;
				
				Put put = new Put(Bytes.toBytes(cSV.getBasetable()));
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(cSV.getName()), Bytes.toBytes(cSV.getViewDefinition()));
			
				table.put(put);
			}
			
			if(createViewTable instanceof CreateDeltaView){
				
				CreateDeltaView cDV = (CreateDeltaView)createViewTable;
				
				Put put = new Put(Bytes.toBytes(cDV.getBasetable()));
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(cDV.getName()), Bytes.toBytes(cDV.getViewDefinition()));
			
				table.put(put);
			}			
			
			if(createViewTable instanceof CreateIndexView){
				
				CreateIndexView cIV = (CreateIndexView)createViewTable;
				
				Put put = new Put(Bytes.toBytes(cIV.getBasetable()));
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(cIV.getName()), Bytes.toBytes(cIV.getViewDefinition()));
			
				table.put(put);
			}
			
			if(createViewTable instanceof CreateReverseJoinView){
				
				CreateReverseJoinView cJPV = (CreateReverseJoinView)createViewTable;
				
				List<String> joinTableNames = JoinTablePair.getJoinTableNames(cJPV.getJoinTables());
				
				for(String joinTableName : joinTableNames){
					
					Put put = new Put(Bytes.toBytes(joinTableName));
					
					put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(cJPV.getName()), Bytes.toBytes(cJPV.getViewDefinition()));
				
					table.put(put);
				}
				
				
			}
			
			if(createViewTable instanceof CreateJoinView){
				
				CreateJoinView cJV = (CreateJoinView)createViewTable;				
					
				Put put = new Put(Bytes.toBytes(cJV.getJoinPairView()));
					
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(cJV.getName()), Bytes.toBytes(cJV.getViewDefinition()));
				
				table.put(put);
				
				
			}
			
//			Put put = new Put(Bytes.toBytes("bt1"));
//			
//			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("zfinish_marker"), Bytes.toBytes("zfinish_marker"));
//			
//			table.put(put);
			
				
			Put put = new Put(Bytes.toBytes("delta1"));
				
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("zfinish_marker"), Bytes.toBytes("zfinish_marker"));
			
			table.put(put);
			
			put = new Put(Bytes.toBytes("delta2"));
			
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("zfinish_marker"),Bytes.toBytes("zfinish_marker"));
			
			table.put(put);
			
			put = new Put(Bytes.toBytes("delta3"));
			
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("zfinish_marker"),Bytes.toBytes("zfinish_marker"));
			
			table.put(put);
			
			put = new Put(Bytes.toBytes("delta4"));
			
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("zfinish_marker"),Bytes.toBytes("zfinish_marker"));
			
			table.put(put);
				
		}

		table.close();

	}	
	
	
	public void loadViewDefinitions(){
		
		
//		System.out.println("View Manager wird beendet5:"+zookeeperAddress);
		log.info(this.getClass(),"connecting to zookeeper on address: "+NetworkConfig.ZOOKEEPER_QUORUM+":"+NetworkConfig.ZOOKEEPER_CLIENTPORT);
		if(tableService == null)tableService = new TableService(log);
		
		log.info(this.getClass()," scanning table ");
			List<Result> results = tableService.scan("viewdefinitions");
			
			List<String> tempViews = new ArrayList<String>();
	
			
			for(Result result : results){
				
				String key =Bytes.toString(result.getRow());
				
				for(KeyValue keyValue : result.list()){
					
					tempViews.add(Bytes.toString(keyValue.getQualifier()));
	
					viewDefinitions.put(Bytes.toString(keyValue.getQualifier()), Bytes.toString(keyValue.getValue()));
					
				}
				viewAssignments.put(key, tempViews);
				tempViews = new ArrayList<String>();
			}
			
			log.info(this.getClass()," view definitions "+viewDefinitions);
			log.info(this.getClass()," view assignments "+viewAssignments);
			
			
			tableService.close();
		
	}

	public List<BaseTableUpdate> process(BaseTableUpdate baseTableUpdate){
		
		
		List<BaseTableUpdate> result = new ArrayList<BaseTableUpdate>();
		
		
		if(viewAssignments.get(baseTableUpdate.getBaseTable()) != null){
		
			for(String view : viewAssignments.get(baseTableUpdate.getBaseTable())){
				
				BaseTableUpdate btu = baseTableUpdate.copy();
				btu.setViewTable(view);
				btu.setViewDefinition(viewDefinitions.get(view));
				result.add(btu);
			}
		}else{ 
			
			
			log.info(this.getClass()," no view found for basetable '"+baseTableUpdate.getBaseTable()+"'");
			
		}
	

		return result;
		
		
	}

	
	public int numOfAssignedViewTables(String baseTable){
		
		
		return viewAssignments.get(baseTable).size();
		
		
	}
	
	
	public Set<String> getBasetablesWithView(){
		
		return viewAssignments.keySet();
		
		
	}


	public Map<String, String> getViewDefinitions() {
		return viewDefinitions;
	}


	public void setViewDefinitions(Map<String, String> viewDefinitions) {
		this.viewDefinitions = viewDefinitions;
	}
	

	
	

}
