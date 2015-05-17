package de.webdataplatform.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.client.Client;
import de.webdataplatform.log.Log;
import de.webdataplatform.regionserver.WALReader;
import de.webdataplatform.settings.CreateAggregationView;
import de.webdataplatform.settings.CreateIndexView;
import de.webdataplatform.settings.CreateSelectionView;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.EvaluationSet;
import de.webdataplatform.settings.ICreateTable;
import de.webdataplatform.settings.ICreateView;
import de.webdataplatform.settings.JoinTablePair;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.settings.ViewTester;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.view.TableService;
import de.webdataplatform.view.ViewDefinitions;
import de.webdataplatform.view.ViewMode;
import de.webdataplatform.viewmanager.processing.Processing;

public class TestClient {

	/**
	 * @param args
	 */
	
	private static Log log; 
	
	
	
	public static void printJoinRelations(List<JoinTablePair> jtps){
		
		for (JoinTablePair joinTablePair : jtps) {
			System.out.println(joinTablePair);
		}
		
	}
	
	
	
	public static void main(String[] args) {

		log = new Log("TestClient");

		SystemConfig.load(log);
		NetworkConfig.load(log);
		DatabaseConfig.load(log);
		EvaluationConfig.load(log);
		
//		
//		BaseTableService bts = new BaseTableService(log);
//		
//		List<Result> results = bts.scanValue("bt1", "colAggKey", "x060");
//		
//		System.out.println(results);
//		
//		Result result;
//		try {
//			result = bts.get("bt2", "x60");
//			System.out.println(result);	
//		} catch (RemoteException e) {
//			e.printStackTrace();
//		}

		
		/*
		CreateAggregationView cAV = (CreateAggregationView)EvaluationConfig.getCreateViewTables().get(4);
		
		System.out.println(cAV.getName());
		
		System.out.println(cAV);
		System.out.println(CreateAggregationView.parse(cAV.getViewDefinition()));
		
		System.out.println("-----------------------------------------");
		
		CreateSelectionView cSV = (CreateSelectionView)EvaluationConfig.getCreateViewTables().get(5);
		
		System.out.println(cSV.getName());
		
		System.out.println(cSV);
		System.out.println(CreateSelectionView.parse(cSV.getViewDefinition()));
		
		System.out.println("-----------------------------------------");
		
		CreateIndexView cIV = (CreateIndexView)EvaluationConfig.getCreateViewTables().get(6);
		
		System.out.println(cIV.getName());
		
		System.out.println(cIV);
		System.out.println(CreateIndexView.parse(cIV.getViewDefinition()));
		
		System.out.println("-----------------------------------------");
		*/
//		CreateJoinView cJV = (CreateJoinView)EvaluationConfig.getCreateViewTables().get(7);
		
//		System.out.println(cJV.getName());
//		System.out.println(cJV.getJoinPairs());
		
//		String viewDefinition = JoinTablePair.getViewDefinition(cJV.getJoinPairs());
//		System.out.println(viewDefinition);
//		System.out.println(JoinTablePair.parse(viewDefinition));
//		
//		System.out.println("-----------------------------------------");	

//		String baseTable = "bt4";
		
	
//		Processing processing = new Processing(log, null, null);
//		BaseTableService bts = new BaseTableService(log);
//		
//		List<Result> result = bts.scan("bt1");
//		
//		for (Result result2 : result) {
//			
//			
//			
//			List<KeyValue> keyValues = result2.getColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey"));
//			String colAggKey=null;
//			
//			for (KeyValue keyValue : keyValues) {
//				colAggKey=Bytes.toString(keyValue.getValue());
//			}
//			System.out.println("------------------");
//			BaseTableUpdate btu = new BaseTableUpdate(log, "bt1;rs1;158142;"+Bytes.toString(result2.getRow())+";Put;colAggVal=058,colAggKey="+colAggKey+"; ;");
//			System.out.println(btu);
//			btu.setViewDefinition(viewDefinition);
//
//			
//			JoinNode rootNode = processing.computeJoinRecords(btu);
//			ViewTableService vts = new ViewTableService(log);
//			
//			if(rootNode != null){
//				List<List<MatchingRow>> resultJoin = new ArrayList<List<MatchingRow>>();
//		
////				System.out.println(rootNode.getRightJoinNodes());
//				
//				rootNode.returnMatchingRow(resultJoin, null);
//				
//				for (List<MatchingRow> list : resultJoin) {
//					
//					String primaryKey = MatchingRow.getPrimaryKey(list);
//					System.out.println("primaryKey: "+primaryKey);
//					
//					Map<byte[], byte[]> cols = MatchingRow.getColumns(list);
//					cols.put(Bytes.toBytes("rs1:158142"), Bytes.toBytes(""));
//					System.out.println("cols: "+MatchingRow.mapToString(cols));
//					
//					vts.put(Bytes.toBytes("joinTable"), Bytes.toBytes(primaryKey), cols);
//				}
//				
//			}else{
//				System.out.println("no join possible");
//			}
//			
//		}
			
		
		
		
		
//		ViewTableService vts = new ViewTableService(log);
//		
//		List<byte[]> list = new ArrayList<byte[]>();
//		list.add(Bytes.toBytes("rs1:158143"));
//		
//		System.out.println(MatchingRow.mapToString(vts.get(Bytes.toBytes("joinTable"), Bytes.toBytes("k032_x097_k048_m085"), list)));
//		
//		
//		
//		Map<byte[], byte[]> cols = new HashMap<byte[], byte[]>();
//		cols.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("15"));
//		cols.put(Bytes.toBytes("rs1:158142"), Bytes.toBytes(""));
//		System.out.println("cols: "+MatchingRow.mapToString(cols));
//		
//		vts.put(Bytes.toBytes("sum1"), Bytes.toBytes("x010"), cols);
//		
//		List<byte[]> list = new ArrayList<byte[]>();
//		list.add(Bytes.toBytes("colAggVal"));
//		list.add(Bytes.toBytes("rs1:158142"));
//		
//		Map<byte[], byte[]>  result = vts.get(Bytes.toBytes("sum1"), Bytes.toBytes("x010"), list);
//		
//		System.out.println(Bytes.toString(result.get(Bytes.toBytes("colAggVal"))));
		
		
//		List<byte[]> list = new ArrayList<byte[]>();
////		list.add(Bytes.toBytes("colAggVal"));
//		list.add(Bytes.toBytes("rs1:158145"));
//		
//		vts.delete(Bytes.toBytes("sum1"), Bytes.toBytes("x010"), list);
		
//		System.out.println(Bytes.toString(result.get(Bytes.toBytes("colAggVal"))));
		
		
		
		
		
		
			
			
			
//			String key ="";
//			vts.deleteWithRegEx(Bytes.toBytes("joinTable"), ".*"+key+".*");
			
			
//			vts.checkAndPutWithSignature2
		
//		
		
		

		
		
		/** CREATE AND FILL VIEW DEFINITIONS*/
		
		ViewDefinitions viewDefinitions = new ViewDefinitions(log, "viewdefinitions");
////		
		try {

			EvaluationSet evaluationSet = EvaluationConfig.EVALUATIONSETS.get(0);
			
//			viewDefinitions.generateTable();
//			viewDefinitions.generateViewDefinitions(evaluationSet.getExperiment().getCreateViewTables());
			viewDefinitions.loadViewDefinitions();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
			
		// CREATE AND FILL BASETABLE 
			
		String baseTable1 = "bt1";
		
		
////		String baseTable2 = "bt2";
//		TableService ts = new TableService(log);
//
//		
		/*Client client = new Client("c1", log);
		try {
//			client.recreateRangeSplitTable(baseTable1, 2);
			client.recreateTable(baseTable1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
//		try {
//			System.out.println("Wating for Regions to move");
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		try {
//			client.fillBaseTable(baseTable1, "uniform", 1000);
			
			int numOfOperations=5;
			
//			for (long i = 0; i < numOfOperations; i++) {
//				
//				log.info(TestVersioning.class, "putting entry: ");
//				Put put = new Put(Bytes.toBytes("k0001"));
//				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x"+i));
//				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes(i+""));
//				baseTable.put(put);
//				
//
//
//			}
			
			HTable baseTable = new HTable(NetworkConfig.getHBaseConfiguration(log), baseTable1);
			Put put = new Put(Bytes.toBytes("k0001"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x10"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes("10"));
			baseTable.put(put);
			
			Delete delete = new Delete(Bytes.toBytes("k0001"));
			baseTable.delete(delete);
			
			put = new Put(Bytes.toBytes("k0001"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x11"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes("11"));
			baseTable.put(put);
			
			delete = new Delete(Bytes.toBytes("k0001"));
			baseTable.delete(delete);
			
			put = new Put(Bytes.toBytes("k0001"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x12"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes("12"));
			baseTable.put(put);
			
			put = new Put(Bytes.toBytes("k0001"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x13"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes("13"));
			baseTable.put(put);
			
//			
//			Get get = new Get(Bytes.toBytes("k0001"));
////			get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"));
//			get.setMaxVersions();
//			Result result = baseTable.get(get);
//		
//			
//			System.out.println("-------------------------");
//			List<Cell> cells = result.getColumnCells(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"));
//			
//			for (Cell cell2 : cells) {
//				System.out.println(cell2.toString());
//				System.out.println(cell2.getTimestamp());
//			}
//			
//	
//			System.out.println("-------------------------");
			
			
			baseTable.close();
			
//			client.deleteTable("bt1");
//			client.deleteTable("bt2");
//			client.deleteTable("bt3");
//			client.deleteTable("bt4");
			
//			client.fillBaseTable(baseTable2, "uniform", 250);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
////		
//		generateInsertStatements(baseTable1, ts);
//		generateInsertStatements2(baseTable2, ts);
		
//		generateDeleteStatements(baseTable1, ts);
//		generateDeleteStatements2(baseTable2, ts);
//		generateUpdateStatements(baseTable1, ts);
		
		
		/** CREATE VIEWTABLES */
		
//		String baseTable = "bt1";
//		
//		
		
//		for (ICreateTable ct : EvaluationConfig.getCreateViewTables()) {
//			try {
//				client.deleteTable(ct.getName());
////				client.recreateTable("join1");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		
//		for (ICreateTable ct : EvaluationConfig.getCreateViewTables()) {
//			try {
//				client.recreateTable(ct.getName());
////				client.recreateTable("join1");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		
		System.out.println("------------------------------------------------");
		System.out.println("------------------------------------------------");
		System.out.println("------------------------------------------------");
		System.out.println("------------------------------------------------");
		/** READ OUT WAL RS1 */	
//			
		Set<String> baseTables = viewDefinitions.getBasetablesWithView();
		System.out.println(baseTables);
		
		Queue<BaseTableUpdate> btuQueue = new ConcurrentLinkedQueue<>();
//		
		WALReader walReader = new WALReader(log, "storageserver1", "rs1", viewDefinitions , btuQueue);
		
		List<String> directoryPath = walReader.searchLogDirectory("/hbase/WALs/", "storageserver1");
		
		System.out.println(directoryPath);
		
		
		/** PROCESS UPDATES */
		if(directoryPath.size() > 0){

			SystemConfig.REGIONSERVER_LOGWAL=true;
			walReader.searchNewEntries(directoryPath);
		}
		

//		Processing processing = new Processing(log, null, null);
		HTable baseTable=null;
		try {
			baseTable = new HTable(NetworkConfig.getHBaseConfiguration(log), baseTable1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		int i = 0;
		while(!btuQueue.isEmpty()){
			
			BaseTableUpdate btu = btuQueue.poll();
			List<BaseTableUpdate> baseTableViewUpdates =viewDefinitions.process(btu);		
			
			for (BaseTableUpdate baseTableUpdate : baseTableViewUpdates) {
				
				
//				loadLastRow(baseTable, btu, baseTableUpdate);

//				processing.process(baseTableUpdate);
				i++;
			}
			System.out.println("------------------------------------------------");
//			System.out.println(btu.getViewDefinition());
			
			
		}
		try {
			baseTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Updates processed: "+i);
		
		
		/** READ OUT WAL RS 2*/	
//		
////		System.out.println(btuQueue);
//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("-------------------RS 2-------------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		
//		
//		btuQueue = new ConcurrentLinkedQueue<>();
////		
//		walReader = new WALReader(log, "storageserver1", "rs2", viewDefinitions , btuQueue, null, null);
//		
//	    directoryPath = walReader.searchLogDirectory("/hbase/.logs/", "storageserver1");
//		
//		/** PROCESS UPDATES */
//		
//		walReader.searchNewEntries("/hbase/.logs/"+directoryPath);
//
//		processing = new Processing(log, null, null);
//		
//		i = 0;
//		while(!btuQueue.isEmpty()){
//			
//			BaseTableUpdate btu = btuQueue.poll();
//			List<BaseTableUpdate> baseTableViewUpdates =viewDefinitions.process(btu);		
//			
//			for (BaseTableUpdate baseTableUpdate : baseTableViewUpdates) {
//				System.out.println(baseTableUpdate);
//				processing.process(baseTableUpdate);
//				i++;
//			}
//			System.out.println("------------------------------------------------");
////			System.out.println(btu.getViewDefinition());
//			
//			
//		}
//		System.out.println("Updates processed: "+i);		
		
		/** PROCESS UPDATES 2nd time */

//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("-------------------2nd round--------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		System.out.println("------------------------------------------------");
//		
//		walReader.searchNewEntries("/hbase/.logs/"+directoryPath);
//
//		while(!btuQueue.isEmpty()){
//			
//			BaseTableUpdate btu = btuQueue.poll();
//			List<BaseTableUpdate> baseTableViewUpdates =viewDefinitions.process(btu);		
//			
//			for (BaseTableUpdate baseTableUpdate : baseTableViewUpdates) {
//				System.out.println(baseTableUpdate);
//				processing.process(baseTableUpdate);
//			}
//			System.out.println("------------------------------------------------");
////			System.out.println(btu.getViewDefinition());
//			
//			
//		}		
		
		
		
		/** CHECK TABLES */
		
//		ICreateView cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(0);	
//		ViewTester viewTester = new ViewTester(log, ts);
//		viewTester.check(baseTable1, "count1", cAV.getViewDefinition());
//		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(1);	
//		viewTester.check(baseTable1, "sum1", cAV.getViewDefinition());
//		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(3);	
//		viewTester.check(baseTable1, "min1", cAV.getViewDefinition());
//		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(4);	
//		viewTester.check(baseTable1, "max1", cAV.getViewDefinition());
//		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(5);	
//		viewTester.check(baseTable1, "selection1", cAV.getViewDefinition());
//		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(6);	
//		viewTester.check(baseTable1, "selectionsum1", cAV.getViewDefinition());		
//		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(7);	
//		viewTester.check(baseTable1, "index1", cAV.getViewDefinition());
//		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(8);	
////		viewTester.check(baseTable1, "join1", cAV.getViewDefinition());		
		
		
		
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(3);	
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(4);	
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(5);	
//		cAV = (ICreateView)EvaluationConfig.getCreateViewTables().get(6);	
//		
		
		
		
//		
//		
//		walReader.
//		
		
		
		
		
//		System.out.println(btu);
//		
//		List<JoinTablePair> joinRelations = processing.computeJoinOrder("bt1", viewDefinition);
//		printJoinRelations(joinRelations);
//		System.out.println("-----------------------------------------");	
//		
//		joinRelations = processing.computeJoinOrder("bt2", viewDefinition);
//		printJoinRelations(joinRelations);
//		System.out.println("-----------------------------------------");	
//		
//		joinRelations = processing.computeJoinOrder("bt3", viewDefinition);
//		printJoinRelations(joinRelations);
//		System.out.println("-----------------------------------------");	
//		
//		joinRelations = processing.computeJoinOrder("bt4", viewDefinition);
//		printJoinRelations(joinRelations);
//		System.out.println("-----------------------------------------");	
		
		
//		processing.propagateInsertJoin(btu);
		
		
//		System.out.println(JoinTablePair.getJoinTables(cJV.getJoinPairs(), "bt2"));
		
//		SVMSystem svmSystem = new SVMSystem();
//		
//		svmSystem.setLog(log);
//		
//		log.info(TestClient.class, "creating view tables, setting process flag....");
//		log.info(TestClient.class, "view tables "+EvaluationConfig.getCreateViewTables());
//		svmSystem.createTables(EvaluationConfig.getCreateViewTables());
//		
//		log.info(TestClient.class, "creating base tables, setting process flag....");
//		log.info(TestClient.class, "base tables "+EvaluationConfig.getCreateBaseTables());
//		svmSystem.createTables(EvaluationConfig.getCreateBaseTables());
//
//		log.info(TestClient.class, "creating view definition");
//		svmSystem.createViewDefTable();
//		
//		try {
//			Thread.sleep(10000);
//			svmSystem.fillViewDefinitions(EvaluationConfig.getCreateViewTables());
//		} catch (IOException e) {
//
//			log.error(TestClient.class, e);
//		} catch (InterruptedException e) {
//
//			e.printStackTrace();
//		}
//
//		log.info(TestClient.class, "filling base tables....");
//		log.info(TestClient.class, "base tables "+EvaluationConfig.getCreateBaseTables());
//		svmSystem.fillBaseTables((List<CreateBaseTable>)EvaluationConfig.getCreateBaseTablesAsBaseTables());
		
//		Map<String, List<ICreateTable>> views = new HashMap<String, List<ICreateTable>>();
		
		
		
//			
//			System.out.println(createViewTable.getName());
//			System.out.println(createViewTable.getBasetable());
//			System.out.println(createViewTable.getType());
//			System.out.println("----------------------");
//
//			
//			List<ICreateTable> viewsOfBT = views.get(createViewTable.getBasetable());
//			
//			if(viewsOfBT == null){
//				
//				viewsOfBT = new ArrayList<ICreateTable>();
//				
//			}
//			viewsOfBT.add(createViewTable);
//			views.put(createViewTable.getBasetable(), viewsOfBT);
//			
//			log.info(TestClient.class, "creating view definitions");
//
//		}
//		System.out.println(views);
		

//		checkTables();
		
		
		
		
		
	}
//		

/*

	private static void loadLastRow(HTable baseTable, BaseTableUpdate btu, BaseTableUpdate baseTableUpdate) {
		

		Map<String, String> map = new HashMap<String, String>();
		
		try {
			Get get = new Get(Bytes.toBytes("k0001"));
			get.setTimeRange(0, Long.parseLong(baseTableUpdate.getTimestamp())-1);
			
			
			Result result = baseTable.get(get);

			List<Cell> cells = result.getColumnCells(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"));
			
			
			
			for (Cell cell2 : cells) {
				map.put("aggKey", Bytes.toString(cell2.getValue()));
						System.out.println(Bytes.toString(cell2.getValue()));
			}
			

			cells = result.getColumnCells(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"));
			
			for (Cell cell2 : cells) {
				map.put("aggVal", Bytes.toString(cell2.getValue()));
						System.out.println(Bytes.toString(cell2.getValue()));
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(map.keySet().size() != 0){
			
			System.out.println(map);
			btu.setOldColumns(map);
			
			
		}
		System.out.println("-------------------------");
		System.out.println("btu: "+baseTableUpdate);
		System.out.println("-------------------------");
	}



	private static void generateInsertStatements(String baseTableName, TableService vts) {
		
			
		byte[] tableName = Bytes.toBytes(baseTableName);
		
		
		byte[] key = Bytes.toBytes("k015");
		Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x042"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("10"));
		vts.put(tableName, key, columns);
		
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k016");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x045"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("50"));
		vts.put(tableName, key, columns);

		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k017");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x042"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("5"));
		vts.put(tableName, key, columns);
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k018");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x042"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("15"));
		vts.put(tableName, key, columns);


		System.out.println("----------------------------------");
		key = Bytes.toBytes("k019");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x047"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("25"));
		vts.put(tableName, key, columns);
		
		
	}
	
	private static void generateInsertStatements2(String baseTableName, TableService vts) {
		
		
		byte[] tableName = Bytes.toBytes(baseTableName);
		
		
		byte[] key = Bytes.toBytes("x047");
		Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("f033"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("30"));
		vts.put(tableName, key, columns);
		
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("x045");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("f022"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("11"));
		vts.put(tableName, key, columns);

		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("x099");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("f013"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("15"));
		vts.put(tableName, key, columns);
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("x042");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("f045"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("125"));
		vts.put(tableName, key, columns);

		
		
	}
	
	private static void generateDeleteStatements(String baseTableName, TableService vts) {
		
		
		byte[] tableName = Bytes.toBytes(baseTableName);
		
		byte[] key = Bytes.toBytes("k015");
		List<byte[]> columns = new ArrayList<byte[]>();

		vts.delete(tableName, key, columns);
		
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k016");
		vts.delete(tableName, key, columns);

		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k017");
		vts.delete(tableName, key, columns);
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k018");
		vts.delete(tableName, key, columns);

	}
	
	private static void generateDeleteStatements2(String baseTableName, TableService vts) {
		
		
		byte[] tableName = Bytes.toBytes(baseTableName);
		
		byte[] key = Bytes.toBytes("x047");
		List<byte[]> columns = new ArrayList<byte[]>();

		vts.delete(tableName, key, columns);
		
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("x042");
		vts.delete(tableName, key, columns);

		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("x099");
		vts.delete(tableName, key, columns);

	}
	

	
	private static void generateUpdateStatements(String baseTableName, TableService vts) {
		
		
		byte[] tableName = Bytes.toBytes(baseTableName);
		
		byte[] key = Bytes.toBytes("k015");
		Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x042"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("10"));
		vts.put(tableName, key, columns);
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k015");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x042"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("40"));
		vts.put(tableName, key, columns);
		
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k016");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x045"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("50"));
		vts.put(tableName, key, columns);

		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k016");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x045"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("25"));
		vts.put(tableName, key, columns);
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k016");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x048"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("50"));
		vts.put(tableName, key, columns);
		
		
		System.out.println("----------------------------------");
		key = Bytes.toBytes("k018");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x042"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("15"));
		vts.put(tableName, key, columns);


		System.out.println("----------------------------------");
		key = Bytes.toBytes("k018");
		columns = new HashMap<byte[], byte[]>();
		columns.put(Bytes.toBytes("colAggKey"), Bytes.toBytes("x049"));
		columns.put(Bytes.toBytes("colAggVal"), Bytes.toBytes("25"));
		vts.put(tableName, key, columns);
	}
	
	
	
	
	*/
	
	

//	private static void checkTables() {
//		for(ICreateTable createViewTable : EvaluationConfig.getCreateViewTables()){
//			
//			
//			checkTable(createViewTable);
//			
//		}
//	}

	
		
		
	

	
	
}
