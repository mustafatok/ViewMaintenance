package de.webdataplatform.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import de.webdataplatform.log.Log;
import de.webdataplatform.log.StatisticLog;
import de.webdataplatform.settings.ColumnDefinition;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.settings.TableDefinition;
import de.webdataplatform.test.ZipfGenerator;

public class Client {

	/**
	 * @param args
	 */
	
	
	private Configuration conf;
	
	private Log log;
	
	private String name;
	
	
	public Client(String name, Log log){

		NetworkConfig.load(log);
		EvaluationConfig.load(log);
		
		conf = NetworkConfig.getHBaseConfiguration(log);
		
		this.log = log;
		this.name = name;
//		StatisticLog.name = "clientoperations";
		

	}
	
	public Client(Log log){

		this.log = log;

	}
	
	
	public void deleteTable(String tableName) throws IOException {
		
		log.info(Client.class,"-----------------------");
		log.info(Client.class,"delete table "+tableName);
		log.info(Client.class,"-----------------------");
		
		
		HBaseAdmin admin = new HBaseAdmin(conf);
	
		try {
		
			admin.disableTable(Bytes.toBytes(tableName));
			boolean isDisabled = admin.isTableDisabled(Bytes.toBytes(tableName));
			log.info(Client.class,"Table is disabled: " + isDisabled);
			
			boolean avail1 = admin.isTableAvailable(Bytes.toBytes(tableName));
			log.info(Client.class,"Table available: " + avail1);
		
			
			admin.deleteTable(Bytes.toBytes(tableName));
		
		} catch (Exception e) {
			
			log.error(Client.class, e);
		
		}
		
		admin.close();

		
		

		

	}
	
	
	
	public List<String> getColumnFamilies(TableDefinition tableDefinition){
	
		List<String> result = new ArrayList<String>();
		
		for (ColumnDefinition coumnDefinition : tableDefinition.getColumns()) {
		
			
			if(!result.contains(coumnDefinition.getFamily()))result.add(coumnDefinition.getFamily());
		}
		
		return result;
	}
	
	public void recreateSimpleTable(String tableName) throws IOException{
		
		try {
			deleteTable(tableName);
		} catch (IOException e) {
			log.error(this.getClass(), e);
		}
		
		createSimpleTable(tableName);
		
		
	}
	
	public void createSimpleTable(String tableName) throws IOException {

		
		log.info(Client.class,"-----------------------");
		log.info(Client.class,"create table "+tableName);
		log.info(Client.class,"-----------------------");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		try {
			
			
			TableDefinition tableDefinition = DatabaseConfig.getTableDefinition(tableName);
			
			
			HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
			

			HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
			desc.addFamily(coldef);
			

			
			admin.createTable(desc);
			
			boolean avail = admin.isTableAvailable(Bytes.toBytes(tableName));
			
			
			log.info(Client.class,"Table available: " + avail);
			log.info(Client.class,"Table enabled: " + admin.isTableEnabled(Bytes.toBytes(tableName)));
			
		} catch (Exception e) {
			
			log.error(Client.class, e);
		
		}
		
		admin.close();
		

	}
	
	public void createTable(String tableName) throws IOException {

		
		log.info(Client.class,"-----------------------");
		log.info(Client.class,"create table "+tableName);
		log.info(Client.class,"-----------------------");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		try {
			
			
			TableDefinition tableDefinition = DatabaseConfig.getTableDefinition(tableName);
			
			
			HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
			
			
			for (String colFam : getColumnFamilies(tableDefinition)) {
				
				HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(colFam));
				desc.addFamily(coldef);
			}
			HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("sigfam1"));
			desc.addFamily(coldef);
			
			
//			coldef.setMaxVersions(Integer.MAX_VALUE);
//			coldef.setKeepDeletedCells(true);
			
			
			
//			desc.setDeferredLogFlush(false);
			
	//		desc.set
			
			admin.createTable(desc);
			
			boolean avail = admin.isTableAvailable(Bytes.toBytes(tableName));
			
	//		admin.enableTable(Bytes.toBytes(tableName));
			
			log.info(Client.class,"Table available: " + avail);
			log.info(Client.class,"Table enabled: " + admin.isTableEnabled(Bytes.toBytes(tableName)));
			
		} catch (Exception e) {
			
			log.error(Client.class, e);
		
		}
		
		admin.close();
		

	}
	
	public void recreateTable(String tableName) throws IOException{
		
		try {
			deleteTable(tableName);
		} catch (IOException e) {
			log.error(this.getClass(), e);
		}
		
		createTable(tableName);
		
		
	}
	
	public void recreateRangeSplitTable(String tableName, int regCount) throws IOException{
		
		try {
			deleteTable(tableName);
		} catch (IOException e) {
			log.error(this.getClass(), e);
		}
		
		try {
			createRangeSplitTable(tableName, regCount);
		} catch (Exception e) {
			log.error(this.getClass(), e);
		}
		
		
	}
	

	
	public void createRangeSplitTable(String tableName,  int regCount)throws Exception{
	
	
		
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
	
			log.error(Client.class, e);
		} catch (ZooKeeperConnectionException e) {
	
			log.error(Client.class, e);
		}
		
		HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
		
		
		TableDefinition tableDefinition = DatabaseConfig.getTableDefinition(tableName);
		
		for (String colFam : getColumnFamilies(tableDefinition)) {
			
			HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(colFam));
			desc.addFamily(coldef);
		}
		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("sigfam1"));
		desc.addFamily(coldef);
		
//		coldef.setMaxVersions(Integer.MAX_VALUE);
//		coldef.setKeepDeletedCells(true);
		
		
		byte[][] regions = createRegionArray(tableName, regCount);
//		System.out.println(Arrays.toString(regions));
		
//		byte[][] regions = new byte[][] {
//		Bytes.toBytes("k0"),
//		Bytes.toBytes("k50")
//		};

		try {
			admin.createTable(desc, regions);
		} catch (IOException e) {
		
			log.error(Client.class, e);
		}
		printTableRegions(conf, tableName);
	
	
	
}




	public byte[][] createRegionArray(String tableName, int regCount)throws Exception {
		TableDefinition tableDefinition = DatabaseConfig.getTableDefinition(tableName);
		
		if(tableDefinition == null)throw new Exception("table definition not found for table: "+tableName+", cannot split keyrange");
		
		long numOfPrimaryKeys = tableDefinition.getPrimaryKey().getNumOfPrimaryKeys();
		
		long recordsPerRegion = numOfPrimaryKeys/regCount;
//		log.info(Client.class,"recordsPerRegion: "+recordsPerRegion);
		
		int digits = String.valueOf(numOfPrimaryKeys).length();
		byte[][] regions = new byte[regCount][];
		
		for (int i = 0; i < regCount; i++) {
			
		
			String rowKey; 
					
			rowKey = tableDefinition.getPrimaryKey().getPrefix();

			for(int x = 0; x < (digits - String.valueOf((i*recordsPerRegion)).length());x++)rowKey+="0";
			rowKey += (i*recordsPerRegion);
				
//			log.info(Client.class,"rowkey: "+rowKey);
			regions[i] = Bytes.toBytes(rowKey);
			
		}
		return regions;
	}
	
	public void queueFinishMarkers(String tableName, int regCount)throws Exception{
	
	
		HTable baseTable = new HTable(conf, tableName);
		
		byte[][] regions = createRegionArray(tableName, regCount);
		
		
		for (int i = 0; i < regions.length; i++) {
			
			
			String key = Bytes.toString(regions[i])+"_finishmarker";
			log.info(Client.class, "queueing finish marker: "+key);
			
			Put put = new Put(Bytes.toBytes(key));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("finishMarker"), Bytes.toBytes(name));	
			baseTable.put(put);
			
		}

		baseTable.close();
	
}
	
	
	private void printTableRegions(Configuration conf, String tableName){
	
	
		log.info(Client.class,"Printing regions of table: " + tableName);
	
	HTable table=null;
	try {
		table = new HTable(conf, Bytes.toBytes(tableName));
	} catch (IOException e) {
		
		log.error(Client.class, e);
	}
	
	Pair<byte[][], byte[][]> pair=null;
	try {
		pair = table.getStartEndKeys();
	} catch (IOException e) {

		log.error(Client.class, e);
	}
	
	for (int n = 0; n < pair.getFirst().length; n++) {
		
	byte[] sk = pair.getFirst()[n];
	
	byte[] ek = pair.getSecond()[n];
	
	log.info(Client.class,"[" + (n + 1) + "]" +
	" start key: " +
	(sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
	", end key: " +
	(ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
	}
	
	try {
		table.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		log.error(Client.class, e);
	}
}	
	
	


	
	public void fillBaseTable(String baseTableName, String distribution, int numOfOperations)throws Exception{
		
		log.info(Client.class,"-----------------------");
		log.info(Client.class,"fill table:"+baseTableName);
		log.info(Client.class,"-----------------------");
		
		log.info(Client.class, "distribution: "+distribution);
		log.info(Client.class, "numOfOperations: "+numOfOperations);
		
		
		setFlag(false);
		
		HTable baseTable = new HTable(conf, baseTableName);


		TableDefinition tableDefinition = DatabaseConfig.getTableDefinition(baseTableName);
		
		if(tableDefinition == null)throw new Exception("table definition not found for: "+baseTableName);
		
		long numOfKeys = tableDefinition.getPrimaryKey().getNumOfPrimaryKeys();
		
		
		Random random = new Random();
		ZipfGenerator zipf = new ZipfGenerator(new Long(numOfKeys).intValue(),1);
//		ZipfDistribution zd = new ZipfDistribution(new Long(numOfKeys).intValue(), 1); 
		
		int sysoutCount=0;
		long startingTime = new Date().getTime();

		
		for (long i = 0; i < numOfOperations; i++) {
			
			sysoutCount++;	

			long k = tableDefinition.getPrimaryKey().getStartRange();
			
			if(distribution.equals("uniform"))k+=random.nextInt(new Long(numOfKeys).intValue());
			if(distribution.equals("zipf"))k+=zipf.next();
			
			int digits = String.valueOf(tableDefinition.getPrimaryKey().getEndRange()).length();
			
			String rowKey="";
			
			if(tableDefinition.getPrimaryKey().getPrefix() != null)rowKey = tableDefinition.getPrimaryKey().getPrefix();
			
			for(int x = 0; x < (digits - String.valueOf(k).length());x++)rowKey+="0";
			rowKey += k;
			
			Get get = new Get(Bytes.toBytes(rowKey));
			boolean exists;
			try {
				exists = baseTable.exists(get);
	
				
				
				if(sysoutCount == SystemConfig.CLIENT_LOGINTERVAL){
					log.info(this.getClass(), "iteration: "+i+", key: "+rowKey+", exists:"+exists);
					log.info(this.getClass(), "took: "+(new Date().getTime()-startingTime)+" ms");
					startingTime = new Date().getTime();
					sysoutCount = 0;
				}
				
				
//				log.info(this.getClass(), "putting: "+i+", key: "+rowKey+", exists:"+exists);
				try {
					Thread.sleep(2);
				} catch (InterruptedException e) {
			
					e.printStackTrace();
				}
				
				if(!exists){
					
					Put put = generatePut(tableDefinition, rowKey);
//					log.info(Client.class, "generating Insert: "+rowKey);
					baseTable.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put);
					
					
				}else{
					
					int zahl = (int)(Math.random() * 2);
					switch(zahl){ 
						case 0: 
							Put update = generatePut(tableDefinition, rowKey);baseTable.put(update);
//							log.info(Client.class, "generating Insert: "+rowKey);	
						break;
						case 1: 
							Delete delete = generateDelete(rowKey);baseTable.delete(delete);
//							log.info(Client.class, "generating delete: "+rowKey);
						break;
					}
					
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			}
			


		}

		
		baseTable.close();
		
		HTable markerTable = new HTable(conf, "finish_markers");
		log.info(Client.class, "queueing finish marker: "+name);
		Put put = new Put(Bytes.toBytes(name));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("client"), Bytes.toBytes(name));	
		markerTable.put(put);
		markerTable.close();
		
//		setFlag(true);
	

	}




	private void setFlag(Boolean flag) {
		PrintWriter writer=null;
		
		try {
			writer = new PrintWriter("basetable-filled", "UTF-8");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		writer.println(flag.toString());
		writer.flush();
		writer.close();
	}
	


	private Put generatePut(TableDefinition tableDefinition, String rowKey) {

		Put put = new Put(Bytes.toBytes(rowKey));
		
		for (ColumnDefinition columnDefinition : tableDefinition.getColumns()) {
			
			
			int digits = String.valueOf(columnDefinition.getEndRange()).length();
			
			int zahl = (int)(columnDefinition.getStartRange()+(Math.random() * columnDefinition.getNumOfValues() + 1));
			
			String value="";
			if(columnDefinition.getPrefix() != null)value = columnDefinition.getPrefix();
			
			for(int x = 0; x < (digits - String.valueOf(zahl).length());x++)value+="0";
			value+=zahl;

			
			put.add(Bytes.toBytes(columnDefinition.getFamily()), Bytes.toBytes(columnDefinition.getName()), Bytes.toBytes(value));	
			
		}
		

		return put;
	}
	
	


	private Delete generateDelete(String rowKey) {
		
		
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));

		

		return delete;
	}	

	
	
	
	
///////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	/*
	private void fillJoinTables(String joinTableLeft, String joinTableRight, Configuration conf) throws IOException {
		
		log.info(Client.class,"-----------------------");
		log.info(Client.class,"fill join tables: joinTableLeft:"+joinTableLeft+", joinTableRight"+joinTableRight);
		log.info(Client.class,"-----------------------");
		
		HTable baseTableLeft = new HTable(conf, joinTableLeft);
		
		HTable baseTableRight = new HTable(conf, joinTableRight);
		
		long operationsPerClient = numOfOperations/numOfClients;
		
		log.info(this.getClass(), "operationsPerClient: "+operationsPerClient);

		
		Random random = new Random();
		ZipfDistribution zd = new ZipfDistribution(new Long(numOfPrimaryKeys).intValue(), 1); 
		ZipfDistribution zdx = new ZipfDistribution(new Long(numOfAggKeys).intValue(), 1); 
		
		int sysoutCount=0;
		for (long i = 0; i < operationsPerClient; i++) {
			sysoutCount++;	

			long k = 0;
			if(distribution.equals(UNIFORM))k=random.nextInt(new Long(numOfPrimaryKeys).intValue());
			if(distribution.equals(ZIPF))k=zd.sample();
			
			int digits = String.valueOf(numOfPrimaryKeys).length();
			String rowKey = "k";
			for(int j = 0; j < (digits - String.valueOf(k).length());j++)rowKey+="0";
			rowKey += k;
			
			
			long x = 0;
			if(distribution.equals(UNIFORM))k=random.nextInt(new Long(numOfAggKeys).intValue());
			if(distribution.equals(ZIPF))k=zdx.sample();
			
			int digitsAggKeys = String.valueOf(numOfAggKeys).length();
			String aggregationKey = "x";
			for(int j = 0; j < (digitsAggKeys - String.valueOf(x).length());j++)aggregationKey+="0";
			aggregationKey+=x;
			
			
			Get get = new Get(Bytes.toBytes(rowKey));
			boolean exists = baseTableLeft.exists(get);
			if(sysoutCount == 10000){
				log.info(this.getClass(), "iteration: "+i+", key: "+rowKey+", exists:"+exists);
				sysoutCount = 0;
			}
			
			boolean insertUpdate=true;
			if(!exists){
				
				
				Put put = generateInsertJoinLeft(rowKey, aggregationKey);
				baseTableLeft.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put);
				
				
			}else{
				
				int zahl = (int)(Math.random() * 2);
				switch(zahl){ 
					case 0: Put update = generateUpdateJoinLeft(rowKey, aggregationKey);baseTableLeft.put(update);
						break;
					case 1: Delete delete = generateDeleteJoinLeft(rowKey);baseTableLeft.delete(delete);insertUpdate=false;
						break;
				}
				
				
			}
			
			if(insertUpdate){
				
				get = new Get(Bytes.toBytes(aggregationKey));
				exists = baseTableRight.exists(get);
				if(sysoutCount == 10000){
					log.info(this.getClass(), "iteration: "+i+", key: "+rowKey+", exists:"+exists);
					sysoutCount = 0;
				}
				
				if(!exists){
					
					
					Put put = generateInsertJoinRight(rowKey);
					baseTableRight.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), null, put);
					
					
				}else{
					
					Put put = generateUpdateJoinRight(rowKey);
					baseTableRight.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), null, put);
					
				}
				i++;
			}


		}

		
		baseTableLeft.close();
		baseTableRight.close();
	

	}
	
	

	
	
	
	private Put generateInsertJoinLeft(String rowKey, String aggregationKey) {
		
		int aggregationValue = (int)(Math.random() * numOfAggValues + 1);
		
		log.info(this.getClass(), "putting key left: "+rowKey);
		
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes(aggregationKey));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(aggregationValue+""));
		return put;
	}
	
	private Put generateUpdateJoinLeft(String rowKey, String aggregationKey) {
		
		int aggregationValue = (int)(Math.random() * numOfAggValues + 1);
		
		log.info(this.getClass(), "updating key left: "+rowKey);
		
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes(aggregationKey));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(aggregationValue+""));
		return put;
	}


	private Delete generateDeleteJoinLeft(String rowKey) {
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		log.info(this.getClass(), "deleting key left: "+rowKey);

		return delete;
	}	

	private Put generateInsertJoinRight(String aggregationKey){
		
		Put put=null;

		
		int aggregationValue = (int)(Math.random() * numOfAggValues + 1);
		
		log.info(this.getClass(), "putting key right: "+aggregationKey);
		
		put = new Put(Bytes.toBytes(aggregationKey));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes(aggregationValue+""));
		
		
		
		return put;
	}
	
	private Put generateUpdateJoinRight(String rowKey) {
		
		
		log.info(this.getClass(), "updating key right: "+rowKey);
		int aggregationValue = (int)(Math.random() * numOfAggValues + 1);
		
		
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes(aggregationValue+""));
		return put;
	}

	
	
	
//	private void fillBaseTableViewDefinitions(String tableName, Configuration conf) throws IOException {
//		
//		log.info(Client.class,"-----------------------");
//		log.info(Client.class,"fill view definition table");
//		log.info(Client.class,"-----------------------");
//		
//		HTable table = new HTable(conf, tableName);
//
//		Put put = new Put(Bytes.toBytes("basetable"));
//		Put put2 = new Put(Bytes.toBytes("basetable_joinleft"));
//		Put put3 = new Put(Bytes.toBytes("basetable_joinright"));
//
//		
//		
//		log.info(Client.class, "creating view tables");
//
//		for (int i = 0; i < viewTableCount; i++) {
//			String viewTableType = viewTableTypes.get(i%viewTableTypes.size());
//
//			if(viewTableType.equals("count")){
//				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_COUNT.toString()+","+"aggregationKey"+","+"aggregationValue"));
//			}
//			if(viewTableType.equals("sum")){
//				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_SUM.toString()+","+"aggregationKey"+","+"aggregationValue"));
//			}
//			if(viewTableType.equals("min")){
//				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_MIN.toString()+","+"aggregationKey"+","+"aggregationValue"));
//			}
//			if(viewTableType.equals("max")){
//				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_MAX.toString()+","+"aggregationKey"+","+"aggregationValue"));
//			}
//			if(viewTableType.equals("selection")){
//				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes(ViewMode.SELECTION.toString()+","+"aggregationValue,<,5"));
//			}
//			if(viewTableType.equals("join")){
//				put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes(ViewMode.JOIN.toString()+",basetable_joinleft,basetable_joinright,aggregationKey"));
//				put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes(ViewMode.JOIN.toString()+",basetable_joinleft,basetable_joinright,aggregationKey"));
//			}
//			
//		}
//		
//		if(viewTableTypes.contains("count") || viewTableTypes.contains("sum") || viewTableTypes.contains("min") || viewTableTypes.contains("max")|| viewTableTypes.contains("selection")){
//			
//			table.put(put);
//		}
//		if(viewTableTypes.contains("join")){
//			
//			table.put(put2);
//			table.put(put3);
//		}
//		table.close();
//
//	}	
	*/
	
	private Map<String, Integer> aggregationCountMap;
	
	private Map<String, Integer> aggregationSumMap;	
	
	private Map<String, Integer> aggregationMinMap;
	
	private Map<String, Integer> aggregationMaxMap;
	

	
	
	
	private void scanBaseTable(String tableName, Configuration conf) throws IOException {
		
		
		aggregationCountMap = new HashMap<String, Integer>();
		
		aggregationSumMap = new HashMap<String, Integer>();
		
		aggregationMinMap = new HashMap<String, Integer>();
		
		aggregationMaxMap = new HashMap<String, Integer>();
		
		long start = System.currentTimeMillis();
		
		HTable table = new HTable(conf, tableName);	
	
		System.out.println("Duration client setup in ms: " + (System.currentTimeMillis() - start));
		
		Scan scan1 = new Scan();
		
		start = System.currentTimeMillis();
		ResultScanner scanner1 = table.getScanner(scan1);
		
		
		
//		for (Result res : scanner1) {
//			System.out.print("Key: "+Bytes.toString(res.getRow())+",  ");
//			System.out.print("AggKey: "+Bytes.toString(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey")))+",  ");
//			System.out.print("AggValue: "+Bytes.toInt(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"))));
//			System.out.println();
////			System.out.println(res);
//		}
//		scanner1.close();
//		
//		
		
		
		
//		InternalScanner scanner = environment.getRegion().getScanner(scan);
	
		
		int result = 0;
		try {
			
			List<KeyValue> curVals;
			Result done;
			do {
				
				curVals = new ArrayList<KeyValue>();
//				curVals.clear();
				done = scanner1.next();
				
				if(done != null){
				
					curVals = done.list();
//					System.out.println(curVals);
					
					Integer updateSumValue = 0;
					Integer updateCountValue = 0;
					Integer updateMinValue = Integer.MAX_VALUE;
					Integer updateMaxValue = Integer.MIN_VALUE;
					String aggregationKey="";
					
					boolean printKey=false;
					
					for(KeyValue keyValue : curVals){
						

						String keyString = Bytes.toString(keyValue.getKey());
						
						if(!printKey && !keyString.contains("rs")){
							System.out.println();
							System.out.print(Bytes.toString(keyValue.getRow())+"; ");
							printKey=true;
						}
						
						if(!keyString.contains("rs"))System.out.print(Bytes.toString(keyValue.getValue())+"; ");
						
						if(keyString.contains("aggregationKey")){
							
							
							aggregationKey = Bytes.toString(keyValue.getValue());
							
//							System.out.println("Key: "+aggregationKey);
							
							if(aggregationSumMap.containsKey(aggregationKey)){
								
								updateSumValue = aggregationSumMap.get(aggregationKey);
							}
							if(aggregationCountMap.containsKey(aggregationKey)){
								
								updateCountValue = aggregationCountMap.get(aggregationKey);
							}	
							if(aggregationMinMap.containsKey(aggregationKey)){
								
								updateMinValue = aggregationMinMap.get(aggregationKey);
							}	
							if(aggregationMaxMap.containsKey(aggregationKey)){
								
								updateMaxValue = aggregationMaxMap.get(aggregationKey);
							}
							
						}
						if(keyString.contains("aggregationValue") && !aggregationKey.equals("")){
	
//							System.out.println("Value:"+keyValue.getValue());
							
							Integer newValue=0;
							try{
								newValue = Integer.parseInt(Bytes.toString(keyValue.getValue()));
//								System.out.println("Value: "+Bytes.toString(keyValue.getValue()));
								
							}catch(Exception e){
								log.error(Client.class, e);
							}
								
								updateSumValue += newValue;
								
								updateCountValue += 1;
	
								
								aggregationCountMap.put(aggregationKey, updateCountValue);
								
								aggregationSumMap.put(aggregationKey, updateSumValue);
								
								if(newValue < updateMinValue)aggregationMinMap.put(aggregationKey, newValue);
								
								if(newValue > updateMaxValue)aggregationMaxMap.put(aggregationKey, newValue);
							
						}
						
					}
					
				
				
				}
				
//				result += countKeyValues ? curVals.size() : 1;
			} while (done != null);
			
			log.info(Client.class,"Duration scan in ms: " + (System.currentTimeMillis() - start));
		} finally {
			scanner1.close();
		}
		

		log.info(Client.class,"--------------Scan of basetable--------------");
		
		
		StatisticLog.direct("viewtablecount");
		
		
//		for (String key : aggregationCountMap.keySet()) {
//			
//			List<String> writeToLog = new ArrayList<String>();
//			writeToLog.add(key);
//			writeToLog.add(aggregationCountMap.get(key)+"");
//			writeToLog.add(aggregationSumMap.get(key)+"");
//			writeToLog.add(aggregationMinMap.get(key)+"");
//			writeToLog.add(aggregationMaxMap.get(key)+"");
//			
//			StatisticLog.info(writeToLog);
//			
//			
//		}
		log.info(Client.class,"--------------Result count view--------------");
		log.info(Client.class,"countresult: "+aggregationCountMap+"");
		log.info(Client.class,"  ");
		log.info(Client.class,"  ");
		
		log.info(Client.class,"--------------Result sum view--------------");
		log.info(Client.class,"sumresult: "+aggregationSumMap+"");
		log.info(Client.class,"  ");
		log.info(Client.class,"  ");
		
		log.info(Client.class,"--------------Result min view--------------");
		log.info(Client.class,"minresult: "+aggregationMinMap+"");
		log.info(Client.class,"  ");
		log.info(Client.class,"  ");
		
		log.info(Client.class,"--------------Result max view--------------");
		log.info(Client.class,"maxresult: "+aggregationMaxMap+"");
		log.info(Client.class,"  ");
		log.info(Client.class,"  ");
		
	}
	
	
	/*
private void scanBaseTableJoin(String tableLeft, String tableRight, Configuration conf) throws IOException {
		
		
		Map<String, List<String>> joinMap = new HashMap<String, List<String>>();
		

		
		long start = System.currentTimeMillis();
		
		HTable table = new HTable(conf, tableLeft);	
//		BaseTableService baseTableService = new BaseTableService(log);
		
		System.out.println("Duration client setup in ms: " + (System.currentTimeMillis() - start));
		
		Scan scan1 = new Scan();
		
		start = System.currentTimeMillis();
		ResultScanner scanner1 = table.getScanner(scan1);
	
		
		int result = 0;
		try {
			
			List<KeyValue> curVals;
			Result done;
			do {
				
				curVals = new ArrayList<KeyValue>();
				done = scanner1.next();
				
				if(done != null){
				
					curVals = done.list();
//					System.out.println(curVals);
					

					String aggregationKey="";
					
					String joinKey = null;
					List<String> joinValues = new ArrayList<String>();
					boolean printKey=false;
					
					
					for(KeyValue keyValue : curVals){
						

						String keyString = Bytes.toString(keyValue.getKey());
						
						if(!printKey && !keyString.contains("rs")){
							System.out.println();
							System.out.print(Bytes.toString(keyValue.getRow())+"; ");
							joinKey = Bytes.toString(keyValue.getRow());
							printKey=true;
							
							
						}
						
						if(!keyString.contains("rs"))System.out.print(Bytes.toString(keyValue.getValue())+"; ");
						
						if(keyString.contains("aggregationKey")){
							
							
							aggregationKey = Bytes.toString(keyValue.getValue());
							joinValues.add(aggregationKey);
							
							Result temp = baseTableService.get(tableRight, aggregationKey);

							if(temp != null){
								for (KeyValue keyValueTemp : temp.list()){
									
//									if(!Bytes.toString(keyValueTemp.getQualifier()).equals("aggregationKey")){
										joinValues.add(Bytes.toString(keyValueTemp.getValue()));
										System.out.print(Bytes.toString(keyValueTemp.getValue())+"; ");
//									}
								}
							}
						
						}
						if(keyString.contains("aggregationValue") && !aggregationKey.equals("")){
	
							
							Integer newValue=0;
							try{
								newValue = Integer.parseInt(Bytes.toString(keyValue.getValue()));
								joinValues.add(newValue+"");
//								System.out.println("Value: "+Bytes.toString(keyValue.getValue()));
								
							}catch(Exception e){
								log.error(Client.class, e);
							}
								
							
						}
						
					}
					joinMap.put(joinKey, joinValues);
					
				
				
				}
				
//				result += countKeyValues ? curVals.size() : 1;
			} while (done != null);
			
			log.info(Client.class,"Duration scan in ms: " + (System.currentTimeMillis() - start));
		} finally {
			scanner1.close();
		}
		

		log.info(Client.class,"--------------Scan of basetable--------------");
		
		
		StatisticLog.direct("viewtablecount");
		
		
//		for (String key : aggregationCountMap.keySet()) {
//			
//			List<String> writeToLog = new ArrayList<String>();
//			writeToLog.add(key);
//			writeToLog.add(aggregationCountMap.get(key)+"");
//			writeToLog.add(aggregationSumMap.get(key)+"");
//			writeToLog.add(aggregationMinMap.get(key)+"");
//			writeToLog.add(aggregationMaxMap.get(key)+"");
//			
//			StatisticLog.info(writeToLog);
//			
//			
//		}
		log.info(Client.class,"--------------Result join view--------------");
		log.info(Client.class,joinMap+"");
		log.info(Client.class,"  ");
		log.info(Client.class,"  ");
		

		
	}
	
	*/
	
	private void testPut(Configuration conf) throws IOException {
		
		
		HTable table=null;
		try {
			table = new HTable(conf, Bytes.toBytes("viewtable"));
		} catch (IOException e) {
			
			log.error(Client.class, e);
		}
		
		Put put = new Put(Bytes.toBytes("x1"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes(200));

		table.put(put);
		
		table.close();
		

		

	}
	
	
	private void testGet(Configuration conf) throws IOException {
		
		
		HTable table=null;
		try {
			table = new HTable(conf, Bytes.toBytes("viewtable"));
		} catch (IOException e) {
			
			log.error(Client.class, e);
		}
		
		Get get = new Get(Bytes.toBytes("x2"));

		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"));	
		
		Result result = table.get(get);
		
		byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"));

		if(val != null){

			System.out.println("Value: " + Bytes.toInt(val));
		}
		
		table.close();

	}	
	
	private void scanViewTable(String tableName, Configuration conf) throws IOException {
		
		System.out.println("----------------------------------------------");
		System.out.println(tableName);
		System.out.println("----------------------------------------------");

		long start = System.currentTimeMillis();
		
		HTable table = new HTable(conf, tableName);	
	
		System.out.println("Duration client setup in ms: " + (System.currentTimeMillis() - start));
		
		Scan scan1 = new Scan();
		
		start = System.currentTimeMillis();
		ResultScanner scanner1 = table.getScanner(scan1);
		
		
	
		
		int result = 0;
	
			
			List<KeyValue> curVals;
			Result done;
			do {
				
				curVals = new ArrayList<KeyValue>();
//				curVals.clear();
				done = scanner1.next();
				
				if(done != null){
				
					curVals = done.list();
//					System.out.println(curVals);

					for(KeyValue keyValue : curVals){
						
						String keyString = Bytes.toString(keyValue.getKey());
//						System.out.println(keyString);

						
						if(keyString.contains("aggregationValue")){
							
							System.out.println("Key: "+Bytes.toString(keyValue.getRow())+", Value: "+Bytes.toInt(keyValue.getValue()));
//							System.out.println("Value: "+Bytes.toInt(keyValue.getValue()));
							
							
							
							
							
							
						}	
					}
				}
			} while (done != null);	
	}
	



}
