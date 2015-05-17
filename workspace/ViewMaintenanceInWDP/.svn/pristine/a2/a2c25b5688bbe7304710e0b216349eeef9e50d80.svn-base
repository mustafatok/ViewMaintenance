package de.webdataplatform.client;


public class CopyOfClient {

	/**
	 * @param args
	 */
	
	/*
	private Configuration conf;
	
////	private String zookeeper;
////	private String evaluationMode;
//	private String tableName; 
//	private long numOfOperations;
//	private long numOfPrimaryKeys;
//	private int numOfRegions;
//	private int numOfRegionsView;
//	private int numOfAggKeys;
//	private int numOfAggValues;
//	private List<String> viewTableTypes;
//	private boolean useDeletes;
//	private boolean useUpdates;
//	private int viewTableCount;
//	private int clientNumber;
//	private int numOfClients;
//	private String distribution;
//	
//	private static String UNIFORM="UNIFORM";
//	private static String ZIPF="ZIPF";
	
	private Log log;
	
	
	public CopyOfClient(Log log){
		
//		this.zookeeper = zookeeper;
		
		NetworkConfig.load(log);
		EvaluationConfig.load(log);
		
		conf = NetworkConfig.getHBaseConfiguration(log);
		
		this.log = log;
		StatisticLog.name = "clientoperations";
		
		
		
		
//		log.info(Client.class, "mode: "+evaluationMode);
	}
	
//	public void scanBaseTable(String baseTable) {
//		
//		
//		this.tableName = baseTable;
//		log.info(Client.class, "tableName: "+tableName);
//		
//		try {
//			scanBaseTable(this.tableName, conf);
//		} catch (IOException e) {
//			log.error(Client.class, e);
//		}
//		
//		
//		
//	}
//	
//	public void scanBaseTableJoin(String tableLeft, String tableRight) {
//		
//		
//		log.info(Client.class, "tableName: "+tableName);
//		
//		try {
//			scanBaseTableJoin(tableLeft, tableRight, conf);
//		} catch (IOException e) {
//			log.error(Client.class, e);
//		}
//		
//		
//		
//	}
//	
//	public void scanViewTable(String viewTable) {
//		
//		this.tableName = viewTable;
//		log.info(Client.class, "tableName: "+tableName);
//		try {
//			scanViewTable(this.tableName, conf);
//		} catch (IOException e) {
//			log.error(Client.class, e);
//		}
//		
//		
//		
//	}
//	
//	
//	public void createBaseTables(List<String> viewTableTypes, long numOfPrimaryKeys, int numOfRegions, int numOfAggKeys) {
//		
//
//		
//		log.info(Client.class, "setting flag for base table to false ");
//		log.info(Client.class, "viewTableTypes: "+viewTableTypes);	
//		log.info(Client.class, "recordcount: "+numOfPrimaryKeys);
//		log.info(Client.class, "regioncount: "+numOfRegions);
//		log.info(Client.class, "numOfAggKeys: "+numOfAggKeys);	
//
//		PrintWriter writer=null;
//		
//		try {
//			writer = new PrintWriter("basetable-filled", "UTF-8");
//			
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (UnsupportedEncodingException e) {
//			e.printStackTrace();
//		}
//
//		writer.println("false");
//		writer.flush();
//		writer.close();
//		
//
//		
//		createBaseTables();
//		
//
//		
//	}
//	
	
	public void fillBaseTables(int clientNumber, int numOfClients, String distribution, List<String> viewTableTypes, long numOfOperations, long numOfPrimaryKeys, int numOfRegions, int numOfAggKeys, int numOfAggValues, boolean useDeletes, boolean useUpdates) {
		
	
		log.info(CopyOfClient.class, "distribution: "+distribution);
		log.info(CopyOfClient.class, "viewTableTypes: "+viewTableTypes);
		log.info(CopyOfClient.class, "numOfOperations: "+numOfOperations);
		log.info(CopyOfClient.class, "numOfPrimaryKeys: "+numOfPrimaryKeys);
		log.info(CopyOfClient.class, "numOfRegions: "+numOfRegions);
		log.info(CopyOfClient.class, "numOfAggKeys: "+numOfAggKeys);	
		log.info(CopyOfClient.class, "numOfAggValues: "+numOfAggValues);	
		log.info(CopyOfClient.class, "useDeletes: "+useDeletes);	
		log.info(CopyOfClient.class, "useUpdates: "+useUpdates);	

		PrintWriter writer=null;
		
		try {
			writer = new PrintWriter("basetable-filled", "UTF-8");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		writer.println("false");
		writer.flush();
		writer.close();
		
		
		fillBaseTables();
		

		log.info(CopyOfClient.class, "setting flag for base table to true ");
		writer=null;
		
		try {
			writer = new PrintWriter("basetable-filled", "UTF-8");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		writer.println("true");
		writer.flush();
		writer.close();
		
	}
	
	
	
	
	
	
//	public void createViewTables(List<String> viewTableTypes, int viewTableCount, long numOfPrimaryKeys, int numOfAggKeys, int numOfRegionsView) {
//		
//		
//		log.info(Client.class, "viewTableTypes: "+viewTableTypes);	
//		log.info(Client.class, "viewTableCount: "+viewTableCount);	
//		log.info(Client.class, "numOfPrimaryKeys: "+numOfPrimaryKeys);
//		log.info(Client.class, "numOfAggKeys: "+numOfAggKeys);
//		log.info(Client.class, "numOfRegionsView: "+numOfRegionsView);
//
//		
//		createViewTables();	
//
//		
//	}



	private void deleteTable(String name) {
		
		
//		if(EvaluationConfig.CLIENT_CREATECONTROLTABLES){
//			log.info(Client.class, "deleting control tables");
//			
//	
//			for (int i = 0; i < viewTableCount; i++) {
//				
			try {
//			deleteTable(viewTableTypes.get(i%viewTableTypes.size())+(i/viewTableTypes.size()+"_controll"), conf);
			
				deleteTable(name, conf);
			
			} catch (IOException e) {
				log.error(CopyOfClient.class, e);
			}
//			}
//		}

//		
//		log.info(Client.class, "deleting view tables");
//		
//		for (int i = 0; i < viewTableCount; i++) {
//			
//			try {
//				deleteTable(viewTableTypes.get(i%viewTableTypes.size())+(i/viewTableTypes.size()), conf);
//			} catch (IOException e) {
//				log.error(Client.class, e);
//			}
//		}
//		
//		try {
//			deleteTable("viewdefinitions", conf);
//		} catch (IOException e) {
//			log.error(Client.class, e);
//		}
		
	}

//	private void createViewTables(String name, String rowKeyPrefix, int recordCount, int regionCount) {
		


		
//		if(EvaluationConfig.CLIENT_CREATECONTROLTABLES){
			
//			log.info(Client.class, "creating control tables");
//
//			for (int i = 0; i < viewTableCount; i++) {
//				String viewTableType = viewTableTypes.get(i%viewTableTypes.size());
//	
//				if(viewTableType.equals("count") || viewTableType.equals("sum") || viewTableType.equals("min") || viewTableType.equals("max")){
					
//					createRangeSplitTable(viewTableType+(i/viewTableTypes.size())+"_controll","x", numOfAggKeys, numOfRegionsView, conf);
//					createRangeSplitTable(name, rowKeyPrefix, recordCount, regionCount, conf);
//				}
//				if(viewTableType.equals("join") || viewTableType.equals("selection")){
					
//					createRangeSplitTable(viewTableType+(i/viewTableTypes.size())+"_controll","k", numOfPrimaryKeys, numOfRegionsView, conf);
//					createRangeSplitTable(viewTableType+(i/viewTableTypes.size())+"_controll","k", numOfPrimaryKeys, numOfRegionsView, conf);
//				}
				
//			}
//		}

//		log.info(Client.class, "creating view tables");
//		
//		
//		for (int i = 0; i < viewTableCount; i++) {
//			String viewTableType = viewTableTypes.get(i%viewTableTypes.size());
//
//			if(viewTableType.equals("count") || viewTableType.equals("sum") || viewTableType.equals("min") || viewTableType.equals("max")){
//				
//				createRangeSplitTable(viewTableType+(i/viewTableTypes.size()),"x", numOfAggKeys, numOfRegionsView, conf);
//			}
//			if(viewTableType.equals("join") || viewTableType.equals("selection")){
//				
//				createRangeSplitTable(viewTableType+(i/viewTableTypes.size()),"k", numOfPrimaryKeys, numOfRegionsView, conf);
//			}
//			
//		}

		

//		try {
//			createTable("viewdefinitions", conf);
//		} catch (IOException e) {
//			log.error(Client.class, e);
//		}
//
//		log.info(Client.class, "filling viewdefinitions");
//		try {
//			fillBaseTableViewDefinitions("viewdefinitions", conf);
//		} catch (IOException e) {
//			log.error(Client.class, e);
//		}
//		log.info(Client.class, "view tables created");
//	}
//	
	

//
//	
//	private void createBaseTables() {
//		
//		
//		
//			log.info(Client.class, "deleting base tables: ");
//			try {
//				deleteTable("basetable", conf);
//			} catch (IOException e) {
//				log.error(Client.class, e);
//			}
//			
//			try {
//				deleteTable("basetable_joinleft", conf);
//			} catch (IOException e) {
//				log.error(Client.class, e);
//			}
//			try {
//				deleteTable("basetable_joinright", conf);
//			} catch (IOException e) {
//				log.error(Client.class, e);
//			}		
//			
//			
//			if(viewTableTypes.contains("count") || viewTableTypes.contains("sum") || viewTableTypes.contains("min") || viewTableTypes.contains("max")|| viewTableTypes.contains("selection")){
//				log.info(Client.class, "creating base tables");
//				createRangeSplitTable("basetable","k", numOfPrimaryKeys, numOfRegions, conf);
//			}	
//			if(viewTableTypes.contains("join")){
//				createRangeSplitTable("basetable_joinleft","k", numOfPrimaryKeys, numOfRegions, conf);
//				createRangeSplitTable("basetable_joinright","x", numOfAggKeys, numOfRegions, conf);
//			}
//			
//	}
	
	public void fillBaseTables(){

			log.info(CopyOfClient.class, "filling base tables");
			try {
				if(viewTableTypes.contains("count") || viewTableTypes.contains("sum") || viewTableTypes.contains("min") || viewTableTypes.contains("max")|| viewTableTypes.contains("selection")){
					fillBaseTable("basetable", conf);
					

					
				}
				if(viewTableTypes.contains("join")){
					fillJoinTables("basetable_joinleft", "basetable_joinright", conf);
				}
				
				log.info(CopyOfClient.class, "base table filled");
			} catch (Exception e) {
				log.error(CopyOfClient.class, e);
			}
			

			
			
	}
	







	private void deleteTable(String tableName, Configuration conf) throws IOException {
		
		log.info(CopyOfClient.class,"-----------------------");
		log.info(CopyOfClient.class,"delete table");
		log.info(CopyOfClient.class,"-----------------------");
		
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		admin.disableTable(Bytes.toBytes(tableName));
		boolean isDisabled = admin.isTableDisabled(Bytes.toBytes(tableName));
		log.info(CopyOfClient.class,"Table is disabled: " + isDisabled);
		
		boolean avail1 = admin.isTableAvailable(Bytes.toBytes(tableName));
		log.info(CopyOfClient.class,"Table available: " + avail1);
		
		try {
			
			admin.deleteTable(Bytes.toBytes(tableName));
		
		} catch (IOException e) {
			
			log.error(CopyOfClient.class, e);
		
		}
		
		admin.close();
		
		

		

	}
	
	private void createTable(String tableName, Configuration conf) throws IOException {

		log.info(CopyOfClient.class,"-----------------------");
		log.info(CopyOfClient.class,"create table");
		log.info(CopyOfClient.class,"-----------------------");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
		
		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
		
		desc.addFamily(coldef);
		
		desc.setDeferredLogFlush(false);
		
//		desc.set
		
		admin.createTable(desc);
		
		boolean avail = admin.isTableAvailable(Bytes.toBytes(tableName));
		
//		admin.enableTable(Bytes.toBytes(tableName));
		
		log.info(CopyOfClient.class,"Table available: " + avail);
		log.info(CopyOfClient.class,"Table enabled: " + admin.isTableEnabled(Bytes.toBytes(tableName)));
		
		admin.close();
		

	}
	

	
	private void createRangeSplitTable(String tableName, String rowKeyPrefix, long recordCount, int regCount, Configuration conf){
	
	
		
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
	
			log.error(CopyOfClient.class, e);
		} catch (ZooKeeperConnectionException e) {
	
			log.error(CopyOfClient.class, e);
		}
		
		HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
		
		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
		
		desc.addFamily(coldef);
		
		long recordsPerRegion = recordCount/regCount;
		log.info(CopyOfClient.class,"recordsPerRegion: "+recordsPerRegion);
		
		int digits = String.valueOf(recordCount).length();
		byte[][] regions = new byte[regCount][];
		
		for (int i = 0; i < regCount; i++) {
			
		
			String rowKey = rowKeyPrefix;
			for(int x = 0; x < (digits - String.valueOf((i*recordsPerRegion)).length());x++)rowKey+="0";
			rowKey += (i*recordsPerRegion);
				
			log.info(CopyOfClient.class,"rowkey: "+rowKey);
			regions[i] = Bytes.toBytes(rowKey);
			
		}
//		System.out.println(Arrays.toString(regions));
		
//		byte[][] regions = new byte[][] {
//		Bytes.toBytes("k0"),
//		Bytes.toBytes("k50")
//		};

		try {
			admin.createTable(desc, regions);
		} catch (IOException e) {
		
			log.error(CopyOfClient.class, e);
		}
		printTableRegions(conf, tableName);
	
	
	
}
	
	
	
	private void printTableRegions(Configuration conf, String tableName){
	
	
		log.info(CopyOfClient.class,"Printing regions of table: " + tableName);
	
	HTable table=null;
	try {
		table = new HTable(conf, Bytes.toBytes(tableName));
	} catch (IOException e) {
		
		log.error(CopyOfClient.class, e);
	}
	
	Pair<byte[][], byte[][]> pair=null;
	try {
		pair = table.getStartEndKeys();
	} catch (IOException e) {

		log.error(CopyOfClient.class, e);
	}
	
	for (int n = 0; n < pair.getFirst().length; n++) {
		
	byte[] sk = pair.getFirst()[n];
	
	byte[] ek = pair.getSecond()[n];
	
	log.info(CopyOfClient.class,"[" + (n + 1) + "]" +
	" start key: " +
	(sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
	", end key: " +
	(ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
	}
	
	try {
		table.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		log.error(CopyOfClient.class, e);
	}
}	
	

	
	private void fillBaseTable(String baseTableName, Configuration conf) throws Exception {
		
		log.info(CopyOfClient.class,"-----------------------");
		log.info(CopyOfClient.class,"fill table:"+baseTableName);
		log.info(CopyOfClient.class,"-----------------------");
		
		HTable baseTable = new HTable(conf, baseTableName);
		long operationsPerClient = numOfOperations/numOfClients;
		
		log.info(this.getClass(), "recordsPerClient: "+operationsPerClient);
//		log.info(this.getClass(), "start: "+((clientNumber-1)*recordsPerClient)+1);
//		log.info(this.getClass(), "end: "+(clientNumber*recordsPerClient));
			
//		long start = ((clientNumber-1)*recordsPerClient)+1;
//		long end = (clientNumber*recordsPerClient);
		
		Random random = new Random();
		ZipfDistribution zd = new ZipfDistribution(new Long(numOfPrimaryKeys).intValue(), 1); 
		
		int sysoutCount=0;
		for (long i = 0; i < operationsPerClient; i++) {
			sysoutCount++;	

			long k = 0;
			
			if(distribution.equals(UNIFORM))k=random.nextInt(new Long(numOfPrimaryKeys).intValue());
			if(distribution.equals(ZIPF))k=zd.sample();
			
			int digits = String.valueOf(numOfPrimaryKeys).length();
			
			String rowKey = "k";
			for(int x = 0; x < (digits - String.valueOf(k).length());x++)rowKey+="0";
			rowKey += k;
			
			Get get = new Get(Bytes.toBytes(rowKey));
			boolean exists = baseTable.exists(get);
			if(sysoutCount == 10000){
				log.info(this.getClass(), "iteration: "+i+", key: "+rowKey+", exists:"+exists);
				sysoutCount = 0;
			}
			if(!exists){
				
				
				Put put = generateInsert(rowKey);
				baseTable.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), null, put);
				
				
			}else{
				
				int zahl = (int)(Math.random() * 2);
				switch(zahl){ 
					case 0: Put update = generateUpdate(rowKey);baseTable.put(update);
						break;
					case 1: Delete delete = generateDelete(rowKey);baseTable.delete(delete);
						break;
				}
				
				
			}
			
			


		}

		
		baseTable.close();
	

	}
	


	private Put generateInsert(String rowKey) {
		

		int digitsAggKeys = String.valueOf(numOfAggKeys).length();
		
		int zahl = (int)(Math.random() * numOfAggKeys + 1);
		String aggregationKey = "x";
		for(int x = 0; x < (digitsAggKeys - String.valueOf(zahl).length());x++)aggregationKey+="0";
		aggregationKey+=zahl;
		
		int aggregationValue = (int)(Math.random() * numOfAggValues + 1);
		
//		log.info(this.getClass(), "putting key: "+rowKey);
//		aggKeys.put(rowKey, aggregationKey);
		
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes(aggregationKey));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(aggregationValue+""));
		return put;
	}
	
	
	private Put generateUpdate(String rowKey) {
		
//		int updateCount = (int)(Math.random() * rowKeys.size());
//		String rowKey = rowKeys.get(updateCount);

		int digitsAggKeys = String.valueOf(numOfAggKeys).length();
		int zahl = (int)(Math.random() * numOfAggKeys + 1);
		String aggregationKey = "x";
		for(int x = 0; x < (digitsAggKeys - String.valueOf(zahl).length());x++)aggregationKey+="0";
		aggregationKey+=zahl;
		
		int aggregationValue = (int)(Math.random() * numOfAggValues + 1);
		
//		log.info(this.getClass(), "updating key: "+rowKey);
		
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes(aggregationKey));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(aggregationValue+""));
		return put;
	}


	private Delete generateDelete(String rowKey) {
		
		
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));

		

		return delete;
	}	

	
	
///////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	
	private void fillJoinTables(String joinTableLeft, String joinTableRight, Configuration conf) throws IOException {
		
		log.info(CopyOfClient.class,"-----------------------");
		log.info(CopyOfClient.class,"fill join tables: joinTableLeft:"+joinTableLeft+", joinTableRight"+joinTableRight);
		log.info(CopyOfClient.class,"-----------------------");
		
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

	
	
	
	private void fillBaseTableViewDefinitions(String tableName, Configuration conf) throws IOException {
		
		log.info(CopyOfClient.class,"-----------------------");
		log.info(CopyOfClient.class,"fill view definition table");
		log.info(CopyOfClient.class,"-----------------------");
		
		HTable table = new HTable(conf, tableName);

		Put put = new Put(Bytes.toBytes("basetable"));
		Put put2 = new Put(Bytes.toBytes("basetable_joinleft"));
		Put put3 = new Put(Bytes.toBytes("basetable_joinright"));

		
		
		log.info(CopyOfClient.class, "creating view tables");

		for (int i = 0; i < viewTableCount; i++) {
			String viewTableType = viewTableTypes.get(i%viewTableTypes.size());

			if(viewTableType.equals("count")){
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_COUNT.toString()+","+"aggregationKey"+","+"aggregationValue"));
			}
			if(viewTableType.equals("sum")){
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_SUM.toString()+","+"aggregationKey"+","+"aggregationValue"));
			}
			if(viewTableType.equals("min")){
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_MIN.toString()+","+"aggregationKey"+","+"aggregationValue"));
			}
			if(viewTableType.equals("max")){
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes( ViewMode.AGGREGATION_MAX.toString()+","+"aggregationKey"+","+"aggregationValue"));
			}
			if(viewTableType.equals("selection")){
				put.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes(ViewMode.SELECTION.toString()+","+"aggregationValue,<,5"));
			}
			if(viewTableType.equals("join")){
				put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes(ViewMode.JOIN.toString()+",basetable_joinleft,basetable_joinright,aggregationKey"));
				put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes(viewTableType+(i/viewTableTypes.size())), Bytes.toBytes(ViewMode.JOIN.toString()+",basetable_joinleft,basetable_joinright,aggregationKey"));
			}
			
		}
		
		if(viewTableTypes.contains("count") || viewTableTypes.contains("sum") || viewTableTypes.contains("min") || viewTableTypes.contains("max")|| viewTableTypes.contains("selection")){
			
			table.put(put);
		}
		if(viewTableTypes.contains("join")){
			
			table.put(put2);
			table.put(put3);
		}
		table.close();

	}	
	
	
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
								log.error(CopyOfClient.class, e);
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
			
			log.info(CopyOfClient.class,"Duration scan in ms: " + (System.currentTimeMillis() - start));
		} finally {
			scanner1.close();
		}
		

		log.info(CopyOfClient.class,"--------------Scan of basetable--------------");
		
		
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
		log.info(CopyOfClient.class,"--------------Result count view--------------");
		log.info(CopyOfClient.class,"countresult: "+aggregationCountMap+"");
		log.info(CopyOfClient.class,"  ");
		log.info(CopyOfClient.class,"  ");
		
		log.info(CopyOfClient.class,"--------------Result sum view--------------");
		log.info(CopyOfClient.class,"sumresult: "+aggregationSumMap+"");
		log.info(CopyOfClient.class,"  ");
		log.info(CopyOfClient.class,"  ");
		
		log.info(CopyOfClient.class,"--------------Result min view--------------");
		log.info(CopyOfClient.class,"minresult: "+aggregationMinMap+"");
		log.info(CopyOfClient.class,"  ");
		log.info(CopyOfClient.class,"  ");
		
		log.info(CopyOfClient.class,"--------------Result max view--------------");
		log.info(CopyOfClient.class,"maxresult: "+aggregationMaxMap+"");
		log.info(CopyOfClient.class,"  ");
		log.info(CopyOfClient.class,"  ");
		
	}
	
private void scanBaseTableJoin(String tableLeft, String tableRight, Configuration conf) throws IOException {
		
		
		Map<String, List<String>> joinMap = new HashMap<String, List<String>>();
		

		
		long start = System.currentTimeMillis();
		
		HTable table = new HTable(conf, tableLeft);	
		BaseTableService baseTableService = new BaseTableService(log);
		
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
								log.error(CopyOfClient.class, e);
							}
								
							
						}
						
					}
					joinMap.put(joinKey, joinValues);
					
				
				
				}
				
//				result += countKeyValues ? curVals.size() : 1;
			} while (done != null);
			
			log.info(CopyOfClient.class,"Duration scan in ms: " + (System.currentTimeMillis() - start));
		} finally {
			scanner1.close();
		}
		

		log.info(CopyOfClient.class,"--------------Scan of basetable--------------");
		
		
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
		log.info(CopyOfClient.class,"--------------Result join view--------------");
		log.info(CopyOfClient.class,joinMap+"");
		log.info(CopyOfClient.class,"  ");
		log.info(CopyOfClient.class,"  ");
		

		
	}
	
	

//	private static void getViewTableAggregation(Configuration conf) throws IOException {
//		HTable table = new HTable(conf, "viewtable_aggregation");
//		
//		
//		Get get = new Get(Bytes.toBytes("x4"));
//		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"));	
//		Result result = table.get(get);
//		byte[] val = result.getValue(Bytes.toBytes("colfam1"),
//		Bytes.toBytes("aggregatedValue"));
//		System.out.println("Value: " + Bytes.toInt(val));
//
//
//		get = new Get(Bytes.toBytes("x5"));
//		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"));	
//		result = table.get(get);
//		val = result.getValue(Bytes.toBytes("colfam1"),Bytes.toBytes("aggregatedValue"));
//		System.out.println("Value: " + Bytes.toInt(val));
//
//
//	}
//	
//	private static void initializeViewTableAggregation(Configuration conf) throws IOException {
//		HTable table = new HTable(conf, "viewtable_aggregation");
//		
//		
//		Put put = new Put(Bytes.toBytes("x7"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes(100));	
//		table.put(put);
//
//		put = new Put(Bytes.toBytes("x8"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes(300));	
//		table.put(put);		
//
//	}
//	
	private void testPut(Configuration conf) throws IOException {
		
		
		HTable table=null;
		try {
			table = new HTable(conf, Bytes.toBytes("viewtable"));
		} catch (IOException e) {
			
			log.error(CopyOfClient.class, e);
		}
		
		Put put = new Put(Bytes.toBytes("x1"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes(200));

		table.put(put);
		
		table.close();
		
		
//		List<KeyValue> parameter = put.getFamilyMap().get(Bytes.toBytes("colfam1"));
//		
//		for(KeyValue keyValue : parameter){
//			
//			String keyString = Bytes.toString(keyValue.getKey());
//			
//			System.out.println(keyString);
//			
//			System.out.println(Bytes.toString(keyValue.getRow()));
//			
//			if(keyString.contains("aggregationKey")){
//				
//				String aggregationKey = Bytes.toString(keyValue.getValue());
//				
//				System.out.println(aggregationKey);
//							
//				
//			}
//			if(keyString.contains("aggregationValue")){
//
//				Integer newValue = Bytes.toInt(keyValue.getValue());
//				
//
//				System.out.println(newValue);
//				
//			}
//			
//		}
		

	}
	
	
	private void testGet(Configuration conf) throws IOException {
		
		
		HTable table=null;
		try {
			table = new HTable(conf, Bytes.toBytes("viewtable"));
		} catch (IOException e) {
			
			log.error(CopyOfClient.class, e);
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
	
//	private static void printTableRegions(Configuration conf, String tableName){
//		
//		
//		System.out.println("Printing regions of table: " + tableName);
//		
//		HTable table=null;
//		try {
//			table = new HTable(conf, Bytes.toBytes(tableName));
//		} catch (IOException e) {
//			
//			log.error(EvaluationSetup.class, e);
//		}
//		
//		Pair<byte[][], byte[][]> pair=null;
//		try {
//			pair = table.getStartEndKeys();
//		} catch (IOException e) {
//
//			log.error(EvaluationSetup.class, e);
//		}
//		
//		for (int n = 0; n < pair.getFirst().length; n++) {
//			
//		byte[] sk = pair.getFirst()[n];
//		
//		byte[] ek = pair.getSecond()[n];
//		
//		System.out.println("[" + (n + 1) + "]" +
//		" start key: " +
//		(sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
//		", end key: " +
//		(ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
//		}
//		
//		
//	}
//	
//	private static void createNewTable(){
//		
//		
//		
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "192.168.127.129");
//		
//		HBaseAdmin admin=null;
//		try {
//			admin = new HBaseAdmin(conf);
//		} catch (MasterNotRunningException e) {
//
//			log.error(EvaluationSetup.class, e);
//		} catch (ZooKeeperConnectionException e) {
//	
//			log.error(EvaluationSetup.class, e);
//		}
//		
//		HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes("testtable1"));
//		
//		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
//		
//		desc.addFamily(coldef);
//		
//		try {
//			admin.createTable(desc, Bytes.toBytes(1L), Bytes.toBytes(100L), 10);
//		} catch (IOException e) {
//		
//			log.error(EvaluationSetup.class, e);
//		}
//		
//		printTableRegions(conf, "testtable1");
//		
//		
//		byte[][] regions = new byte[][] {
//		Bytes.toBytes("A"),
//		Bytes.toBytes("D"),
//		Bytes.toBytes("G"),
//		Bytes.toBytes("K"),
	
	
	
//		Bytes.toBytes("O"),
//		Bytes.toBytes("T")
//		};
//		desc.setName(Bytes.toBytes("testtable2"));
//		try {
//			admin.createTable(desc, regions);
//		} catch (IOException e) {
//		
//			log.error(EvaluationSetup.class, e);
//		}
//		printTableRegions(conf, "testtable2");
//		
//		
//		
//	}
//	
//	
//	
//	private static void updateTestTable(Configuration conf) throws IOException {
//
//		HTable table = new HTable(conf, "testtable1");
//
//		
//		Put put=null;
//		for (int i = 0; i < 150; i++) {
//			
//			
//			int random = (int) (Math.random()*100+1);
//			System.out.println("i:"+i+"random:"+random);
//			put = new Put(Bytes.toBytes(i));
//			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x"+i));	
//
//			table.put(put);
//		}
//		
//		table.flushCommits();
//		
//
//	}
//	
//	private static void updateBaseTable(Configuration conf) throws IOException {
//		HTable table = new HTable(conf, "basetable");
//		
//		Put put = new Put(Bytes.toBytes("k2"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(200));
//
//		table.put(put);
//		
//		put = new Put(Bytes.toBytes("k4"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x1"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(400));
//
//		table.put(put);
//		
//		put = new Put(Bytes.toBytes("k5"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x1"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(400));
//
//		table.put(put);
//		
//		put = new Put(Bytes.toBytes("k6"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(200));		
//		
//		table.put(put);
//		
//		put = new Put(Bytes.toBytes("k7"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x3"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(350));		
//		
//		table.put(put);
//		put = new Put(Bytes.toBytes("k8"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x3"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(600));		
//		
//		table.put(put);
//		put = new Put(Bytes.toBytes("k9"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x4"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(1200));		
//		
//		table.put(put);
//	}
//
//
*/

}
