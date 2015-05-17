package de.webdataplatform.view;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.BytesUtil;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;


public class TableService{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7481947086820528375L;


	private Configuration conf;
	
//	private String zookeeperAddress;
//	
//	private String aggregationKey;
	
	private Map<String, HTable> tableCache;
	
	private Log log;
	
	

	public TableService(Log log){
		
		this.tableCache = new HashMap<String, HTable>();
		this.log = log;
		
	}

	public void close(){
		
		for (String tableName : tableCache.keySet()) {
			try {
				tableCache.get(tableName).close();
			} catch (IOException e) {
			
				e.printStackTrace();
			}
		}
		
	}
	

	
//	private HTable table;
	
	private HTable initTable(String tableName) {
		
		HTable table=null;
		
		conf = NetworkConfig.getHBaseConfiguration(log);
		
		try {
			table = new HTable(conf, tableName);
		} catch (IOException e1) {

			log.error(this.getClass(), e1);
			e1.printStackTrace();
		}
		
		return table;
	}	
	
	

	
	public HTable getTable(String tableName) {
		
		HTable table = tableCache.get(tableName);
		
		if(table == null){
			
			table = initTable(tableName);
			tableCache.put(tableName, table);
		}
		
		
		return table;
	}



	public static boolean isInteger(String s) {
	    try { 
	        Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    }
	    // only got here if we didn't return false
	    return true;
	}
	
	


//////////////////////////////////////////PUT/////////////////////////////////////////////////	
	

//	public void put(String tableName, String key, String colFam,  Integer value) throws RemoteException {
//		
//		Put put = new Put(Bytes.toBytes(key));
//		put.add(Bytes.toBytes(colFam), Bytes.toBytes("aggVal"), Bytes.toBytes(value));	
//		
//		try {
//
//			getTable(tableName).put(put);
//
//		} catch (IOException e) {
//
//			log.error(this.getClass(), e);
//		}
//		
//	}
	
	public void put(byte[] viewTableName, byte[] key, byte[] columnFamily, Put put, byte[] signature) {
		
		
		checkAndPut(viewTableName, key, columnFamily, null, null, put, signature);
	}
	
	public void put(byte[] viewTableName, byte[] key, byte[] columnFamily, Map<byte[], byte[]> columns, byte[] signature) {
		
		
		checkAndPut(viewTableName, key, columnFamily, null, null, columns, signature);
	}
	
	public boolean checkAndPut(byte[] viewTableName, byte[] key,  byte[] columnFamily, byte[] checkQualifier, byte[] checkValue, Map<byte[], byte[]> columns, byte[] signature) {
		
//		log.update(this.getClass(), "putting to table:"+viewTableName+",  key="+key+", checkQualifier="+checkQualifier+", checkValue="+checkValue+", columns="+MatchingRow.mapToString(columns));
		
		Put put = new Put(key);
		
		for (byte[] columnQualifier : columns.keySet()) {
			byte[] value = columns.get(columnQualifier);
			put.add(columnFamily, columnQualifier, value);	
		}

		return checkAndPut(viewTableName, key,  columnFamily, checkQualifier, checkValue, put, signature);

	}
	
	public boolean checkAndPut(byte[] viewTableName, byte[] key,  byte[] columnFamily, byte[] checkQualifier, byte[] checkValue, Put put, byte[] signature) {
		
//		log.update(this.getClass(), "putting to table:"+viewTableName+",  key="+key+", checkQualifier="+checkQualifier+", checkValue="+checkValue+", columns="+MatchingRow.mapToString(columns));
		

		if(signature != null){
			put.add(Bytes.toBytes("sigfam1"), signature, Bytes.toBytes(""));	
		}
		
		boolean check=false;
		try {

			long startTime = new Date().getTime();

			if(checkQualifier == null){
				getTable(Bytes.toString(viewTableName)).put(put);
			}else{
				
				check = getTable(Bytes.toString(viewTableName)).checkAndPut(key,  columnFamily, checkQualifier,  checkValue, put);
			}
			
			long stopTime = new Date().getTime();
			
			if((stopTime - startTime) > SystemConfig.VIEWMANAGER_CRUDOPERATIONSALARMTHRESHOLD){
				log.info(this.getClass(), "put very slow: "+(stopTime - startTime)+" ms:  tableName="+viewTableName+", key="+key+", qualifierValue="+checkQualifier);
			}

		} catch (IOException e) {

			log.error(this.getClass(), e);
		}
		return check;
	}	
	
	
	
//////////////////////////////////////////GET/////////////////////////////////////////////////
	

	public Result get(String tableName, String key) throws RemoteException {
		
		
		Get get = new Get(Bytes.toBytes(key));
		Result result=null;
		int s=0;
		try {

	
			
			result = getTable(tableName).get(get);
			

//			if(result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggValue")) != null)
//			s = Bytes.toInt(result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggValue")));


			
		} catch (IOException e) {

			log.error(this.getClass(), e);
		}
		

		
		return result;
	}
	
	public Result get(byte[] tableName, byte[] key, List<byte[]> columnFamilies, List<byte[]> columns, byte[] signature) {
		
		return getWithTimestampRange(tableName, key, columnFamilies, columns, null, null, signature);
	}

	
	public Result getWithTimestampRange(byte[] tableName, byte[] key, List<byte[]> columnFamilies, List<byte[]> columns, Long tMin, Long tMax, byte[] signature) {	
		
		Get get = new Get(key);
		
		if(columns == null && columnFamilies != null){
			
			for (byte[] colFam : columnFamilies) {
				
				get.addFamily(colFam);
			}
		}
		if(columns != null && columnFamilies != null){
			for(byte[] column : columns){
				
				get.addColumn(columnFamilies.get(0), column);
			}
		}
		if(signature != null){
			get.addColumn(Bytes.toBytes("sigfam1"), signature);	
		}
		
		if(tMin != null && tMax != null){
			
			try {
				get.setTimeRange(tMin, tMax);
			} catch (IOException e) {

				log.error(this.getClass(), e);
			}
		}
		
		log.update(this.getClass(), "get: tableName="+Bytes.toString(tableName)+", key="+Bytes.toString(key)+", cols="+BytesUtil.listToString(columns));

		try {

	
			long startTime = new Date().getTime();

			Result result = getTable(Bytes.toString(tableName)).get(get);
			
			if(result.isEmpty())return null;
			
			long stopTime = new Date().getTime();
			
			if((stopTime - startTime) > SystemConfig.VIEWMANAGER_CRUDOPERATIONSALARMTHRESHOLD){
				log.info(this.getClass(), "get very slow: "+(stopTime - startTime)+" ms:  tableName="+tableName+", key="+key+",  cols="+BytesUtil.listToString(columns));
			}

			log.update(this.getClass(), "tmpResult: "+result);
			
			return result;
			
		} catch (IOException e) {

			log.error(this.getClass(), e);
		}
		
		
		
		return null;
	}
	

	
	
//////////////////////////////////////////DELETE/////////////////////////////////////////////////	
//	
//	public void delete(String tableName, String x) throws RemoteException {
//		
//		Delete delete = new Delete(Bytes.toBytes(x));
//		
//		try {
//			getTable(tableName).delete(delete);
//			
//		} catch (IOException e) {
//	
//			log.error(this.getClass(), e);
//		}
//		
//	
//	}


	
	
	public boolean delete(byte[] tableName, byte[] key, byte[] colFam,  List<byte[]> columns){ 
		
		return checkAndDeleteWithSignature(tableName, key, colFam, null,  null, columns, null);
	}
	
	public boolean deleteWithSignature(byte[] tableName, byte[] key, byte[] colFam,  List<byte[]> columns, byte[] signature){ 
	
		return checkAndDeleteWithSignature(tableName, key, colFam, null,  null, columns, signature);
	}
	
	public boolean checkAndDelete(byte[] tableName, byte[] key,  byte[] colFam, byte[] checkQualifier,  byte[] checkValue, List<byte[]> columns){ 
		
		return checkAndDeleteWithSignature(tableName, key, colFam, checkQualifier,  checkValue, columns, null);
	}
	
	public boolean checkAndDeleteWithSignature(byte[] tableName, byte[] key, byte[] colFam, byte[] checkQualifier,  byte[] checkValue, List<byte[]> columns, byte[] signature) {
		

		log.update(this.getClass(), "deleting from table:"+tableName+",  key="+key+", checkQualifier="+checkQualifier+", checkValue="+checkValue+", signature="+signature);
		
		Delete delete = new Delete(key);

		for(byte[] col : columns){
			delete.deleteColumns(colFam, col);
		}
		
		
		boolean check=false;
		
		try {
			
			long currentTime = new Date().getTime();

			if(checkQualifier == null){
				getTable(Bytes.toString(tableName)).delete(delete);
				check=true;
			}else{
				check = getTable(Bytes.toString(tableName)).checkAndDelete(key, colFam, checkQualifier,checkValue, delete);
			}

			if(signature != null && check == true){
				
				Put put = new Put(key);
				put.add(Bytes.toBytes("sigfam1"), signature, Bytes.toBytes(""));
				getTable(Bytes.toString(tableName)).put(put);
			}

			long currentTime2 = new Date().getTime();
			
			if((currentTime2 - currentTime) > SystemConfig.VIEWMANAGER_CRUDOPERATIONSALARMTHRESHOLD){
				log.info(this.getClass(), "checkAndDeleteWithSignature very slow: "+(currentTime2 - currentTime)+" ms:  tableName="+tableName+", key="+key);
			}
			
			
		} catch (IOException e) {
	
			log.error(this.getClass(), e);
		}
		
		return check;
	}
	
	
//	public void deleteWithRegEx(byte[] tableName, byte[] key, List<byte[]> columns, byte[] signature) {
//		
//
//		Scan scan = new Scan();
////		scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-0"));
//
//		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*"+Bytes.toString(key)+".*"));
//		scan.setFilter(filter2);
//		ResultScanner scanner2= null;
//		try {
//			scanner2 = getTable(Bytes.toString(tableName)).getScanner(scan);
//		} catch (IOException e) {
//
//			e.printStackTrace();
//		}
//		for (Result res : scanner2) {
//		System.out.println(res);
//			deleteWithSignature(tableName, key, columns, signature);
//		}
//		scanner2.close();
//	}	
	
//	public List<Result> getWithRegEx(byte[] tableName, byte[] keyPart) {
//		
//		String regex = ".*"+Bytes.toString(keyPart)+".*";
////		System.out.println("regex:"+regex);
//		Scan scan = new Scan();
////		scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-0"));
//
//		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(regex));
//		scan.setFilter(filter2);
//		ResultScanner scanner2= null;
//		try {
//			scanner2 = getTable(Bytes.toString(tableName)).getScanner(scan);
//		} catch (IOException e) {
//
//			e.printStackTrace();
//		}
//		List<Result> result = new ArrayList<Result>();
//		for (Result res : scanner2) {
////		System.out.println(res);
////		System.out.println(BytesUtil.mapToString(res.getFamilyMap(Bytes.toBytes("colfam1"))));
//		result.add(res);
//		}
//		scanner2.close();
//		
//		return result;
//	}	
	


	

//
	public List<Result> scan(String tableName){
		
		Scan scan1 = new Scan();
		ResultScanner scanner1=null;
		try {
			scanner1 = getTable(tableName).getScanner(scan1);
		} catch (IOException e) {
	
			e.printStackTrace();
		}
		List<Result> result = new ArrayList<Result>();
		for (Result res : scanner1) {
			
			result.add(res);

		}
		scanner1.close();
		
		return result;
		
	}
//	
//
//	public List<Result> scanValue(String tableName, String keyQualifier, String value){
//		
//
//		
//		Scan scan1 = new Scan();
////		scan1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(keyQualifier));
//
//		ResultScanner scanner1=null;
//		try {
//			scanner1 = getTable(tableName).getScanner(scan1);
//		} catch (IOException e) {
//	
//			e.printStackTrace();
//		}
//		List<Result> results = new ArrayList<Result>();
//		for (Result res : scanner1) {
//			
//			
//			KeyValue keyValueKey = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(keyQualifier));
//
////			System.out.println(Bytes.toString(keyValueKey.getValue()));
//			if(value.equals(Bytes.toString(keyValueKey.getValue()))){
////				System.out.println(keyValueKey);
//				results.add(res);
//
//			}
//
//		}
//		scanner1.close();
//		
//		
//		return results;
//		
//	}
	

//	public Integer scanMinimum(String tableName, String keyQualifier, String valueQualifier, String key){
//		
//
////		System.out.println("scanMinimum");
////		System.out.println("tableName:"+tableName);
////		System.out.println("keyQualifier:"+keyQualifier);
////		System.out.println("valueQualifier:"+valueQualifier);
////		System.out.println("key:"+key);
////		
//		Scan scan1 = new Scan();
//		scan1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(keyQualifier));
//		scan1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(valueQualifier));
//		ResultScanner scanner1=null;
//		try {
//			scanner1 = getTable(tableName).getScanner(scan1);
//		} catch (IOException e) {
//	
//			e.printStackTrace();
//		}
//		Integer smallestValue = null;
//		for (Result res : scanner1) {
//			
//			KeyValue keyValueKey = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(keyQualifier));
//			
//			if(Bytes.toString(keyValueKey.getValue()).equals(key)){
//				
//				KeyValue keyValueValue = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(valueQualifier));
//				Integer value = Integer.parseInt(Bytes.toString(keyValueValue.getValue()));
//				if(smallestValue == null || value < smallestValue){
//					smallestValue = value;
//				}
//				System.out.println("smallestValue:"+smallestValue);
//			}
//
//		}
//		scanner1.close();
//		
//		
//		return smallestValue;
//		
//	}
	
//	public List<byte[]> getFromIndexTable(String viewTableName, String indexKey){
//		
//		
//		Map<byte[], byte[]> map = get(Bytes.toBytes(viewTableName), Bytes.toBytes(indexKey), new ArrayList<byte[]>());
//
//		
//		List<byte[]> keyList = new ArrayList<byte[]>();
//		for (byte[] key : BytesUtil.eliminateSignatures(map).keySet()) {
//		
//			keyList.add(key);
//
//			
//		}
//		return keyList;
//		
//	}

	
//	public Integer indexMinimum(String indexTable, String baseTable, String key){
//		
//		
//		Map<byte[], byte[]> map = get(Bytes.toBytes(indexTable), Bytes.toBytes(key), new ArrayList<byte[]>());
//		
//		System.out.println(BytesUtil.mapToString(BytesUtil.eliminateSignatures(map)));
//		
//		long startTime = new Date().getTime();
//		for (byte[] minKey : BytesUtil.eliminateSignatures(map).keySet()) {
//		
//			Map<byte[], byte[]> minKeys = get(Bytes.toBytes(baseTable), minKey, new ArrayList<byte[]>());
//			System.out.println(BytesUtil.mapToString(BytesUtil.eliminateSignatures(minKeys)));
//		}
//	
//		long endTime = new Date().getTime();
//		System.out.println("Time to get results: "+(endTime - startTime));
//		
//	}
	
	
	
//	public Integer scanMaximum(String tableName, String keyQualifier, String valueQualifier, String key){
//		
//
//		
//		Scan scan1 = new Scan();
//		scan1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(keyQualifier));
//		scan1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(valueQualifier));
//		ResultScanner scanner1=null;
//		try {
//			scanner1 = getTable(tableName).getScanner(scan1);
//		} catch (IOException e) {
//	
//			e.printStackTrace();
//		}
//		Integer biggestValue = null;
//		for (Result res : scanner1) {
//			
//			KeyValue keyValueKey = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(keyQualifier));
//			
//			if(Bytes.toString(keyValueKey.getValue()).equals(key)){
//				
//				KeyValue keyValueValue = res.getColumnLatest(Bytes.toBytes("colfam1"), Bytes.toBytes(valueQualifier));
//				Integer value = Integer.parseInt(Bytes.toString(keyValueValue.getValue()));
//				if(biggestValue == null || value > biggestValue){
//					biggestValue = value;
//				}
//			}
//
//		}
//		scanner1.close();
//		
//		
//		return biggestValue;
//		
//	}	
	
	




	
	private Result result=null;



	public Result getResult() {
		return result;
	}




	public void setResult(Result result) {
		this.result = result;
	}





	
	

}
