package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class MatchingRow {


	private byte[] tableName;
	
	private byte[] primaryKey;
	
	private Map<byte[], byte[]> columns;

	
	public MatchingRow(String tableName, String primaryKey, Map<String, String> columns) {
		super();
		this.tableName = Bytes.toBytes(tableName);
		this.primaryKey = Bytes.toBytes(primaryKey);
		this.columns = BytesUtil.convertMap(columns);
	}
	
	
	public static Map<byte[], byte[]> getColumns(List<MatchingRow> matchingRows){				
		
		Map<byte[], byte[]> result = new HashMap<byte[], byte[]>();
		
		for (MatchingRow matchingRow : matchingRows) {
			result.putAll(matchingRow.getColumns());
			System.out.println(BytesUtil.mapToString(matchingRow.getColumns()));
			
//			Bytes.
		}
		return result;
	
}
	

	/**
	 * Translate results to a list of MatchingRow objects
	 * @param results
	 * @return
	 */
	public static List<MatchingRow> transformResults(String tableName, List<Result> results) {
		
//		System.out.println("results:"+results);
		
		List<MatchingRow> matchingRows = new ArrayList<MatchingRow>();
		
		if(results.size() > 0){
			
			for (Result result : results) {
				
				
				MatchingRow matchingRow = new MatchingRow(Bytes.toBytes(tableName),  result.getRow(), result.getFamilyMap(Bytes.toBytes("colfam1")));
				
				matchingRows.add(matchingRow);
				
			}
			
		}
		
		return matchingRows;
	}
	
	
	
	public MatchingRow copy(){
		
		Map<byte[], byte[]> copyMap = new HashMap<byte[], byte[]>();
		
		for (byte[] key : this.columns.keySet()) {
			copyMap.put(key, this.columns.get(key));
		}
		
		
		MatchingRow matchingRow = new MatchingRow(this.tableName, this.primaryKey, copyMap);
		
		return matchingRow;
		
	}
	
	
	public MatchingRow(byte[] tableName, byte[] primaryKey, Map<byte[], byte[]> columns) {
		super();
		this.tableName = tableName;
		this.primaryKey = primaryKey;
		this.columns = columns;
	}


	public byte[] getTableName() {
		return tableName;
	}
	
	public String getTableNameString() {
		return Bytes.toString(tableName);
	}

	public void setTableName(byte[] tableName) {
		this.tableName = tableName;
	}


	public byte[] getPrimaryKey() {
		return primaryKey;
	}
	
	public String getPrimaryKeyString() {
		return Bytes.toString(primaryKey);
	}

	public void setPrimaryKey(byte[] primaryKey) {
		this.primaryKey = primaryKey;
	}


	public Map<byte[], byte[]> getColumns() {
		return columns;
	}


	public void setColumns(Map<byte[], byte[]> columns) {
		this.columns = columns;
	}


	@Override
	public String toString() {
		return "MatchingRow [tableName=" + Bytes.toString(tableName)
				+ ", primaryKey=" + Bytes.toString(primaryKey) + ", columns="
				+ BytesUtil.convertMapBack(columns) + "]";
	}
	


	
	
}
