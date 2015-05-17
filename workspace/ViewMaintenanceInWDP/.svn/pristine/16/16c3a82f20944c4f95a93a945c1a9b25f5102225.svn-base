package de.webdataplatform.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.view.OperationMode;
import de.webdataplatform.view.ViewMode;


public class BaseTableUpdate implements Serializable, Writable{


	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9013050318641185187L;

	private Log log;
	

	public BaseTableUpdate(){
		
		
	}
	
	
	public Map<String, String> readMapFromString(String inputString){

		
		Map<String, String> result = new HashMap<String, String>();
		
		
//		Log.info(this.getClass(), "columnsString: "+columnsString);

		if(!inputString.equals(" ") && !inputString.equals("") && !inputString.equals("=")){
		
			String[] columnsKVs = inputString.split(SystemConfig.MESSAGES_SPLITIDSEQUENCE);
			
			
			for (int i = 0; i < columnsKVs.length; i++) {
				
				String columnKV = columnsKVs[i];
				String[] kv = columnKV.split("=");
				
				if(kv.length > 1)
					result.put(kv[0], kv[1]);
				if(kv.length == 1)
					result.put(kv[0], "");
//				else columns.put(kv[0], "");
				
			}
		}
		return result;
		
	}
	
	
	
	public BaseTableUpdate(Log log, String updateString){
		
		this.log = log;
		
		try{
			
		
		String[] updateParts = updateString.split(SystemConfig.MESSAGES_SPLITSEQUENCE);
		
		baseTable = updateParts[0].trim();
		
		regionServer = updateParts[1].trim();
		
		region = updateParts[2].trim();
		
		seqNo = updateParts[3].trim();
		
		timestamp = updateParts[4].trim();
		
		key = updateParts[5].trim();

		type = updateParts[6].trim();
		
		columns = readMapFromString(updateParts[7].trim());
		
		oldColumns = readMapFromString(updateParts[8].trim());
		
		colFamilies = removeTags(readMapFromString(updateParts[9].trim()));
		
		
		List<String> keysToRemove = new ArrayList<String>();
		Map<String, String> keysToAdd = new HashMap<String, String>();
		
		for (String key : columns.keySet()) {
			
			if(key.contains("_new")){
				keysToAdd.put(key.replace("_new", ""),columns.get(key));
				keysToRemove.add(key);
			}
			if(key.contains("_old")){
				oldColumns.put(key.replace("_old", ""),columns.get(key));
				keysToRemove.add(key);
			}
			
		}
		
		columns.putAll(keysToAdd);
		
		for (String string : keysToRemove) {
			columns.remove(string);
		}
		

		
		}catch(Exception e){
			
			log.info(this.getClass(), "update failed: "+updateString);
			log.error(this.getClass(), e);
			
		}

//		aggKey = updateParts[4].trim();
		
//		value = updateParts[5].trim();
		

		
	}

	public Map<String, String> removeTags(Map<String, String> map){
		
		
		Map<String, String> result  = new HashMap<String, String>();
		
		
		for (String key : map.keySet()) {
			result.put(key.replace("_new", "").replace("_old", ""), map.get(key));
		}
		
		return result;
		
	}


	public BaseTableUpdate(String baseTable, String regionServer, String region, String seqNo, String timestamp, String key, String type, Map<String, String> columns,  Map<String, String> oldColumns, Map<String, String> colFamilies) {
		super();
		this.baseTable = baseTable;
		this.regionServer = regionServer;
		this.seqNo = seqNo;
		this.key = key;
		this.region = region;
//		this.aggKey = aggKey;
		this.columns = columns;
		this.oldColumns = oldColumns;
		this.colFamilies = colFamilies;
//		this.value = value;
		this.type = type;
		this.timestamp = timestamp;
	}

	private String baseTable;
	
	private String regionServer;
	
	private String region;
	
	private String seqNo;

	private String timestamp;

	private String key;
	
	private String type;
	
	private String viewTable;
	
	private String viewDefinition;
	
	private Map<String, String> columns;
	
	private Map<String, String> oldColumns;
	
	private Map<String, String> colFamilies;
	
//	private String aggKey;
	
//	private String value;
	
	

	public String convertMapToString(Map<String, String> map){

		String result = "";
		
		if(map != null && !map.isEmpty() && !map.keySet().isEmpty()){
			for (String key : map.keySet()) {
				
				result += key+"="+map.get(key)+SystemConfig.MESSAGES_SPLITIDSEQUENCE;
				
			}
			result = result.substring(0, result.length()-1);
		}else{
			result = " ";
		}
		
		return result;
		
	}

	public String convertToString(){
		
		
		
		return SystemConfig.MESSAGES_STARTSEQUENCE+baseTable+SystemConfig.MESSAGES_SPLITSEQUENCE+regionServer+SystemConfig.MESSAGES_SPLITSEQUENCE+region+SystemConfig.MESSAGES_SPLITSEQUENCE+seqNo+SystemConfig.MESSAGES_SPLITSEQUENCE+timestamp+SystemConfig.MESSAGES_SPLITSEQUENCE+key+SystemConfig.MESSAGES_SPLITSEQUENCE+type+SystemConfig.MESSAGES_SPLITSEQUENCE+convertMapToString(columns)+SystemConfig.MESSAGES_SPLITSEQUENCE+convertMapToString(oldColumns)+SystemConfig.MESSAGES_SPLITSEQUENCE+convertMapToString(colFamilies)+SystemConfig.MESSAGES_ENDSEQUENCE;
	}

	public ViewMode getViewMode(){
		
		ViewMode viewMode = ViewMode.valueOf(this.getViewDefinition().split(",")[0]);
		
		return viewMode;
	}
	
	
	public Map<String, String> getColumnsWithoutSignatures(){
		
		Map<String, String> result = new HashMap<String, String>();
		
		for (String columnKey : columns.keySet()) {
			
			if(colFamilies.get(columnKey) != null && colFamilies.get(columnKey).equals("sigfam1")){
				continue;
			}
			result.put(columnKey, columns.get(columnKey));
			
		}
		return result;
	}
	
	public Map<String, String> getOldColumnsWithoutSignatures(){
		
		Map<String, String> result = new HashMap<String, String>();
		
		for (String columnKey : oldColumns.keySet()) {
			
			if(colFamilies.get(columnKey) != null && colFamilies.get(columnKey).equals("sigfam1")){
				continue;
			}
			result.put(columnKey, oldColumns.get(columnKey));
			
		}
		return result;
	}
	
	
//	public String getColumnFamily(){
//		
//		
//		for (String columnKey : columns.keySet()) {
//			
//			if(colFamilies.get(columnKey) != null && !colFamilies.get(columnKey).equals("sigfam1")){
//				return colFamilies.get(columnKey);
//			}
//
//			
//		}
//		return null;
//	}
	
	
//	public PropagationMode getPropagationMode(){
//		
//
//		return propagationMode;
//	}
//	
//	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}





	public String getBaseTable() {
		return baseTable;
	}




	public void setBaseTable(String baseTable) {
		this.baseTable = baseTable;
	}




	public String getViewTable() {
		return viewTable;
	}




	public void setViewTable(String viewTable) {
		this.viewTable = viewTable;
	}
	
	

	public String getViewDefinition() {
		return viewDefinition;
	}

	public void setViewDefinition(String viewDefinition) {
		this.viewDefinition = viewDefinition;
	}
	
	

	public String getRegionServer() {
		return regionServer;
	}

	public void setRegionServer(String regionServer) {
		this.regionServer = regionServer;
	}

	public String getSeqNo() {
		return seqNo;
	}

	public void setSeqNo(String seqNo) {
		this.seqNo = seqNo;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	



	public BaseTableUpdate copy(){
		
		BaseTableUpdate btu = new BaseTableUpdate();
		
		btu.baseTable = this.baseTable;
		btu.regionServer = this.regionServer;
		btu.region = this.region;
		btu.seqNo = this.seqNo;
		btu.key = this.key;
		btu.columns = this.columns;
		btu.oldColumns = this.oldColumns;
		btu.colFamilies = this.colFamilies;
		btu.type = this.type;
		btu.viewTable = this.viewTable;
		btu.viewDefinition = this.viewDefinition;
		btu.timestamp = this.timestamp;
		
		return btu;
		
	}


	
	

	@Override
	public String toString() {
		return "BaseTableUpdate [log=" + log + ", baseTable=" + baseTable
				+ ", regionServer=" + regionServer + ", region=" + region
				+ ", seqNo=" + seqNo + ", timestamp=" + timestamp + ", key="
				+ key + ", type=" + type + ", viewTable=" + viewTable
				+ ", viewDefinition=" + viewDefinition + ", columns=" + columns
				+ ", oldColumns=" + oldColumns + ", colFamilies=" + colFamilies
				+ "]";
	}
	
	

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public Map<String, String> getColumns() {
		return columns;
	}

	public void setColumns(Map<String, String> columns) {
		this.columns = columns;
	}

	public Map<String, String> getOldColumns() {
		return oldColumns;
	}

	public void setOldColumns(Map<String, String> oldColumns) {
		this.oldColumns = oldColumns;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, String> getColFamilies() {
		return colFamilies;
	}

	public void setColFamilies(Map<String, String> colFamilies) {
		this.colFamilies = colFamilies;
	}
	
	
	
	
}
