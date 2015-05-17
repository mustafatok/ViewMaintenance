package de.webdataplatform.view;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.BytesUtil;
import de.webdataplatform.settings.CreateReverseJoinView;
import de.webdataplatform.settings.CreateJoinView;
import de.webdataplatform.settings.JoinNode;
import de.webdataplatform.settings.JoinTable;
import de.webdataplatform.settings.JoinTablePair;
import de.webdataplatform.settings.MatchingRow;
import de.webdataplatform.storage.BaseTableUpdate;

public class Join {


//	private TableService tableService;
//	
//	private Log log;
//	
//	private CreateReverseJoinView cJV;
//	 
//	public Join(CreateReverseJoinView cJV, TableService tableService, Log log) {
//		super();
//		this.cJV = cJV;
//		this.tableService = tableService;
//		this.log = log;
//	}
//
//	
//	public String getPrimaryKey(List<MatchingRow> matchingRows){				
//		
//	
//		
//		List<String> tableOrder = getJoinTableOrder();
//		List<String> compositeKey = new ArrayList<String>();
//		
//		for (String tableName : tableOrder) {
//			
//			for (int i = 0; i < matchingRows.size(); i++) {
//				
//				if(tableName.equals(Bytes.toString(matchingRows.get(i).getTableName()))){
//					
//					compositeKey.add(matchingRows.get(i).getPrimaryKeyString());
//
//				}
//			}
//		}
//		return keyToString(compositeKey);
//	
//    }
//	
//	public Map<byte[],byte[]> getColumns(List<MatchingRow> matchingRows){				
//		
//		
//		Map<byte[],byte[]> newViewRecord = new HashMap<byte[], byte[]>();
//		
//		List<String> tableOrder = getJoinTableOrder();
//		
//		for (String tableName : tableOrder) {
//			
//			for (int i = 0; i < matchingRows.size(); i++) {
//				
//				if(tableName.equals(Bytes.toString(matchingRows.get(i).getTableName()))){
//					
//					
//					for(byte[] key : matchingRows.get(i).getColumns().keySet()){
//						
//						
//						if(BytesUtil.mapContains(key, newViewRecord)){
//							String newKey = Bytes.toString(key)+"_1";
//							newViewRecord.put(Bytes.toBytes(newKey), matchingRows.get(i).getColumns().get(key));
//						}else{
//							newViewRecord.put(key, matchingRows.get(i).getColumns().get(key));
//						}
//					}
//					
//				}
//			}
//		}
//		return newViewRecord;
//	
//    }
//	
//	public String keyToString(List<String> compositeKey){
//	
//		String result="";
//		
//		for (int i = 0; i < compositeKey.size(); i++) {
//			result += compositeKey.get(i);
//			if(i != compositeKey.size()-1)result+="_";
//		}
//		return result;
//		
//	}
//	
//	public List<String> getJoinTableOrder(){
//		 
//		List<JoinTablePair> jTPs = cJV.getJoinPairs();
//		List<String> result = new ArrayList<String>();
//		
//		for (JoinTablePair joinTablePair : jTPs) {
//			if(!result.contains(joinTablePair.getLeftTable()))result.add(joinTablePair.getLeftTable().getTableName());
//			if(!result.contains(joinTablePair.getRightTable()))result.add(joinTablePair.getRightTable().getTableName());
//		}
//		return result;
//	 }
//	
//	/**
//	 * 
//	 * Given the updatedTable and the view definition of the join, this procedure computes the correct
//	 * order in which the tables have to be joined. Start point is the updated base table.
//	 * @param updatedTable
//	 * @param viewDefinition
//	 * @return
//	 */
//	public List<JoinTablePair> getJoinComputingOrder(String updatedTable){
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
//			List<JoinTablePair> jTPsSelectedTable = JoinTablePair.getJoinTables(cJV.getJoinPairs(), selectedTable);
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
//	/**
//	 * Compute join records of a base table update, that has been applied to a certain table. Computing
//	 * the join record includes resolving the join definition and computing the matching rows
//	 * @param btu
//	 */
//	public JoinNode computeJoinRecords(BaseTableUpdate btu){
//		
//		
//
//		
//		List<JoinTablePair> joinTablePairs = getJoinComputingOrder(btu.getBaseTable());
//		
//		
//		JoinNode rootNode = new JoinNode(new MatchingRow(btu.getBaseTable(), btu.getKey(), btu.getColumns()), new ArrayList<JoinNode>());
//		List<JoinNode> selectedNodes = new ArrayList<JoinNode>();
//		selectedNodes.add(rootNode);
//		
//		for (JoinTablePair joinTablePair : joinTablePairs) {
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
//		return queryRightTable(joinKey, rightTable);
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
//
//	/**
//	 * Query right table to determine the matching rows in the a given relation
//	 * @param rightTable
//	 * @param joinKey
//	 * @return
//	 */
//	
//	private List<MatchingRow> queryRightTable(String joinKey, JoinTable rightTable) {
//		
//		
////		System.out.println("query right table: "+rightTable+" with key: "+joinKey);
//		List<MatchingRow> matchingRows= new ArrayList<MatchingRow>();
////		List<MatchingRow> matchingRowsIndex = new ArrayList<MatchingRow>();
//		List<Result> results = new ArrayList<Result>();
//		try {
//			
//			if(rightTable.getJoinKey().equals("primaryKey")){
////				System.out.println("joinKey: "+joinKey);
//				long startTime = new Date().getTime();
//				
////				log.updates(this.getClass(), "primKey");
//				Result result = tableService.get(rightTable.getTableName(), joinKey);
//				
//				
//				if(!result.isEmpty()){
//
//					
//					results.add(result);
//				}
//				matchingRows = MatchingRow.transformResults(rightTable.getTableName(), results);	
////				log.updates(this.getClass(), "time to calculate primkey: "+(new Date().getTime()-startTime));
//				
//			}else{
//				long startTime = new Date().getTime();
//				
//				/*
//				if(!cJV.isUseIndex()){
////					log.updates(this.getClass(), "secKey mit scan");
//					
//					results = tableService.scanValue(rightTable.getTableName(), rightTable.getJoinKey(), joinKey);
//					matchingRows = MatchingRow.transformResults(rightTable.getTableName(), results);
//				}else{
//				
////					log.updates(this.getClass(), "secKey mit index");
////					matchingRowsIndex = new ArrayList<MatchingRow>();
//					
////					log.updates(this.getClass(), "indexTable: "+cJV.getName()+"_"+rightTable.getTableName()+"_index");
//					log.performance(this.getClass(), "indexKey: "+joinKey);
//					
//					List<byte[]> indexResult = tableService.getFromIndexTable(cJV.getName()+"_"+rightTable.getTableName()+"_index", joinKey);
//					
//					log.performance(this.getClass(), "indexResult: "+BytesUtil.listToString(indexResult));
//					
//					for (byte[] key : indexResult) {
//						
//						Map<byte[], byte[]> minKeys = tableService.get(Bytes.toBytes(rightTable.getTableName()), key, new ArrayList<byte[]>());
//						log.performance(this.getClass(), "indexResultQuery: "+BytesUtil.mapToString(minKeys));
//						if(minKeys != null && minKeys.keySet().size() > 0){
//							MatchingRow matchingRow = new MatchingRow(Bytes.toBytes(rightTable.getTableName()),  key, minKeys);
//							matchingRows.add(matchingRow);
//						}
//					}
//				}
//				*/
////				log.updates(this.getClass(), "time to calculate index: "+(new Date().getTime()-startTime));
//				
//			}
//			
//			
////			log.updates(this.getClass(), "matchingRows: "+matchingRows);
//			
//		}catch(Exception e){
//			log.error(this.getClass(), e);
//		}
//		return matchingRows;
//	}

	

}
