package de.tok.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import de.tok.coprocessor.generated.BSVCoprocessorProtos;
import net.sf.jsqlparser.statement.select.Join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

public class SelectElement extends LogicalElement implements Runnable {
	private String tableName = null;
	private List<BSVCoprocessorProtos.BSVColumn> columns = new ArrayList<BSVCoprocessorProtos.BSVColumn>();
	private List<BSVCoprocessorProtos.Condition> conditions = new ArrayList<BSVCoprocessorProtos.Condition>();
	private List<ByteString> aggregations = new ArrayList<ByteString>();
	private String aggregationKey = "";
	private String joinKey = "";
	private String joinTable = "";
	private Join join = null;
	private boolean isMaterialize = false;
	private boolean isReturningResults = false;
	private String SQL = "";
	private String viewName = "";
	private boolean isBuildJoinView = false;
	/**
	 * The following fields are for separating block and non-block operations
	 */
	private boolean nonBlock = false;
	private int waitForBlock = 0;
	private int finishBlock = 0;
	/**
	 * End
	 */


	@Override
	public void execute() {
		// depends on whether it is a non-blocking logical element
		// if non-blocking new a thread to execute
		// if blocking then execute directly unless the non-blocking element is not yet finish
		// in this case a loop for checking should be create
		// check until the non-blocking element has already finish
		// then it is safe to run
		if(nonBlock){
			Thread thread = new Thread(this);
			thread.start();
		}else{
			while(finishBlock != waitForBlock){
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// blocking execution
			System.out.println("Run blocking execution");
			run();
		}
		
		if(next != null){
			next.execute();
		}
	}
	

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<BSVCoprocessorProtos.BSVColumn> getParameters() {
		return columns;
	}

	public void setParameters(List<BSVCoprocessorProtos.BSVColumn> columns) {
		this.columns = columns;
	}

	public List<BSVCoprocessorProtos.BSVColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<BSVCoprocessorProtos.BSVColumn> columns) {
		this.columns = columns;
	}

	public List<BSVCoprocessorProtos.Condition> getConditions() {
		return conditions;
	}

	public void setConditions(List<BSVCoprocessorProtos.Condition> conditions) {
		this.conditions = conditions;
	}

	public List<ByteString> getAggregations() {
		return aggregations;
	}

	public void setAggregations(List<ByteString> aggregations) {
		this.aggregations = aggregations;
	}
	
	public String getAggregationKey() {
		return aggregationKey;
	}

	public void setAggregationKey(String aggregationKey) {
		this.aggregationKey = aggregationKey;
	}

	public String getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(String joinKey) {
		this.joinKey = joinKey;
	}

	public String getJoinTable() {
		return joinTable;
	}

	public void setJoinTable(String joinTable) {
		this.joinTable = joinTable;
	}

	public Join getJoin() {
		return join;
	}

	public void setJoin(Join join) {
		this.join = join;
	}

	public boolean isMaterialize() {
		return isMaterialize;
	}

	public void setMaterialize(boolean isMaterialize) {
		this.isMaterialize = isMaterialize;
	}

	public boolean isNonBlock() {
		return nonBlock;
	}

	public void setNonBlock(boolean nonBlock) {
		this.nonBlock = nonBlock;
	}

	public int getWaitForBlock() {
		return waitForBlock;
	}

	public void setWaitForBlock(int waitForBlock) {
		this.waitForBlock = waitForBlock;
	}

	public int getFinishBlock() {
		return finishBlock;
	}

	public void setFinishBlock(int finishBlock) {
		this.finishBlock = finishBlock;
	}

	public boolean isReturningResults() {
		return isReturningResults;
	}

	public void setReturningResults(boolean isReturningResults) {
		this.isReturningResults = isReturningResults;
	}

	public String getSQL() {
		return SQL;
	}

	public void setSQL(String sQL) {
		SQL = sQL;
	}

	public boolean isBuildJoinView() {
		return isBuildJoinView;
	}

	public void setBuildJoinView(boolean isBuildJoinView) {
		this.isBuildJoinView = isBuildJoinView;
	}

	@Override
	public String toString() {
		return "SelectElement ["
				+ "tableName=" + tableName 
				+ ", columns=" + columns 
				+ ", aggregations=" + aggregations 
				+ ", conditions=" + conditions 
				+ ", aggregationKey=" + aggregationKey 
				+ ", joinKey=" + joinKey 
				+ ", joinTable=" + joinTable 
				+ ", isReturningResults=" + isReturningResults 
				+ ", isBuildingJoinView=" + isBuildJoinView + "]";
	}
	
	/**
	 * Construct the SQL using the existing fields
	 * @return
	 */
	public String constructSQLByField(){
		String result = "";
		result += "select ";
		for(int i = 0; i < columns.size(); i++){
			if(i != 0 && i != (columns.size() - 1)){
				result += ", ";
			}
			result += columns.get(i).getFamily().toStringUtf8() + ".";
			result += columns.get(i).getColumn().toStringUtf8();
		}
		result += " from " + tableName;
		if(!conditions.isEmpty()){
			result += " where ";
			for(int i = 0; i < conditions.size(); i++){
				if(i != 0 && i != (conditions.size() - 1)){
					result += " ";
				}
				result += conditions.get(i).getColumn().getFamily().toStringUtf8() + ".";
				result += conditions.get(i).getColumn().getColumn().toStringUtf8();
				result += conditions.get(i).getOperator().toStringUtf8();
				result += conditions.get(i).getValue().toStringUtf8();
			}
		}
		return result;
	}

	@Override
	public void run() {
		Configuration conf = HBaseConfiguration.create();
		HTable table;
		try {
			table = new HTable(conf, this.tableName);
			BSVCoprocessorProtos.ParameterMessage.Builder request = BSVCoprocessorProtos.ParameterMessage.newBuilder();
			
			// add columns
			for(BSVCoprocessorProtos.BSVColumn bsvColumn:this.columns){
				request.addColumn(bsvColumn);
			}
			
			// add conditions
			for(BSVCoprocessorProtos.Condition condition:conditions){
				request.addCondition(condition);
			}
			
			// add aggregation
			for(ByteString aggregation:aggregations){
				request.addAggregation(aggregation);
			}
			
			// set aggregation key
			request.setAggregationKey(ByteString.copyFrom(aggregationKey.getBytes()));
			
			// add join key 
			if(!joinKey.trim().equals("")){
				request.setJoinKey(ByteString.copyFrom(joinKey.getBytes()));	
			}
			
			// add join table
			if(!joinTable.trim().equals("")){
				request.setJoinTable(ByteString.copyFrom(joinTable.getBytes()));
			}
			
			// set if the coprocessor should return the results
			request.setIsReturningResults(isReturningResults);
			
			// set materialize
			request.setIsMaterialize(isMaterialize);
			
			// set SQL
			request.setSQL(ByteString.copyFrom(SQL.getBytes()));
			
			// set viewName
			request.setViewName(ByteString.copyFrom(viewName.getBytes()));
			
			// set is-build-join-view
			request.setIsBuildJoinView(isBuildJoinView);
			 
			System.out.println("=======================================================================");
			Date begin = new Date();
			System.out.println(begin + " Begining to execute batch job");
			Map<byte[], BSVCoprocessorProtos.ResultMessage> results = table.batchCoprocessorService(
					BSVCoprocessorProtos.Execute.getDescriptor().findMethodByName("batch"),
					request.build(), HConstants.EMPTY_START_ROW,
					HConstants.EMPTY_END_ROW,
					BSVCoprocessorProtos.ResultMessage.getDefaultInstance());
			Date end = new Date();
			System.out.println(end + " Finish batch job in " + (end.getTime() - begin.getTime()) + " miliseconds");
			
			long total = 0;
			for (Map.Entry<byte[], BSVCoprocessorProtos.ResultMessage> entry : results.entrySet()) {
				BSVCoprocessorProtos.ResultMessage response = entry.getValue();
				total += response.getSize();
				System.out.println("Region: " + Bytes.toString(entry.getKey())
						+ ", Count: " + response.getSize());
				System.out.println(response.toString());
			}
			System.out.println("Total Count: " + total);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ServiceException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		
		// if this is non-blocking element
		// add one finish flag to the first occurring blocking element
		SelectElement element = (SelectElement) this.next;
		while(element != null && element.isNonBlock()){
			element = (SelectElement) element.next;
		}
		if(element != null){
			element.setFinishBlock(element.getFinishBlock() + 1);
		}
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}
}
