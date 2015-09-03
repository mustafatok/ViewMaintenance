package com.lin.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVColumn;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Condition;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Execute;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ParameterMessage;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ResultMessage;

public class LogicalElement {
	private LogicalElement next = null;
	private String tableName = null;
	private List<BSVColumn> columns = new ArrayList<BSVColumn>();
	private List<Condition> conditions = new ArrayList<Condition>();
	private List<ByteString> aggregations = new ArrayList<ByteString>();
	private String aggregationKey = "";
	private String joinKey = "";
	private String joinTable = "";

	public LogicalElement getNext() {
		return next;
	}

	public void execute() {
		Configuration conf = HBaseConfiguration.create();
		HTable table;
		try {
			table = new HTable(conf, this.tableName);
			ParameterMessage.Builder request = ParameterMessage.newBuilder();
			
			// add columns
			for(BSVColumn bsvColumn:this.columns){
				request.addColumn(bsvColumn);
			}
			
			// add conditions
			for(Condition condition:conditions){
				request.addCondition(condition);
			}
			
			// add aggregation
			for(ByteString aggregation:aggregations){
				request.addAggregation(aggregation);
			}
			
			// add join key and join table
			if(!joinKey.trim().equals("")){
				request.setJoinKey(ByteString.copyFrom(joinKey.getBytes()));
				request.setJoinTable(ByteString.copyFrom(joinTable.getBytes()));
			}
			 
			System.out.println("=======================================================================");
			Date begin = new Date();
			System.out.println(begin + " Beging to execute batch job");
			Map<byte[], ResultMessage> results = table.batchCoprocessorService(
					Execute.getDescriptor().findMethodByName("batch"),
					request.build(), HConstants.EMPTY_START_ROW,
					HConstants.EMPTY_END_ROW,
					ResultMessage.getDefaultInstance());
			Date end = new Date();
			System.out.println(end + " Finish batch job in " + (end.getTime() - begin.getTime()) + " miliseconds");
			long total = 0;
			for (Map.Entry<byte[], ResultMessage> entry : results
					.entrySet()) {
				ResultMessage response = entry.getValue();
				total += response.getSize();
				System.out.println("Region: " + Bytes.toString(entry.getKey())
						+ ", Count: " + response.getSize());
				System.out.println(response.toString());
			}
			System.out.println("Total Count: " + total);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setNext(LogicalElement next) {
		this.next = next;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<BSVColumn> getParameters() {
		return columns;
	}

	public void setParameters(List<BSVColumn> columns) {
		this.columns = columns;
	}

	public List<BSVColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<BSVColumn> columns) {
		this.columns = columns;
	}

	public List<Condition> getConditions() {
		return conditions;
	}

	public void setConditions(List<Condition> conditions) {
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

	@Override
	public String toString() {
		return "LogicalElement [tableName=" + tableName + ", columns="
				+ columns + ", aggregations=" + aggregations + " conditions=" + conditions 
				+ "aggregationKey=" + aggregationKey 
				+ "joinKey=" + joinKey 
				+ "joinTable=" + joinTable + "]";
	}


}
