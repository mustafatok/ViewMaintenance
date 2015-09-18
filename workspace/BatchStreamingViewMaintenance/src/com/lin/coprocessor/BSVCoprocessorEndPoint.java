package com.lin.coprocessor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVColumn;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVRow;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Condition;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Execute;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.KeyValue;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ParameterMessage;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ResultMessage;
import com.lin.test.HBaseHelper;
import com.lin.utils.Common;

public class BSVCoprocessorEndPoint extends Execute implements Coprocessor,
		CoprocessorService{
	private RegionCoprocessorEnvironment env;
	private AggregationManager aggregationManager = null;
	private MaterializeManager materialize = null;
	private String joinFamily = null;
	private String joinQualifier = null;
	private HTable joinTable = null;
	private ParameterMessage request = null;

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		this.env = (RegionCoprocessorEnvironment) env;
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {

	}

	@Override
	public void batch(RpcController controller, ParameterMessage request,
			RpcCallback<ResultMessage> done) {
		System.out.println("============================================================");
		System.out.println((new Date())+"Begin to execute");
		
		// store in local variable for later use
		this.request = request;
		
		// initialize materialize manager
		materialize = new MaterializeManager(request);
		
		// initialize aggregation manager
		aggregationManager = new AggregationManager(request);
		
		// initialize scan
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		
		// check if join
		// If join, set is-materialize to be true.
		// Fill joinKey and joinTable for later use
		if(!request.getJoinKey().toStringUtf8().equals("")){
			// Connect to the join table using the join table name from request
			Configuration configuration = HBaseConfiguration.create();;
			try {
				joinTable = new HTable(configuration, request.getJoinTable().toByteArray());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// fill join family and join qualifier for later use
			joinFamily = request.getJoinKey().toStringUtf8().split("\\.")[0];
			joinQualifier = request.getJoinKey().toStringUtf8().split("\\.")[1];
			
			// add joinkey to the scan filter
			scan.addColumn(joinFamily.getBytes(), joinQualifier.getBytes());
		}
		
		// add column as filter
		// add all columns in the "column" field 
		for(BSVColumn bsvColumn:request.getColumnList()){
			System.out.println((new Date())+"Begin to add column: "+bsvColumn);
			// get parameters
			byte[] family = bsvColumn.getFamily().toByteArray();
			byte[] column = bsvColumn.getColumn().toByteArray();
			
			scan.addColumn(family, column);			
			System.out.println((new Date())+"Finish adding column: "+bsvColumn);
		}
		
		// also add columns in the "condition" field
		for(Condition condition:request.getConditionList()){
			System.out.println(new Date() + "Begin to add condition column: " + condition);
			// get family and qualifier
			byte[] family = condition.getColumn().getFamily().toByteArray();
			byte[] column = condition.getColumn().getColumn().toByteArray();
			
			// add column filter to scan
			scan.addColumn(family, column);
		}

		// response builder
		ResultMessage.Builder response = ResultMessage.newBuilder();

		// use an internal scanner to perform scanning.
		InternalScanner scanner = null;
		try {
			scanner = env.getRegion().getScanner(scan);

			// scan a row of results every time and add this row to 
			// the result arraylist
			List<Cell> curVals = new ArrayList<Cell>();
			boolean finish = false;
			long count = 0;
			Date begin = new Date();
			System.out.println(begin+"Begin to scan");
			do {
				curVals.clear();
				finish = scanner.next(curVals);
				List<Cell> row = new ArrayList<Cell>(curVals);
				
				// find the aggregation key first
				String aggKey = "";
				for(Cell cell:row){
					String columnName = new String(CellUtil.cloneFamily(cell)) + "." + new String(CellUtil.cloneQualifier(cell));
					System.out.println("Finding the aggregation key of the row: " + columnName + " and " + request.getAggregationKey().toStringUtf8());
					if(columnName.equals(request.getAggregationKey().toStringUtf8())){
						aggKey = new String(CellUtil.cloneValue(cell));
						System.out.println("Found aggregation key: " + aggKey);
						break;
					}
				}
				
				/*
				 * start of handling result
				 */
				BSVRow.Builder bsvRow = BSVRow.newBuilder();
				System.out.println((new Date())+"Building row no." + count);
				
				// If is-materialize flag is true build the delta view for every row
				if(request.getIsMaterialize()){
					materialize.putToDeltaView(row);
				}
				
				boolean meetCondition = handleRow(request, row, bsvRow, aggKey);
				
				// discard the whole row if the cell fails to meet the condition
				if(meetCondition){
					response.addRow(bsvRow);
					count++;
					
					// build the select view
					if(request.getIsMaterialize()){
						// selection view
						if(request.getAggregationKey().toStringUtf8().equals("")){
							materialize.putToSelectView(row);
						}
					}
					
					// if is one of the join table put it to reverse join table
					if(!request.getJoinKey().toStringUtf8().equals("")){
						putToReverseJoinTable(row);
					}
				}
				/*
				 * End of handling result
				 */
			} while (finish);
			Date end = new Date();
			System.out.println(end+" Finish scanning in " + (end.getTime()-begin.getTime()) + " million seconds");
			
			System.out.println((new Date())+"Finish building response message");
			response.setSize(count);
			
			// add aggregation result to response
			// put all the aggregation in one row
			BSVRow.Builder bsvRow = BSVRow.newBuilder();
			for (Entry<String, List<Aggregation>> entry : aggregationManager.getAggregations().entrySet())
			{
				for(Aggregation aggregation:entry.getValue()){
					KeyValue.Builder keyValue = KeyValue.newBuilder();
					// build cell using row-key as:
					// SUM/MAX/MIN/AVG/COUNT
					keyValue.setRowKey(ByteString.copyFrom(aggregation.getName().getBytes()));
					// set key as:
					// familu.qualifier
					keyValue.setKey(ByteString.copyFrom(entry.getKey().getBytes()));
					// set value as result of aggregation
					keyValue.setValue(ByteString.copyFrom((aggregation.getResult()+"").getBytes()));
					
					bsvRow.addKeyValue(keyValue.build());
				}
			}
			BSVRow aggregationRow = bsvRow.build();
			response.addRow(aggregationRow);
			
			// handle aggregation view materialization
			if(!aggregationManager.getAggregations().isEmpty() || request.getIsMaterialize()){
				materialize.putToAggregationView(aggregationRow);
			}
			
			// Depending on the plan, return the results or nothing
			System.out.println("Client wants the results: " + request.getIsReturningResults());
			if(request.getIsReturningResults()){
				done.run(response.build());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (scanner != null)
					scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void putToReverseJoinTable(List<Cell> row)
			throws InterruptedIOException, RetriesExhaustedWithDetailsException {
		// find join key
		for(Cell cell:row){
			String cellFamilyClone = new String(CellUtil.cloneFamily(cell));
			String cellQualifierClone = new String(CellUtil.cloneQualifier(cell));
			String cellString = cellFamilyClone + "." + cellQualifierClone;

			// use join key to put a row in reverse join table, use join key 
			// value as the row key
			if(cellString.equals(joinFamily + "." + joinQualifier)){
				Put put = new Put(CellUtil.cloneValue(cell));
				
				for(Cell cellForAdd:row){
					put.add(CellUtil.cloneFamily(cellForAdd), CellUtil.cloneQualifier(cellForAdd), CellUtil.cloneValue(cellForAdd));
				}
				
				joinTable.put(put);
				
				break;
			}
		}
	}

	/**
	 * If all cells pass the conditions build the whole row
	 * @param request
	 * @param row
	 * @param bsvRow
	 * @param aggKey 
	 * @return
	 */
	public boolean handleRow(ParameterMessage request, List<Cell> row,
			BSVRow.Builder bsvRow, String aggKey) {
		// in cell we have to consider the conditions
		boolean meetCondition = true;
		for(Cell cell:row){
			boolean checkCell = checkCellForConditions(request, cell);
			
			// if this cell doesn't meet any one of the conditions, this row doesn't pass
			if(!checkCell){
				meetCondition = false;
				break;
			}
			
			// pass all the conditions
			// now start to build cell body
			KeyValue.Builder keyvalue = KeyValue.newBuilder();
			System.out.println((new Date())+"Building cell " + cell);
			
			handleCell(cell, keyvalue, request, aggKey);
			
			bsvRow.addKeyValue(keyvalue);
		}
		return meetCondition;
	}

	/**
	 * Check if all cells pass the conditions.
	 * If yes than build the cell
	 * @param cell
	 * @param keyvalue
	 * @param request 
	 * @param aggKey 
	 * @return
	 */
	public byte[] handleCell(Cell cell, KeyValue.Builder keyvalue, ParameterMessage request, String aggKey) {
		// handle aggregation
		aggregationManager.handle(cell, aggKey);
		// row key
		byte[] rowBytes = new byte[cell.getRowLength()];
		System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowBytes, 0, cell.getRowLength());
		keyvalue.setRowKey(ByteString.copyFrom(rowBytes));
		
		// family and qualifier
		String familyQualifier = new String(CellUtil.cloneFamily(cell)) + "." + new String(CellUtil.cloneQualifier(cell));
		keyvalue.setKey(ByteString.copyFrom(familyQualifier.getBytes()));
		
		// value
		keyvalue.setValue(ByteString.copyFrom(CellUtil.cloneValue(cell)));
		return rowBytes;
	}

	/**
	 * Check if cell meets all the conditions 
	 * @param request
	 * @param cell
	 * @return
	 */
	public boolean checkCellForConditions(ParameterMessage request, Cell cell) {
		// test value for every condition
		boolean checkCell = true;
		for(Condition condition:request.getConditionList()){
			// get left expression and right expression
			String value = new String(CellUtil.cloneValue(cell));
			String compare = "";
			try {
				compare = condition.getValue().toString(StandardCharsets.UTF_8.displayName());
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			
			// greater than
			if(condition.getOperator().equals(ByteString.copyFrom(">".getBytes()))){
				if(Integer.parseInt(value) <= Integer.parseInt(compare)){
					checkCell = false;
					break;
				}
			}
			// less than
			else if(condition.getOperator().equals(ByteString.copyFrom("<".getBytes()))){
				if(Integer.parseInt(value) >= Integer.parseInt(compare)){
					checkCell = false;
					break;
				}
			}
			// greater than equal
			else if(condition.getOperator().equals(ByteString.copyFrom(">=".getBytes()))){
				if(Integer.parseInt(value) < Integer.parseInt(compare)){
					checkCell = false;
					break;
				}
			}
			// less than equal
			else if(condition.getOperator().equals(ByteString.copyFrom("<=".getBytes()))){
				if(Integer.parseInt(value) > Integer.parseInt(compare)){
					checkCell = false;
					break;
				}
			}
		}
		return checkCell;
	}
	
	/**
	 * Use this class to handle aggregation
	 * @author xiaojielin
	 *
	 */
	public class AggregationManager implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 3858867995910160121L;
		/**
		 * For every cell, there should be a list of aggregation
		 * Use the family
		 */
		private Map<String, List<Aggregation>> aggregations;
		
		/**
		 * Loop check request.aggregations and add them to aggregation list
		 * @param request
		 */
		public AggregationManager(ParameterMessage request){
			aggregations = new HashedMap();
			for(ByteString aggregation:request.getAggregationList()){
				// aggregation is in following format
				// sum:family.qualifier
				// max:family.qualifier
				// min:family.qualifier
				// avg:family.qualifier
				// count:familu.qualifier
				// ...
				String function = aggregation.toStringUtf8().split(":")[0];
				String key = aggregation.toStringUtf8().split(":")[1];
				
				// for every kind of aggregation new a class
				Aggregation agg = null;
				if(function.equalsIgnoreCase("sum")){
					agg = new Sum();
				}
				else if(function.equalsIgnoreCase("max")){
					agg = new Max();
				}
				else if(function.equalsIgnoreCase("min")){
					agg = new Min();
				}
				else if(function.equalsIgnoreCase("avg")){
					agg = new Avg();
				}
				else if(function.equalsIgnoreCase("count")){
					agg = new Count();
				}
				
				// first check if the list is created
				// for every key build a list
				if(!aggregations.containsKey(key)){
					List<Aggregation> aggList = new ArrayList<Aggregation>();
					aggregations.put(key, aggList);
				}
				aggregations.get(key).add(agg);
			}
			
			
		}

		/**
		 * Identify the aggregation for a cell and execute all the aggregation for the cell
		 * @param cell
		 * @param aggKey 
		 */
		public void handle(Cell cell, String aggKey) {
			String family = new String(CellUtil.cloneFamily(cell));
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			String keyPrefix = family + "." + qualifier;
			String key = family + "." + qualifier + "." + aggKey;
			
		
			// check if there is colfam.qualifier exist
			//   if yes, check if there is colfam.qualifier.aggkey exist
			//     if yes, execute aggregation
			//     if no, copy the list of aggregation from the list given the key of colfam.qualifier and then execute the aggregation
			System.out.println("Aggregation handler: check if aggregations " + aggregations + " contains prefix " + keyPrefix);
			if(aggregations.containsKey(keyPrefix)){
				if(aggKey.equals("")){
					for(Aggregation aggregation:aggregations.get(keyPrefix)){
						aggregation.execute(cell);
					}
				}else{
					System.out.println("Check if contains key " + key);
					if(!aggregations.containsKey(key)){
						System.out.println("Don't contains " + key);
						
						// deep copy the list
						ArrayList<Aggregation> copyList = new ArrayList<Aggregation>();
						for(Aggregation tmpApp:aggregations.get(keyPrefix)){
							try {
								copyList.add((Aggregation) tmpApp.clone());
							} catch (CloneNotSupportedException e) {
								e.printStackTrace();
							}
						}
						aggregations.put(key, copyList);
						System.out.println("After put into aggregations: " + aggregations);
					}
					
					// for each aggregation
					System.out.println("Execute over " + aggregations);
					for(Aggregation aggregation:aggregations.get(key)){
						aggregation.execute(cell);
					}
				}
			}
			
		}

		public Map<String, List<Aggregation>> getAggregations() {
			return aggregations;
		}

		public void setAggregations(Map<String, List<Aggregation>> aggregations) {
			this.aggregations = aggregations;
		}
		
		
	}
	
	/**
	 * Use this interface to define share method of aggregation
	 * @author xiaojielin
	 *
	 */
	public abstract class Aggregation implements Cloneable{
		/**
		 * Return the final result of this aggregation
		 * @return
		 */
		public abstract int getResult();
		
		/**
		 * Handle a cell
		 * @param cell
		 */
		public abstract void execute(Cell cell);
		
		/**
		 * Get human readable name
		 * @return
		 */
		public abstract String getName();

		@Override
		protected Object clone() throws CloneNotSupportedException {
			return super.clone();
		}
	}
	
	
	/**
	 * Sum implementation
	 * @author xiaojielin
	 *
	 */
	public class Sum extends Aggregation implements Cloneable{
		private int result = 0;

		@Override
		public int getResult() {
			return result;
		}

		@Override
		public void execute(Cell cell) {
			String value = new String(CellUtil.cloneValue(cell));
			int val = Integer.parseInt(value);
			result += val;
		}

		@Override
		public String getName() {
			return "SUM";
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			Sum sum = (Sum) super.clone(); 
			sum.result = result;
			return sum;
		}
	}
	
	/**
	 * Max implementation
	 * @author xiaojielin
	 *
	 */
	public class Max extends Aggregation implements Cloneable{
		private int max = Integer.MIN_VALUE;

		@Override
		public int getResult() {
			return max;
		}

		@Override
		public void execute(Cell cell) {
			String value = new String(CellUtil.cloneValue(cell));
			int val = Integer.parseInt(value);
			max = Math.max(val, max);
		}

		@Override
		public String getName() {
			return "MAX";
		}
		
		@Override
		protected Object clone() throws CloneNotSupportedException {
			Max maxObj = (Max) super.clone(); 
			maxObj.max = max;
			return maxObj;
		}
	}
	
	/**
	 * Min implementation
	 * @author xiaojielin
	 *
	 */
	public class Min extends Aggregation implements Cloneable{
		private int min = Integer.MAX_VALUE;

		@Override
		public int getResult() {
			return min;
		}

		@Override
		public void execute(Cell cell) {
			String value = new String(CellUtil.cloneValue(cell));
			int val = Integer.parseInt(value);
			min = Math.min(val, min);
		}

		@Override
		public String getName() {
			return "MIN";
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			Min minObj = (Min)super.clone();
			minObj.min = min;
			return minObj;
		}
	}
	
	/**
	 * Agerage implementation
	 * @author xiaojielin
	 *
	 */
	public class Avg extends Aggregation implements Cloneable{
		private int sum = 0;
		private int count = 0;

		@Override
		public int getResult() {
			if(count == 0){
				return 0;
			}else{
				return sum / count;
			}
		}

		@Override
		public void execute(Cell cell) {
			String value = new String(CellUtil.cloneValue(cell));
			int val = Integer.parseInt(value);
			sum += val;
			count++;
		}

		@Override
		public String getName() {
			return "AVG";
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			Avg avgObj = (Avg)super.clone();
			avgObj.sum = sum;
			avgObj.count = count;
			return avgObj;
		}
		
	}
	
	/**
	 * Count implementation
	 * @author xiaojielin
	 *
	 */
	public class Count extends Aggregation implements Cloneable{
		private int count = 0;

		@Override
		public int getResult() {
			return count;
		}

		@Override
		public void execute(Cell cell) {
			count++;
		}

		@Override
		public String getName() {
			return "COUNT";
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			Count countObj = (Count)super.clone();
			countObj.count = count;
			return countObj;
		}
		
	}
	
	/**
	 * Use this manager for materializing results
	 * @author jeff
	 *
	 */
	public class MaterializeManager{
		private String SQL = "";
		private HTable deltaView = null;
		private HTable selectView = null;
		private HTable aggregationView = null;
		
		public MaterializeManager(ParameterMessage request) {
			SQL = request.getSQL().toStringUtf8();
		}

		/**
		 * Build the aggregation view
		 * @param aggregationRow
		 */
		public void putToAggregationView(BSVRow aggregationRow) {
			if(aggregationView == null){
				Configuration conf = HBaseConfiguration.create();
				try {
					aggregationView = new HTable(conf, Common.senitiseSQL(SQL) + "_aggregation");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			putToAgggationTable(aggregationView, aggregationRow);
		}

		/**
		 * Put one row to aggregation view
		 * @param aggregationView2
		 * @param aggregationRow
		 */
		private void putToAgggationTable(HTable table, BSVRow aggregationRow) {
			// put to table
			for(int i = 0; i < aggregationRow.getKeyValueCount(); i++){
				// put one row 
				KeyValue keyValue = aggregationRow.getKeyValue(i);
				Put put = new Put(keyValue.getKey().toByteArray());
				put.add("colfam".getBytes(),(keyValue.getRowKey().toStringUtf8() + "_old").getBytes(), null);
				put.add("colfam".getBytes(), (keyValue.getRowKey().toStringUtf8() +  "_new").getBytes(), keyValue.getValue().toByteArray());
				try {
					table.put(put);
				} catch (RetriesExhaustedWithDetailsException e) {
					e.printStackTrace();
				} catch (InterruptedIOException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * Build the select view and put a row to it
		 * @param row
		 */
		public void putToSelectView(List<Cell> row) {
			if(selectView == null){
				Configuration conf = HBaseConfiguration.create();
				try {
					selectView = new HTable(conf, Common.senitiseSQL(SQL) + "_select");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			putToTable(selectView, row);
		}

		/**
		 * Handling of a row. Build the delta view. And put a row to it
		 * @param tableName
		 * @param row
		 */
		public void putToDeltaView(List<Cell> row){
			// Connect to the delta table that is created in the client
			// The table name would be the SQL statement with all the space replaced by '_'
			// To separate the delta view from the select view, a 'delta' is put the end of table name
			if(deltaView == null){
				Configuration conf = HBaseConfiguration.create();
				try {
					deltaView = new HTable(conf, Common.senitiseSQL(SQL) + "_delta");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			putToTable(deltaView, row);
		}

		/**
		 * Put one row to table
		 * @param table
		 * @param row
		 */
		private void putToTable(HTable table, List<Cell> row) {
			// put to table
			for(Cell cell:row){
				// put one row 
				Put put = new Put(CellUtil.cloneRow(cell));
				put.add("colfam".getBytes(), (new String(CellUtil.cloneQualifier(cell)) + "_old").getBytes(), null);
				put.add("colfam".getBytes(), (new String(CellUtil.cloneQualifier(cell)) + "_new").getBytes(), CellUtil.cloneValue(cell));
				try {
					table.put(put);
				} catch (RetriesExhaustedWithDetailsException e) {
					e.printStackTrace();
				} catch (InterruptedIOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
