package com.lin.coprocessor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

import com.lin.sql.JsqlParser;
import com.lin.utils.HBaseHelper;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

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

public class BSVCoprocessorEndPoint extends Execute implements Coprocessor,
		CoprocessorService{
	private RegionCoprocessorEnvironment env;
	private AggregationManager aggregationManager = null;
	private MaterializeManager materialize = null;
	private String joinFamily = null;
	private String joinQualifier = null;
	private ParameterMessage request = null;
	private RpcCallback<ResultMessage> doneCallback;
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
		this.doneCallback = done;
		// initialize materialize manager
		materialize = new MaterializeManager(request);

		// initialize aggregation manager
		aggregationManager = new AggregationManager(request);

		Scan scan = prepareScan();
		doBatchProcessing(scan);
	}

	private Scan prepareScan(){
		// initialize scan
		Scan scan = new Scan();
		scan.setMaxVersions(1);

		// check if join
		// If join, set is-materialize to be true.
		// Fill joinKey for later use
		if(!request.getJoinKey().toStringUtf8().equals("")){
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
		return scan;
	}
	private ResultMessage.Builder putAggregations(ResultMessage.Builder response){
		// add aggregation result to response
		// put all the aggregation in one row
		if(!aggregationManager.isAvailable()) return response;


		HashSet<Byte> requiredAggFunctions = JsqlParser.typeOfAggregation(materialize.getQuery());

		BSVRow.Builder bsvRow = BSVRow.newBuilder();
		BSVRow.Builder bsvRowDelta = BSVRow.newBuilder();
		boolean avgFlag = false, rowFlag = false;
		if(requiredAggFunctions.contains(JsqlParser.AGGREGATION_AVG)){
			avgFlag = true;
		}
		for (Entry<String, List<Aggregation>> entry : aggregationManager.getAggregations().entrySet())
		{
			ByteString key = ByteString.copyFrom(entry.getKey().getBytes());
			int sum = 1, count = 1;

			for(Aggregation aggregation:entry.getValue()){
				KeyValue.Builder keyValue = KeyValue.newBuilder();
				// build cell using row-key as:
				// SUM/MAX/MIN/AVG/COUNT
				String aggType = aggregation.getName();
				keyValue.setRowKey(ByteString.copyFrom(aggType.getBytes()));
				// set key as:
				// family.qualifier
				keyValue.setKey(key);
				// set value as result of aggregation
				keyValue.setValue(ByteString.copyFrom((aggregation.getResult()+"").getBytes()));

				if(requiredAggFunctions.contains(aggregation.getType())){
					rowFlag = true;
					bsvRow.addKeyValue(keyValue.build());
				}
				bsvRowDelta.addKeyValue(keyValue.clone());

				if(aggType.equalsIgnoreCase("SUM")) sum = aggregation.getResult();
				else if(aggType.equalsIgnoreCase("COUNT")) count = aggregation.getResult();
			}

			if(avgFlag){
				double value = 1.0 * sum / count;
				KeyValue.Builder keyValue = KeyValue.newBuilder().setRowKey(ByteString.copyFromUtf8("AVG")).setKey(key).setValue(ByteString.copyFromUtf8(String.valueOf(value)));
				bsvRow.addKeyValue(keyValue.build());
			}
		}
		BSVRow aggregationRow = bsvRow.build();
		BSVRow aggregationRowDelta = bsvRowDelta.build();
		response.addRow(aggregationRow);

		// handle aggregation view materialization
		if(!aggregationManager.getAggregations().isEmpty() || request.getIsMaterialize()){
			if(rowFlag) materialize.putToView(aggregationRow);
			materialize.putToDeltaView(aggregationRowDelta);
		}
		return response;
	}
	private void doBatchProcessing(Scan scan){

		// response builder
		ResultMessage.Builder response = ResultMessage.newBuilder();

		response = scanOverTable(scan, response);
		response = putAggregations(response);

		// closing connections
		materialize.close();

		// Depending on the plan, return the results or nothing
		System.out.println("Client wants the results: " + request.getIsReturningResults());
		if(request.getIsReturningResults()){
			doneCallback.run(response.build());
		}

	}

	private void putToReverseJoinTable(List<Cell> row)
			throws InterruptedIOException, RetriesExhaustedWithDetailsException {
		// find join key

		// TODO: Optimize this by converting one for loop
		for(Cell cell:row){
			String cellFamilyClone = new String(CellUtil.cloneFamily(cell));
			String cellQualifierClone = new String(CellUtil.cloneQualifier(cell));
			String cellString = cellFamilyClone + "." + cellQualifierClone;

			// use join key to put a row in reverse join table, use join key
			// value as the row key
			if(cellString.equals(joinFamily + "." + joinQualifier)){
				Put put = new Put(CellUtil.cloneValue(cell));

				for(Cell cellForAdd:row){
					System.out.println("Puting cell " + new String(CellUtil.cloneQualifier(cellForAdd)) + " = " + new String(CellUtil.cloneValue(cellForAdd)));
					String cellForAddRowClone = new String(CellUtil.cloneRow(cell));
					String cellForAddQualifierClone = new String(CellUtil.cloneQualifier(cellForAdd));
//					if(cellForAddQualifierClone.equals(cellQualifierClone)){
//						continue;
//					}
					put.add(env.getRegionInfo().getTable().getName(),
							(cellForAddRowClone + "_" + cellForAddQualifierClone).getBytes(),
							CellUtil.cloneValue(cellForAdd));
				}
				materialize.putToDeltaView(put);
				break;
			}
		}
	}

	private void processJoinView(List<Cell> curVals){
		System.out.println("Join row: " + new String(CellUtil.cloneRow(curVals.get(0))));
		// Construct join table view
		// 1. construct temporary map

		Map<String, Map<String, Map<String, String>>> tmp = new HashMap<String, Map<String, Map<String, String>>>();
		List<String> familyList = new ArrayList<String>();
		String joinKey = null;
		for(Cell cell:curVals){
			// save join key for later use
			if(joinKey == null){
				joinKey = new String(CellUtil.cloneRow(cell));
			}
			System.out.println("Constructing cell "
					+ new String(CellUtil.cloneFamily(cell))
					+ ":"
					+ new String(CellUtil.cloneQualifier(cell))
					+ "="
					+ new String(CellUtil.cloneValue(cell)));

			String family =  new String(CellUtil.cloneFamily(cell));
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			String value = new String(CellUtil.cloneValue(cell));

			String rowKey = qualifier.split("_")[0];

			if(!tmp.containsKey(family)){
				Map<String, Map<String, String>> tmpMap = new HashMap<String, Map<String, String>>();
				tmp.put(family, tmpMap);
				familyList.add(family);
			}

			if(!tmp.get(family).containsKey(rowKey)){
				tmp.get(family).put(rowKey, new HashMap<String, String>());
			}

			tmp.get(family).get(rowKey).put(qualifier, value);
		}
		System.out.println(tmp);

		// 2. construct join row with temporary list

		Map<String, String> fullJoinRow = new HashMap<String, String>();
		// now we consider only inner join
		// if familyList has less than 2 element
		// we do nothing
		if(familyList.size() >= 2){
			Map<String, Map<String, String>> leftJoin = tmp.get(familyList.get(0));
			Map<String, Map<String, String>> rightJoin = tmp.get(familyList.get(1));
			for(Entry<String, Map<String, String>> leftJoinRow:leftJoin.entrySet()){

				for(Entry<String, Map<String, String>> rightJoinRow:rightJoin.entrySet()){

					System.out.println("Joining " + leftJoinRow.getKey() + " and " + rightJoinRow.getKey());

					Put put = new Put((familyList.get(0) + "_" + leftJoinRow.getKey() + "_" + familyList.get(1) + "_" + rightJoinRow.getKey()).getBytes());

					// add all in left join
					for(Entry<String, String> leftJoinCell:leftJoinRow.getValue().entrySet()){
						put.add("colfam".getBytes(),
								leftJoinCell.getKey().split("_")[1].getBytes(),
								leftJoinCell.getValue().getBytes());
					}
					// add all in right join
					for(Entry<String, String> rightJoinCell:rightJoinRow.getValue().entrySet()){
						put.add("colfam".getBytes(),
								rightJoinCell.getKey().split("_")[1].getBytes(),
								rightJoinCell.getValue().getBytes());
					}
					materialize.putToView(put);
				}


			}
			System.out.println("Full join row : \n" + fullJoinRow);



		}
	}
	private ResultMessage.Builder scanOverTable(Scan scan, ResultMessage.Builder response){
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

				// show scan result
				System.out.println("Scan result:");
				for(Cell cell:row){
					System.out.println("*******************************************************");
					System.out.println("\tRow: " + new String(CellUtil.cloneRow(cell)));
					System.out.println("\tColumn Family: " + new String(CellUtil.cloneFamily(cell)));
					System.out.println("\tQualifier: " + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("\tValue: " + new String(CellUtil.cloneValue(cell)));
				}

				// find the aggregation key first
				// TODO: Add a control. It is doing unnecessary calculations for select and join views..
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

				// build join table
				if(request.getIsBuildJoinView()){
					processJoinView(curVals);
				}

				/*
				 * start of handling result
				 */
				BSVRow.Builder bsvRow = BSVRow.newBuilder();
				System.out.println((new Date())+"Building row no." + count);

				boolean meetCondition = handleRow(request, row, bsvRow, aggKey);

				// discard the whole row if the cell fails to meet the condition
				if(meetCondition){
					response.addRow(bsvRow);

					count++;

					// build the select view
					if(request.getIsMaterialize()){
						// selection view
						if(JsqlParser.typeOfQuery(materialize.getQuery()) == JsqlParser.SELECT){
							materialize.putToView(row);
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
		return response;
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
		if(aggregationManager.isAvailable()) aggregationManager.handle(cell, aggKey);
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
			// first check if this cell is the left operator
			// if not skip this
			String leftColumn = condition.getColumn().getFamily().toStringUtf8() + "." + condition.getColumn().getColumn().toStringUtf8();
			String cellColumn = (new String(CellUtil.cloneFamily(cell))) + "." + (new String(CellUtil.cloneQualifier(cell)));
			System.out.println("comparing " + leftColumn + " with " + cellColumn);
			if(!leftColumn.equals(cellColumn)){
				break;
			}
			
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

		private boolean available = false;
		private String deltaView;
		/**
		 * Loop check request.aggregations and add them to aggregation list
		 * @param request
		 */
		public AggregationManager(ParameterMessage request){
			if(JsqlParser.typeOfQuery(request.getSQL().toStringUtf8()) == JsqlParser.AGGREGATION) {
				deltaView = request.getViewName().toStringUtf8() + "_delta";

				aggregations = new HashedMap();

				String key = request.getAggregationList().get(0).toStringUtf8().split(":")[1];

				if (!aggregations.containsKey(key)) {
					List<Aggregation> aggList = new ArrayList<Aggregation>();
					aggregations.put(key, aggList);
				}
				aggregations.get(key).add(new Min());
				aggregations.get(key).add(new Max());
				aggregations.get(key).add(new Count());
				aggregations.get(key).add(new Sum());

				available = true;
			}
		}

		public boolean isAvailable() {
			return available;
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
				String row;
				String colfam = "colfam";;
				String qual;
				String value;
				if(aggKey.equals("")){
					for(Aggregation aggregation:aggregations.get(keyPrefix)){
						aggregation.execute(cell);
					}
					row = keyPrefix;
				}else{
					System.out.println("Check if contains key " + key);
					if(!aggregations.containsKey(key)){
						System.out.println("Don't contain " + key);
						
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

					row = key;
				}
				// Add to deltaview.
				qual = new String(CellUtil.cloneRow(cell));
				value = new String(CellUtil.cloneValue(cell));
				try {
					HBaseHelper.getHelper(HBaseConfiguration.create()).put(deltaView, row, colfam, qual, value);
				} catch (IOException e) {
					e.printStackTrace();
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

		public abstract Byte getType();

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
		public Byte getType() {
			return JsqlParser.AGGREGATION_SUM;
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
		public Byte getType() {
			return JsqlParser.AGGREGATION_MAX;
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
		public Byte getType() {
			return JsqlParser.AGGREGATION_MIN;
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
		public Byte getType() {
			return JsqlParser.AGGREGATION_AVG;
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

		@Override
		public Byte getType() {
			return JsqlParser.AGGREGATION_COUNT;
		}
		
	}
	
	/**
	 * Use this manager for materializing results
	 * @author jeff
	 *
	 */
	public class MaterializeManager{
		private String query = "";
		private String viewName = "";
		private HTableInterface deltaView = null;
		private HTableInterface view = null;
		
		/**
		 * Close table connection
		 */
		public void close(){
			if(deltaView != null){
				try {
					deltaView.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(view != null){
				try {
					view.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		public MaterializeManager(ParameterMessage request) {
			query = request.getSQL().toStringUtf8();
			viewName = request.getViewName().toStringUtf8();
			view = getView(viewName);
			deltaView = getView(viewName + "_delta");
		}

		private HTableInterface getView(String viewName){
			HTableInterface view = null;
			try {
				view = env.getTable(TableName.valueOf(viewName));
			} catch (IOException e) {
				e.printStackTrace();
			}
			return view;
		}
		/**
		 * Build the aggregation view
		 * @param aggregationRow
		 */
		public void putToView(BSVRow aggregationRow) { // Aggregation View
			putToTable(view, aggregationRow);
		}

		/**
		 * Build the aggregation view
		 * @param aggregationRow
		 */
		public void putToDeltaView(BSVRow aggregationRow) { // Aggregation Delta View
			putToTable(deltaView, aggregationRow);
		}

		public void putToView(Put put) { // Aggregation Delta View
			putToTable(view, put);
		}
		public void putToDeltaView(Put put) { // Aggregation Delta View
			putToTable(deltaView, put);
		}

		/**
		 * Put one row to aggregation view
		 * @param aggregationRow
		 */
		private void putToTable(HTableInterface table, BSVRow aggregationRow) {
			// put to table
			for(int i = 0; i < aggregationRow.getKeyValueCount(); i++){
				// put one row 
				KeyValue keyValue = aggregationRow.getKeyValue(i);
				Put put = new Put(keyValue.getKey().toByteArray());
				put.add("colfam".getBytes(), (keyValue.getRowKey().toStringUtf8()).getBytes(), keyValue.getValue().toByteArray());
				putToTable(table, put);
			}
		}

		/**
		 * Build the select view and put a row to it
		 * @param row
		 */
		public void putToView(List<Cell> row) { // Select View
			putToTable(view, row);
		}

		/**
		 * Handling of a row. Build the delta view. And put a row to it
		 * @param row
		 */
		public void putToDeltaView(List<Cell> row){
			putToTable(deltaView, row);
		}

		/**
		 * Put one row to table
		 * @param table
		 * @param row
		 */
		private void putToTable(HTableInterface table, List<Cell> row) {
			// put to table
			for(Cell cell:row){
				// put one row
				Put put = new Put(CellUtil.cloneRow(cell));
				put.add("colfam".getBytes(), (new String(CellUtil.cloneQualifier(cell))).getBytes(), CellUtil.cloneValue(cell));
				putToTable(table, put);
			}
		}

		private void putToTable(HTableInterface table, Put put){
			try {
				table.put(put);
			} catch (RetriesExhaustedWithDetailsException e) {
				e.printStackTrace();
			} catch (InterruptedIOException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public String getQuery() {
			return query;
		}

	}
}
