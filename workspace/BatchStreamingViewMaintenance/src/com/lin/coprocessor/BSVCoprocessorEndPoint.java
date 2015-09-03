package com.lin.coprocessor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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

public class BSVCoprocessorEndPoint extends Execute implements Coprocessor,
		CoprocessorService {
	private RegionCoprocessorEnvironment env;
	private AggregationManager aggregationManager = null;
	private boolean isMaterialize = false;
	private String joinFamily = null;
	private String joinQualifier = null;
	private HTable joinTable = null;

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
		
		
		// initialize aggregation manager
		aggregationManager = new AggregationManager(request);
		
		// initialize scan
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		
		// check if join
		// If join, set is-materialize to be true.
		// Fill joinKey and joinTable for later use
		if(request.getJoinKey() != null){
			isMaterialize = true;
			
			// Connect to the join table using the join table name from request
			Configuration configuration = HBaseConfiguration.create();;
			try {
				joinTable = new HTable(configuration, request.getJoinTable().toByteArray());
			} catch (IOException e) {
				// TODO Auto-generated catch block
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
			List<List<Cell>> results = new ArrayList<List<Cell>>();
			boolean finish = false;
			Date begin = new Date();
			System.out.println(begin+"Begin to scan");
			do {
				curVals.clear();
				finish = scanner.next(curVals);
				List<Cell> tmp = new ArrayList<Cell>(curVals);
				results.add(tmp);
			} while (finish);
			Date end = new Date();
			System.out.println(end+" Finish scanning in " + (end.getTime()-begin.getTime()) + " million seconds");
			System.out.println("Scann result are: " + results.toString());

			long count = 0;
			System.out.println((new Date())+"Begin to build response message");
			
			// handle results
			for (List<Cell> row : results) {
				BSVRow.Builder bsvRow = BSVRow.newBuilder();
				System.out.println((new Date())+"Building row no." + count);
				
				boolean meetCondition = handleRow(request, row, bsvRow);
				
				// discard the whole row if the cell fails to meet the condition
				if(meetCondition){
					response.addRow(bsvRow);
					count++;
					
					// if is materialize put it to join table
					if(isMaterialize){
						// find join key
						for(Cell cell:row){
							String cellFamilyClone = new String(CellUtil.cloneFamily(cell));
							String cellQualifierClone = new String(CellUtil.cloneQualifier(cell));
							String cellString = cellFamilyClone + "." + cellQualifierClone;

							// use join key to put a row in reverse join table, use join key 
							// value as the row key
							if(cellString.equals(joinFamily + "." + joinQualifier)){
								Put put = new Put(cellString.getBytes());
								
								for(Cell cellForAdd:row){
									put.add(cellForAdd);
								}
								
								joinTable.put(put);
							}
						}
						
					}
				}
			}
			
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
			response.addRow(bsvRow.build());

			done.run(response.build());
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

	/**
	 * If all cells pass the conditions build the whole row
	 * @param request
	 * @param row
	 * @param bsvRow
	 * @return
	 */
	public boolean handleRow(ParameterMessage request, List<Cell> row,
			BSVRow.Builder bsvRow) {
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
			
			handleCell(cell, keyvalue, request);
			
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
	 * @return
	 */
	public byte[] handleCell(Cell cell, KeyValue.Builder keyvalue, ParameterMessage request) {
		// handle aggregation
		aggregationManager.handle(cell);
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
	public class AggregationManager{
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
		 */
		public void handle(Cell cell) {
			String family = new String(CellUtil.cloneFamily(cell));
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			String key = family + "." + qualifier;
			
			// for each cell
			if(aggregations.containsKey(key)){
				// for each aggregation
				for(Aggregation aggregation:aggregations.get(key)){
					aggregation.execute(cell);
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
	public interface Aggregation{
		/**
		 * Return the final result of this aggregation
		 * @return
		 */
		public int getResult();
		
		/**
		 * Handle a cell
		 * @param cell
		 */
		public void execute(Cell cell);
		
		public String getName();
	}
	
	/**
	 * Sum implementation
	 * @author xiaojielin
	 *
	 */
	public class Sum implements Aggregation{
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
		
	}
	
	/**
	 * Max implementation
	 * @author xiaojielin
	 *
	 */
	public class Max implements Aggregation{
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
	}
	
	/**
	 * Min implementation
	 * @author xiaojielin
	 *
	 */
	public class Min implements Aggregation{
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
		
	}
	
	/**
	 * Agerage implementation
	 * @author xiaojielin
	 *
	 */
	public class Avg implements Aggregation{
		private int sum = 0;
		private int count = 0;

		@Override
		public int getResult() {
			return sum / count;
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
		
	}
	
	/**
	 * Count implementation
	 * @author xiaojielin
	 *
	 */
	public class Count implements Aggregation{
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
		
	}

}
