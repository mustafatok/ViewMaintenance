package com.lin.coprocessor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
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
		CoprocessorService {
	private RegionCoprocessorEnvironment env;

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
		// TODO Auto-generated method stub

	}

	@Override
	public void batch(RpcController controller, ParameterMessage request,
			RpcCallback<ResultMessage> done) {
		System.out.println("============================================================");
		System.out.println((new Date())+"Begin to execute");
		// initialize scan
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		
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

		// response
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
				}
			}
			
			System.out.println((new Date())+"Finish building response message");
			response.setSize(count);

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
			
			byte[] rowBytes = handleCell(cell, keyvalue);
			bsvRow.addKeyValue(keyvalue);
		}
		return meetCondition;
	}

	/**
	 * Check if all cells pass the conditions.
	 * If yes than build the cell
	 * @param cell
	 * @param keyvalue
	 * @return
	 */
	public byte[] handleCell(Cell cell, KeyValue.Builder keyvalue) {
		// row key
		byte[] rowBytes = new byte[cell.getRowLength()];
		System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowBytes, 0, cell.getRowLength());
		keyvalue.setRowKey(ByteString.copyFrom(rowBytes));
		
		// qualifier
		keyvalue.setKey(ByteString.copyFrom(CellUtil.cloneQualifier(cell)));
		
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
				// TODO Auto-generated catch block
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

}
