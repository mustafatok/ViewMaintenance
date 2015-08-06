package com.lin.coprocessor;

import java.io.IOException;
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
		System.out.println((new Date())+"[INFO] Begin to execute");
		// initialize scan
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		
		for(BSVColumn bsvColumn:request.getColumnList()){
			System.out.println((new Date())+"[INFO] Begin to add column: "+bsvColumn);
			// get parameters
			byte[] family = bsvColumn.getFamily().toByteArray();
			byte[] column = bsvColumn.getColumn().toByteArray();
			
			scan.addColumn(family, column);			
			System.out.println((new Date())+"[INFO] Finish adding column: "+bsvColumn);
		}

		// response
		ResultMessage.Builder response = ResultMessage.newBuilder();

		// use an internal scanner to perform scanning.
		InternalScanner scanner = null;
		try {
			scanner = env.getRegion().getScanner(scan);

			List<Cell> curVals = new ArrayList<Cell>();
			List<List<Cell>> results = new ArrayList<List<Cell>>();
			boolean finish = false;
			Date begin = new Date();
			System.out.println(begin+"[INFO] Begin to scan");
			do {
				curVals.clear();
				finish = scanner.next(curVals);
				results.add(curVals);
			} while (finish);
			Date end = new Date();
			System.out.println(end+"[INFO] Finish scanning in " + (end.getTime()-begin.getTime()) + " million seconds");

			// build every cell
			long count = 0;
			System.out.println((new Date())+"[INFO] Begin to build response message");
			for (List<Cell> row : results) {
				BSVRow.Builder bsvRow = BSVRow.newBuilder();
				System.out.println((new Date())+"[INFO] Building row" + count);
				for(Cell cell:row){
					System.out.println((new Date())+"[INFO] Building cell " + cell);
					KeyValue.Builder keyvalue = KeyValue.newBuilder();
					keyvalue.setKey(ByteString.copyFrom(CellUtil.cloneQualifier(cell)));
					bsvRow.addKeyValue(keyvalue);
				}
				response.addRow(bsvRow);
				count++;
			}
			System.out.println((new Date())+"[INFO] Finish building response message");
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

}
