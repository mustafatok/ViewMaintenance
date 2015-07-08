package com.lin.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.lin.coprocessor.generated.ProjectionProtos;
import com.lin.coprocessor.generated.ProjectionProtos.Projection;
import com.lin.coprocessor.generated.ProjectionProtos.ProjectionRequest;
import com.lin.coprocessor.generated.ProjectionProtos.ProjectionResponse;

public class ProjectionEndPoint extends Projection implements Coprocessor,
		CoprocessorService {
	private RegionCoprocessorEnvironment env;

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment arg0) throws IOException {
		System.out.println("jeff+++++++++++++");
		System.out.println(arg0);
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendProjection(RpcController controller,
			ProjectionRequest request, RpcCallback<ProjectionResponse> done) {
		// get parameters
		byte[] family = request.getFamily().toByteArray();
		byte[] column = request.getColumn().toByteArray();

		// initialize scan
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.addColumn(family, column);
		
		// response
		ProjectionResponse.Builder response = ProjectionResponse.newBuilder();

		// use an internal scanner to perform scanning.
		InternalScanner scanner = null;
		try {
			scanner = env.getRegion().getScanner(scan);

			List<Cell> curVals = new ArrayList<Cell>();
			List<Cell> results = new ArrayList<Cell>();
			boolean finish = false;
			do {
				curVals.clear();
				finish = scanner.next(curVals);
				results.addAll(curVals);
			} while (finish);
			
			// build every cell
			for(Cell cell:results){
				ProjectionProtos.Cell projectionCell = ProjectionProtos.Cell.newBuilder().setValue(Bytes.toString(CellUtil.cloneValue(cell))).build();
				response.addResults(projectionCell);
			}
			
			done.run(response.build());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
