package com.lin.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ProtobufCoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.lin.coprocessor.generated.SelectCoprocessor;
import com.lin.coprocessor.generated.SelectCoprocessor.Select;
import com.lin.coprocessor.generated.SelectCoprocessor.SelectRequest;
import com.lin.coprocessor.generated.SelectCoprocessor.SelectResponse;
import com.lin.coprocessor.generated.SumCoprocessor.SumResponse;

public class SelectCoprocessorImpl extends Select implements
		Coprocessor, CoprocessorService {
	private RegionCoprocessorEnvironment env;
	
	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment arg0) throws IOException {
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
	public void performSelect(RpcController controller, SelectRequest request,
			RpcCallback<SelectResponse> done) {
		Scan scan = new Scan();
        scan.addFamily(request.getFamily().toByteArray());
        scan.addColumn(request.getFamily().toByteArray(), request.getProjection().toByteArray());
        SelectResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            do {
                hasMore = scanner.next(results);
                org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result oneResult = null;
                for (Cell cell : results) {
                	System.out.println("getFamilyArray:"+cell.getFamilyArray());
                	System.out.println("getFamily:"+cell.getFamily());
                	System.out.println("ByteString.copyFrom:"+ByteString.copyFrom(cell.getFamilyArray()));
                	System.out.println("getValueArray:"+cell.getValueArray());
                	System.out.println("getValue:"+cell.getValue());
                	System.out.println("ByteString.copyFrom:"+ByteString.copyFrom(cell.getValueArray()));
                	//result = Long.parseLong(new String(cell.getValue()));
                    org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell cellMessage = org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell.newBuilder()
                    		.setFamily(ByteString.copyFrom(cell.getFamilyArray()))
                    		.setRow(ByteString.copyFrom(cell.getRowArray()))
                    		.setValue(ByteString.copyFrom(cell.getValueArray())).build(); 
                    oneResult = oneResult.newBuilder().addCell(cellMessage).build();
                }
                results.clear();
                
                response = response.newBuilder().addResultRows(oneResult).build();
            } while (hasMore);
 
             
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }
        done.run(response);
	}



}
