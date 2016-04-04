//package com.lin.coprocessor;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.Coprocessor;
//import org.apache.hadoop.hbase.CoprocessorEnvironment;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
//import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
//import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
//import org.apache.hadoop.hbase.protobuf.ResponseConverter;
//import org.apache.hadoop.hbase.regionserver.InternalScanner;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import com.google.protobuf.RpcCallback;
//import com.google.protobuf.RpcController;
//import com.google.protobuf.Service;
//import com.lin.coprocessor.generated.SelectCoprocessor.Select;
//import com.lin.coprocessor.generated.SelectCoprocessor.SelectRequest;
//import com.lin.coprocessor.generated.SelectCoprocessor.SelectResponse;
//import com.lin.coprocessor.generated.SelectCoprocessor.SelectResponse.Builder;
//
//public class SelectCoprocessorImpl extends Select implements
//		Coprocessor, CoprocessorService {
//	private RegionCoprocessorEnvironment env;
//
//	@Override
//	public Service getService() {
//		return this;
//	}
//
//	@Override
//	public void start(CoprocessorEnvironment env) throws IOException {
//		if (env instanceof RegionCoprocessorEnvironment) {
//			this.env = (RegionCoprocessorEnvironment) env;
//		} else {
//			throw new CoprocessorException("Must be loaded on a table region!");
//		}
//	}
//
//	@Override
//	public void stop(CoprocessorEnvironment arg0) throws IOException {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void performSelect(RpcController controller, SelectRequest request,
//			RpcCallback<SelectResponse> done) {
//		Scan scan = new Scan();
//		System.out.println("going to set family " + request.getFamily());
//        scan.addFamily(request.getFamily().toByteArray());
//        System.out.println("going to set column " + request.getProjection());
//        scan.addColumn(request.getFamily().toByteArray(), request.getProjection().toByteArray());
//        SelectResponse response = null;
//        InternalScanner scanner = null;
//        try {
//            scanner = env.getRegion().getScanner(scan);
//            List<Cell> results = new ArrayList<Cell>();
//            Builder builder = response.newBuilder();
//            boolean hasMore = false;
//            byte[] lastRow = null;
//            long count = 0;
//            do {
//                hasMore = scanner.next(results);
//                System.out.println("we have " + results.size() + " results here");
//                org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result oneResult = null;
//                for (Cell cell : results) {
//                	byte[] currentRow = CellUtil.cloneRow(cell);
//                    if (lastRow == null || !Bytes.equals(lastRow, currentRow)) {
//                      lastRow = currentRow;
//                      count++;
//                    }
////                	System.out.println("getFamilyArray:"+cell.getFamilyArray());
////                	System.out.println("getFamily:"+cell.getFamily());
////                	System.out.println("ByteString.copyFrom:"+ByteString.copyFrom(cell.getFamilyArray()));
////                	System.out.println("getValueArray:"+cell.getValueArray());
////                	System.out.println("getValue:"+cell.getValue());
////                	System.out.println("ByteString.copyFrom:"+ByteString.copyFrom(cell.getValueArray()));
////                	//result = Long.parseLong(new String(cell.getValue()));
////                    org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell cellMessage = org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell.newBuilder()
////                    		.setFamily(ByteString.copyFrom(cell.getFamilyArray()))
////                    		.setRow(ByteString.copyFrom(cell.getRowArray()))
////                    		.setValue(ByteString.copyFrom(cell.getValueArray())).build();
////                    oneResult = oneResult.newBuilder().addCell(cellMessage).build();
//                }
//                results.clear();
//
//                System.out.println("the count is " + count);
//
//                builder.addResultRows(oneResult);
//            } while (hasMore);
//
//            response = builder.build();
//        } catch (IOException ioe) {
//            ResponseConverter.setControllerException(controller, ioe);
//        } finally {
//            if (scanner != null) {
//                try {
//                    scanner.close();
//                } catch (IOException ignored) {}
//            }
//        }
//        done.run(response);
//	}
//
//
//
//}
