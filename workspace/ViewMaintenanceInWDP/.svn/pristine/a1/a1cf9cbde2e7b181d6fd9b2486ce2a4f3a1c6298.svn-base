package de.webdataplatform.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import de.webdataplatform.test.Sum.KeyValue;
import de.webdataplatform.test.Sum.SumRequest;
import de.webdataplatform.test.Sum.SumResponse.Builder;
import de.webdataplatform.test.Sum.SumResponse;
import de.webdataplatform.test.Sum.SumService;


public class SumEndpoint extends SumService implements Coprocessor, CoprocessorService {
    
    private RegionCoprocessorEnvironment env;
     
    @Override
    public Service getService() {
        return this;
    }
 
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }
 
     
    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // do mothing
    }
    
	
	
	
	private static Map<String, Integer> aggregationMap=new HashMap<String, Integer>();

	long completeTime = 0;
     
    @Override
    public void getSum(RpcController controller, SumRequest request, RpcCallback done) {
        
    	aggregationMap=new HashMap<String, Integer>();
    	Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(request.getFamily()));
        scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes("colAggKey"));
        scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes("colAggVal"));
        SumResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);

    			List<Cell> curVals;
//    			Result done;
    			
    			long startTime = new Date().getTime();
    			
    			boolean isDone = false;
    			do {
    				
    				curVals = new ArrayList<Cell>();
    				curVals.clear();
    				isDone = scanner.next(curVals);
    				
    	
    					Integer updateSumValue = 0;
    					Integer updateCountValue = 0;
    					String aggregationKey="";
    					
    					for(Cell cell : curVals){

//    						System.out.println("Key: "+Bytes.toString(cell.getRow()));
    						
    						
    						if(Bytes.toString(cell.getQualifier()).equals("colAggKey")){
    							
    							aggregationKey = Bytes.toString(cell.getValue());
    							
    						}
    						
    						if(Bytes.toString(cell.getQualifier()).equals("colAggVal")){
    							
    							if(aggregationMap.containsKey(aggregationKey)){
    								Integer val = aggregationMap.get(aggregationKey);
    								aggregationMap.put(aggregationKey, val+1);
    							}else{
    								aggregationMap.put(aggregationKey, 1);
    							}
    							
    						}
//    						System.out.println("Key: "+aggregationKey);
//    								
//    						updateSumValue = aggregationSumMap.get(aggregationKey);
    								
    							

    						
    					}
    				

    				
    			} while (isDone);
    			
    			completeTime+=new Date().getTime() - startTime;
    			System.out.println("Scan finished took:" + (new Date().getTime() - startTime));
    			System.out.println("completeTime:" + completeTime);
//            List<Cell> results = new ArrayList<Cell>();
//            boolean hasMore = false;
//                        long sum = 0L;
//                do {
//                        hasMore = scanner.next(results);
//                        for (Cell cell : results) {
//                            sum = sum + 1;
////                            sum = sum + Bytes.toLong(CellUtil.cloneValue(cell));
//                        }
//                        results.clear();
//                } while (hasMore);
// 
    			Builder builder = SumResponse.newBuilder();
    			
    			for (String key : aggregationMap.keySet()) {
					
    				KeyValue keyValue = KeyValue.newBuilder().setKey(key).setValue(aggregationMap.get(key)).build();
    				builder.addSumMap(keyValue);
    				
				}
                response = builder.build();
//             
                
                
                
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
