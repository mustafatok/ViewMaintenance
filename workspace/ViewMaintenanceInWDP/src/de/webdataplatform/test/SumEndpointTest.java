package de.webdataplatform.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

import de.webdataplatform.test.Sum.KeyValue;
import de.webdataplatform.test.Sum.SumRequest;
import de.webdataplatform.test.Sum.SumResponse;
import de.webdataplatform.test.Sum.SumService;

public class SumEndpointTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "deltaserver1");
		HConnection connection;
		HTableInterface table=null;
		try {
			connection = HConnectionManager.createConnection(conf);
			table = connection.getTable("bt1");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		final SumRequest request = SumRequest.newBuilder().setFamily("colfam1").setColumn("gross").build();
		try {
		Map<byte[], List<KeyValue>> results = table.coprocessorService (SumService.class, null, null, new Batch.Call<SumService, List<KeyValue>>() {
		    @Override
		        public List<KeyValue> call(SumService aggregate) throws IOException {
		    		BlockingRpcCallback rpcCallback = new BlockingRpcCallback();
		            aggregate.getSum(null, request, rpcCallback);
		            SumResponse response = (SumResponse)rpcCallback.get();
		            return response.getSumMapList();
		        }
		    });
		    
		Map<String, Long> aggregationMap=new HashMap<String, Long>();
		for (byte[] key : results.keySet()) {
			System.out.println(Bytes.toString(key));
		}
		for (List<KeyValue> sum : results.values()) {
			for (KeyValue keyValue : sum) {
				if(aggregationMap.containsKey(keyValue.getKey())){
					Long val = aggregationMap.get(keyValue.getKey());
					aggregationMap.put(keyValue.getKey(), val+keyValue.getValue());
				}else{
					aggregationMap.put(keyValue.getKey(), keyValue.getValue());
				}
				aggregationMap.put(keyValue.getKey(), keyValue.getValue());
			}
		 }
		System.out.println("Map = " + aggregationMap);
		    
		    
		} catch (ServiceException e) {
		e.printStackTrace();
		} catch (Throwable e) {
		    e.printStackTrace();
		}

	}

}
