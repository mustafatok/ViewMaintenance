package com.lin.test;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import com.lin.coprocessor.generated.RowCounterProtos;
import com.lin.utils.HBaseHelper;

public class EndpointExample {
	public static void main(String[] args) throws IOException {
	    Configuration conf = HBaseConfiguration.create();
	    TableName tableName = TableName.valueOf("testtable");
//	    Connection connection = ConnectionFactory.createConnection(conf);
	    // ^^ EndpointExample
	    HBaseHelper helper = HBaseHelper.getHelper(conf);
	    helper.dropTable("testtable");
	    helper.createTable("testtable", "colfam1", "colfam2");
	    helper.put("testtable",
	      new String[]{"row1", "row2", "row3", "row4", "row5"},
	      new String[]{"colfam1", "colfam2"},
	      new String[]{"qual1", "qual1"},
	      new long[]{1, 2},
	      new String[]{"val1", "val2"});
	    System.out.println("Before endpoint call...");
	    helper.dump("testtable",
	      new String[]{"row1", "row2", "row3", "row4", "row5"},
	      null, null);
	    // get table for connection
	    HTable table = new HTable(conf,"testtable");
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    try {
	      admin.split(tableName.toBytes(), Bytes.toBytes("row3"));
	    } catch (IOException e) {
	      e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    // wait for the split to be done
	    while (admin.getTableRegions(tableName).size() < 2)
	      try {
	        Thread.sleep(1000);
	      } catch (InterruptedException e) {
	      }
	    //vv EndpointExample
//	    Table table = connection.getTable(tableName);
	    try {
	      final RowCounterProtos.CountRequest request =
	        RowCounterProtos.CountRequest.getDefaultInstance();
	      Map<byte[], Long> results = table.coprocessorService(
	        RowCounterProtos.RowCountService.class, 
	        null, null, 
	        new Batch.Call<RowCounterProtos.RowCountService, Long>() { // co EndpointExample-3-Batch Create an anonymous class to be sent to all region servers.
	          public Long call(RowCounterProtos.RowCountService counter)
	          throws IOException {
	            BlockingRpcCallback<RowCounterProtos.CountResponse> rpcCallback =
	              new BlockingRpcCallback<RowCounterProtos.CountResponse>();
	            counter.getRowCount(null, request, rpcCallback); // co EndpointExample-4-Call The call() method is executing the endpoint functions.
	            RowCounterProtos.CountResponse response = rpcCallback.get();
	            return response.hasCount() ? response.getCount() : 0;
	          }
	        }
	      );

	      long total = 0;
	      for (Map.Entry<byte[], Long> entry : results.entrySet()) { // co EndpointExample-5-Print Iterate over the returned map, containing the result for each region separately.
	        total += entry.getValue().longValue();
	        System.out.println("Region: " + Bytes.toString(entry.getKey()) +
	          ", Count: " + entry.getValue());
	      }
	      System.out.println("Total Count: " + total);
	    } catch (Throwable throwable) {
	      throwable.printStackTrace();
	    }
	  }
}
