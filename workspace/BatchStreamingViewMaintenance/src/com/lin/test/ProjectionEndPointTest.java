package com.lin.test;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.lin.coprocessor.generated.ProjectionProtos.Projection;
import com.lin.coprocessor.generated.ProjectionProtos.ProjectionRequest;
import com.lin.coprocessor.generated.ProjectionProtos.ProjectionResponse;

public class ProjectionEndPointTest {

	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		TableName tableName = TableName.valueOf("testtable");
		// ^^ EndpointExample
		HBaseAdmin admin = null;
		HBaseHelper helper;
		try {
			/*helper = HBaseHelper.getHelper(conf);
			helper.dropTable("testtable");
			helper.createTable("testtable", "colfam1", "colfam2");
			helper.put("testtable", new String[] { "row1", "row2", "row3",
					"row4", "row5" }, new String[] { "colfam1", "colfam2" },
					new String[] { "qual1", "qual1" }, new long[] { 1, 2 },
					new String[] { "val1", "val2" });
			System.out.println("Before endpoint call...");
			helper.dump("testtable", new String[] { "row1", "row2", "row3",
					"row4", "row5" }, null, null);
			admin = new HBaseAdmin(conf);
			admin.split("testtable", "row3");
			// wait for the split to be done
			while (admin.getTableRegions(tableName).size() < 2)
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}*/

			HTable table = new HTable(conf, "bt1");
			ProjectionRequest request = ProjectionRequest.newBuilder().setFamily(ByteString.copyFrom(Bytes.toBytes("colfam1"))).setColumn(ByteString.copyFrom(Bytes.toBytes("colAggKey"))).build();
			Map<byte[], ProjectionResponse> results = table
					.batchCoprocessorService(Projection.getDescriptor()
							.findMethodByName("sendProjection"), request,
							HConstants.EMPTY_START_ROW,
							HConstants.EMPTY_END_ROW, ProjectionResponse
									.getDefaultInstance());

			long total = 0;
			for (Map.Entry<byte[], ProjectionResponse> entry : results
					.entrySet()) {
				ProjectionResponse response = entry.getValue();
				total += response.getResultsCount();
				System.out.println("Region: " + Bytes.toString(entry.getKey())
						+ ", Count: " + response.getResultsCount());

			}
			System.out.println("Total Count: " + total);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}

}
