package com.lin.test;

import static org.junit.Assert.*;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVColumn;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Execute;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ParameterMessage;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ResultMessage;
import com.lin.coprocessor.generated.ProjectionProtos.Projection;
import com.lin.coprocessor.generated.ProjectionProtos.ProjectionRequest;
import com.lin.coprocessor.generated.ProjectionProtos.ProjectionResponse;

public class BSVTest {

	@Test
	public void test() {
		Configuration conf = HBaseConfiguration.create();
		TableName tableName = TableName.valueOf("testtable1");
		HBaseAdmin admin = null;
		HBaseHelper helper;
		try {
			helper = HBaseHelper.getHelper(conf);
			helper.dropTable("testtable1");
			helper.createTable("testtable1", "colfam1", "colfam2");
			helper.put("testtable1", new String[] { "row1", "row2", "row3",
					"row4", "row5" }, new String[] { "colfam1", "colfam2" },
					new String[] { "qual1", "qual1" }, new long[] { 1, 2 },
					new String[] { "val1", "val2" });
			System.out.println("Before endpoint call...");
			helper.dump("testtable1", new String[] { "row1", "row2", "row3",
					"row4", "row5" }, null, null);
			admin = new HBaseAdmin(conf);
			admin.split("testtable1", "row3");
			// wait for the split to be done
			while (admin.getTableRegions(tableName).size() < 2)
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}

			HTable table = new HTable(conf, "testtable1");
			ParameterMessage.Builder request = ParameterMessage.newBuilder();
			BSVColumn.Builder bsvColumn = BSVColumn.newBuilder();
			bsvColumn.setFamily(ByteString.copyFrom(Bytes.toBytes("colfam1")));
			bsvColumn.setColumn(ByteString.copyFrom(Bytes.toBytes("qual1")));
			request.addColumn(bsvColumn);
			Map<byte[], ResultMessage> results = table.batchCoprocessorService(
					Execute.getDescriptor().findMethodByName("batch"),
					request.build(), HConstants.EMPTY_START_ROW,
					HConstants.EMPTY_END_ROW,
					ResultMessage.getDefaultInstance());

			long total = 0;
			for (Map.Entry<byte[], ResultMessage> entry : results
					.entrySet()) {
				ResultMessage response = entry.getValue();
				total += response.getSize();
				System.out.println("Region: " + Bytes.toString(entry.getKey())
						+ ", Count: " + response.getSize());
				System.out.println(response.toString());
			}
			System.out.println("Total Count: " + total);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}

}
