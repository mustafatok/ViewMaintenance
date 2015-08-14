package com.lin.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVColumn;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Execute;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ParameterMessage;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.ResultMessage;

public class BSVTest {

	/* test 1 */
//	public static void main(String[] args) {
//		Configuration conf = HBaseConfiguration.create();
//		TableName tableName = TableName.valueOf("testtable1");
//		HBaseAdmin admin = null;
//		HBaseHelper helper;
//		try {
//			helper = HBaseHelper.getHelper(conf);
//			helper.dropTable("testtable1");
//			helper.createTable("testtable1", "colfam1", "colfam2");
//			
//			String[] rows = {};
//			List<String> stringArray = new ArrayList<String>();
//			for(int i = 1; i <= 100; i++){
//				stringArray.add("row" + i);
//			}
//			rows = stringArray.toArray(new String[0]);
//			helper.put("testtable1", 
//					rows, 
//					new String[] { "colfam1", "colfam2" },
//					new String[] { "qual1", "qual2" }, 
//					new long[] { 1, 2 },
//					new String[] { "1", "2" });
//			System.out.println("Before endpoint call...");
//			helper.dump("testtable1", new String[] { "row1", "row2", "row3",
//					"row4", "row5" }, null, null);
//			admin = new HBaseAdmin(conf);
//			/*admin.split("testtable1", "row3");
//			// wait for the split to be done
//			while (admin.getTableRegions(tableName).size() < 2)
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//				}*/
//
//			HTable table = new HTable(conf, "testtable1");
//			ParameterMessage.Builder request = ParameterMessage.newBuilder();
//			BSVColumn.Builder bsvColumn = BSVColumn.newBuilder();
//			bsvColumn.setFamily(ByteString.copyFrom(Bytes.toBytes("colfam1")));
//			bsvColumn.setColumn(ByteString.copyFrom(Bytes.toBytes("qual1")));
//			request.addColumn(bsvColumn);
//			System.out.println("=======================================================================");
//			Date begin = new Date();
//			System.out.println(begin + " Beging to execute batch job");
//			Map<byte[], ResultMessage> results = table.batchCoprocessorService(
//					Execute.getDescriptor().findMethodByName("batch"),
//					request.build(), HConstants.EMPTY_START_ROW,
//					HConstants.EMPTY_END_ROW,
//					ResultMessage.getDefaultInstance());
//			Date end = new Date();
//			System.out.println(end + " Finish batch job in " + (end.getTime() - begin.getTime()) + " miliseconds");
//			long total = 0;
//			for (Map.Entry<byte[], ResultMessage> entry : results
//					.entrySet()) {
//				ResultMessage response = entry.getValue();
//				total += response.getSize();
//				System.out.println("Region: " + Bytes.toString(entry.getKey())
//						+ ", Count: " + response.getSize());
//				System.out.println(response.toString());
//			}
//			System.out.println("Total Count: " + total);
//		} catch (Throwable throwable) {
//			throwable.printStackTrace();
//		}
//	}
	
	// test 2
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper;
		try {
			helper = HBaseHelper.getHelper(conf);
			helper.dropTable("testtable2");
			helper.createTable("testtable2", "colfam1");
			
			String[] rows = {};
			List<String> stringArray = new ArrayList<String>();
			for(int i = 1; i <= 100; i++){
				helper.put("testtable2", "row"+i, "colfam1", "qual1", 1, "val"+i);
			}
			
			System.out.println("Before endpoint call...");
			

			HTable table = new HTable(conf, "testtable2");
			ParameterMessage.Builder request = ParameterMessage.newBuilder();
			BSVColumn.Builder bsvColumn = BSVColumn.newBuilder();
			bsvColumn.setFamily(ByteString.copyFrom(Bytes.toBytes("colfam1")));
			bsvColumn.setColumn(ByteString.copyFrom(Bytes.toBytes("qual1")));
			request.addColumn(bsvColumn);
			System.out.println("=======================================================================");
			Date begin = new Date();
			System.out.println(begin + " Beging to execute batch job");
			Map<byte[], ResultMessage> results = table.batchCoprocessorService(
					Execute.getDescriptor().findMethodByName("batch"),
					request.build(), HConstants.EMPTY_START_ROW,
					HConstants.EMPTY_END_ROW,
					ResultMessage.getDefaultInstance());
			Date end = new Date();
			System.out.println(end + " Finish batch job in " + (end.getTime() - begin.getTime()) + " miliseconds");
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
