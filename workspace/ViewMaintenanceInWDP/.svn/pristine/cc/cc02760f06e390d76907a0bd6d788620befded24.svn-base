package de.webdataplatform.test;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import de.webdataplatform.storage.BaseRecord;
import de.webdataplatform.storage.Update;
import de.webdataplatform.view.IViewRecord;
import de.webdataplatform.view.ViewRecord;

public class HBaseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.127.129");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		conf.set("hbase.regionserver.port","60020");
//		conf.set("hbase.master", "192.168.127.129:60000");
		
		
//		  configStore.add(new Update<BaseTableRecord>("k1", new BaseTableRecord("x1", 100)));
//		  configStore.add(new Update<BaseTableRecord>("k2", new BaseTableRecord("x1", 200)));
//		  configStore.add(new Update<BaseTableRecord>("k3", new BaseTableRecord("x2", 300)));
//		  configStore.add(new Update<BaseTableRecord>("k4", new BaseTableRecord("x2", 400)));
		
//		updates.add(new Update<BaseTableRecord>("k2", new BaseTableRecord("x2", 200)));
//		updates.add(new Update<BaseTableRecord>("k4", new BaseTableRecord("x1", 400)));
//		
//		updates.add(new Update<BaseTableRecord>("k5", new BaseTableRecord("x1", 400)));
//		updates.add(new Update<BaseTableRecord>("k6", new BaseTableRecord("x2", 200)));
//		updates.add(new Update<BaseTableRecord>("k7", new BaseTableRecord("x3", 350)));
//		updates.add(new Update<BaseTableRecord>("k8", new BaseTableRecord("x3", 600)));
//		updates.add(new Update<BaseTableRecord>("k9", new BaseTableRecord("x4", 1200)));
		
		
//		initializeBaseTable(conf);
//		
//		updateBaseTable(conf);
		
//		updateTestTable(conf);
		
		 createNewTable();
		
//		scanBaseTable(conf);
		
//		deleteBaseTable(conf);
//		
//		initializeViewTableAggregation(conf);
//		
//		getViewTableAggregation(conf);

		System.out.println("fertig");
	}
	
	
	
	

	private static void initializeBaseTable(Configuration conf) throws IOException {
		
		HTable table = new HTable(conf, "basetable");
		
		Put put = new Put(Bytes.toBytes("k1"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x1"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(100));

		table.put(put);
		
		
		put = new Put(Bytes.toBytes("k2"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x1"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(200));

		table.put(put);
		
		put = new Put(Bytes.toBytes("k3"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(300));

		table.put(put);
		
		put = new Put(Bytes.toBytes("k4"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(400));		
		
		table.put(put);
		
	}
	

	private static void getViewTableAggregation(Configuration conf) throws IOException {
		HTable table = new HTable(conf, "viewtable_aggregation");
		
		
		Get get = new Get(Bytes.toBytes("x4"));
		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"));	
		Result result = table.get(get);
		byte[] val = result.getValue(Bytes.toBytes("colfam1"),
		Bytes.toBytes("aggregatedValue"));
		System.out.println("Value: " + Bytes.toInt(val));


		get = new Get(Bytes.toBytes("x5"));
		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"));	
		result = table.get(get);
		val = result.getValue(Bytes.toBytes("colfam1"),Bytes.toBytes("aggregatedValue"));
		System.out.println("Value: " + Bytes.toInt(val));


	}
	
	private static void initializeViewTableAggregation(Configuration conf) throws IOException {
		HTable table = new HTable(conf, "viewtable_aggregation");
		
		
		Put put = new Put(Bytes.toBytes("x7"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes(100));	
		table.put(put);

		put = new Put(Bytes.toBytes("x8"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes(300));	
		table.put(put);		

	}
	
//	private static void initializeViewTableProjection(Configuration conf) throws IOException {
//		HTable table = new HTable(conf, "viewtable_projection");
//		
//		  configView.add(new Update<IViewTableRecord>("x1", new ViewTableRecordImpl(100)));
//		  configView.add(new Update<IViewTableRecord>("x2", new ViewTableRecordImpl(300)));
//		
//		
//		Put put = new Put(Bytes.toBytes("x1"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes("x1"));	
//
//
//		table.put(put);
//		
//		put = new Put(Bytes.toBytes("k2"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x1"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(200));
//
//		table.put(put);
//		
//		put = new Put(Bytes.toBytes("k3"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(300));
//
//		table.put(put);
//		
//		put = new Put(Bytes.toBytes("k4"));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(400));		
//		
//		table.put(put);
//	}
	
	private static void printTableRegions(Configuration conf, String tableName){
		
		
		System.out.println("Printing regions of table: " + tableName);
		
		HTable table=null;
		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		Pair<byte[][], byte[][]> pair=null;
		try {
			pair = table.getStartEndKeys();
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		for (int n = 0; n < pair.getFirst().length; n++) {
			
		byte[] sk = pair.getFirst()[n];
		
		byte[] ek = pair.getSecond()[n];
		
		System.out.println("[" + (n + 1) + "]" +
		" start key: " +
		(sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
		", end key: " +
		(ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
		}
		
		
	}
	/*
	public void deleteTable() throws IOException {
		
		System.out.println("-----------------------");
		System.out.println("delete table "+tableName);
		System.out.println("-----------------------");
		
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		admin.disableTable(Bytes.toBytes(tableName));
		boolean isDisabled = admin.isTableDisabled(Bytes.toBytes(tableName));
		System.out.println("Table is disabled: " + isDisabled);
		
		boolean avail1 = admin.isTableAvailable(Bytes.toBytes(tableName));
		System.out.println("Table available: " + avail1);
		
		try {
			
			admin.deleteTable(Bytes.toBytes(tableName));
		
		} catch (IOException e) {
			
			System.err.println("Error deleting table: " + e.getMessage());
		
		}
		
		admin.close();
		
		

		

	}
	
	public void createTable() throws IOException {

		System.out.println("-----------------------");
		System.out.println("create table "+tableName);
		System.out.println("-----------------------");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
		
		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(colfam));
		
		desc.addFamily(coldef);
		
		admin.createTable(desc);
		
		boolean avail = admin.isTableAvailable(Bytes.toBytes(tableName));
		
//		admin.enableTable(Bytes.toBytes(tableName));
		
		System.out.println("Table available: " + avail);
		System.out.println("Table enabled: " + admin.isTableEnabled(Bytes.toBytes(tableName)));
		
		admin.close();
		

	}*/
	
	private static void createNewTable(){
		
		
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.26.135");
		
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {

			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
	
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes("viewtable1"));
		
		HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
		
		desc.addFamily(coldef);
		
		try {
			admin.createTable(desc, Bytes.toBytes(1L), Bytes.toBytes(100L), 10);
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		
		printTableRegions(conf, "viewtable1");
		
		
		/*byte[][] regions = new byte[][] {
		Bytes.toBytes("A"),
		Bytes.toBytes("D"),
		Bytes.toBytes("G"),
		Bytes.toBytes("K"),
		Bytes.toBytes("O"),
		Bytes.toBytes("T")
		};
		desc.setName(Bytes.toBytes("testtable2"));
		try {
			admin.createTable(desc, regions);
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		printTableRegions(conf, "testtable2");*/
		
		
		
	}
	
	
	
	private static void updateTestTable(Configuration conf) throws IOException {

		HTable table = new HTable(conf, "testtable1");

		
		Put put=null;
		for (int i = 0; i < 150; i++) {
			
			
			int random = (int) (Math.random()*100+1);
			System.out.println("i:"+i+"random:"+random);
			put = new Put(Bytes.toBytes(i));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x"+i));	

			table.put(put);
		}
		
		table.flushCommits();
		

	}
	
	private static void updateBaseTable(Configuration conf) throws IOException {
		HTable table = new HTable(conf, "basetable");
		
		Put put = new Put(Bytes.toBytes("k2"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(200));

		table.put(put);
		
		put = new Put(Bytes.toBytes("k4"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x1"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(400));

		table.put(put);
		
		put = new Put(Bytes.toBytes("k5"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x1"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(400));

		table.put(put);
		
		put = new Put(Bytes.toBytes("k6"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x2"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(200));		
		
		table.put(put);
		
		put = new Put(Bytes.toBytes("k7"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x3"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(350));		
		
		table.put(put);
		put = new Put(Bytes.toBytes("k8"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x3"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(600));		
		
		table.put(put);
		put = new Put(Bytes.toBytes("k9"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey"), Bytes.toBytes("x4"));	
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"), Bytes.toBytes(1200));		
		
		table.put(put);
	}
	
	private static void deleteBaseTable(Configuration conf) throws IOException {
		HTable table = new HTable(conf, "basetable");
		
		Delete delete = new Delete(Bytes.toBytes("k1"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k2"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k3"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k4"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k5"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k6"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k7"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k8"));
		table.delete(delete);
		
		delete = new Delete(Bytes.toBytes("k9"));
		table.delete(delete);
		

	}

	private static void scanBaseTable(Configuration conf) throws IOException {
		
		HTable table = new HTable(conf, "basetable");	
	
		Scan scan1 = new Scan();
		
		ResultScanner scanner1 = table.getScanner(scan1);
		for (Result res : scanner1) {
			System.out.print("Key: "+Bytes.toString(res.getRow())+",  ");
			System.out.print("AggKey: "+Bytes.toString(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationKey")))+",  ");
			System.out.print("AggValue: "+Bytes.toInt(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregationValue"))));
			System.out.println();
//			System.out.println(res);
		}
		scanner1.close();
		
	}

}
