package de.webdataplatform.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.ColumnDefinition;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.settings.TableDefinition;

public class TestVersioning {

	/**
	 * @param args
	 */
	
	
	private Configuration conf;
	
	private Log log;

	
	
	public TestVersioning(Log log){

		SystemConfig.load(log);
		NetworkConfig.load(log);
		EvaluationConfig.load(log);
		
		conf = NetworkConfig.getHBaseConfiguration(log);
		
		this.log = log;

//		StatisticLog.name = "clientoperations";
		

	}
	
	
	public static void main(String[] args) {
		
		Log log = new Log("werw.log");
		
		TestVersioning testVersioning = new TestVersioning(log);
		
		
		try {
//			testVersioning.recreateTable("bt1");
			testVersioning.fillBaseTable("bt1", 5);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}
	

	
	
	public void deleteTable(String tableName) throws IOException {
		
		log.info(TestVersioning.class,"-----------------------");
		log.info(TestVersioning.class,"delete table "+tableName);
		log.info(TestVersioning.class,"-----------------------");
		
		
		HBaseAdmin admin = new HBaseAdmin(conf);
	
		try {
		
			admin.disableTable(Bytes.toBytes(tableName));
			boolean isDisabled = admin.isTableDisabled(Bytes.toBytes(tableName));
			log.info(TestVersioning.class,"Table is disabled: " + isDisabled);
			
			boolean avail1 = admin.isTableAvailable(Bytes.toBytes(tableName));
			log.info(TestVersioning.class,"Table available: " + avail1);
		
			
			admin.deleteTable(Bytes.toBytes(tableName));
		
		} catch (Exception e) {
			
			log.error(TestVersioning.class, e);
		
		}
		
		admin.close();

		
		

		

	}
	
	public void createTable(String tableName) throws IOException {

		
		log.info(TestVersioning.class,"-----------------------");
		log.info(TestVersioning.class,"create table "+tableName);
		log.info(TestVersioning.class,"-----------------------");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		try {
			
			
			HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
			
			HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("colfam1"));
			
			coldef.setMaxVersions(5);
			coldef.setKeepDeletedCells(true);
			
			desc.addFamily(coldef);
			
			desc.setDeferredLogFlush(false);
			
	//		desc.set
			
			admin.createTable(desc);
			
			boolean avail = admin.isTableAvailable(Bytes.toBytes(tableName));
			
	//		admin.enableTable(Bytes.toBytes(tableName));
			
			log.info(TestVersioning.class,"Table available: " + avail);
			log.info(TestVersioning.class,"Table enabled: " + admin.isTableEnabled(Bytes.toBytes(tableName)));
			
		} catch (Exception e) {
			
			log.error(TestVersioning.class, e);
		
		}
		
		admin.close();
		

	}
	
	public void recreateTable(String tableName) throws IOException{
		
		try {
			deleteTable(tableName);
		} catch (IOException e) {
			log.error(this.getClass(), e);
		}
		
		createTable(tableName);
		
		
	}
	

	

	
	


	
	public void fillBaseTable(String baseTableName, int numOfOperations)throws Exception{
		
		log.info(TestVersioning.class,"-----------------------");
		log.info(TestVersioning.class,"fill table:"+baseTableName);
		log.info(TestVersioning.class,"-----------------------");
		
		log.info(TestVersioning.class, "numOfOperations: "+numOfOperations);
		
	



		HTable baseTable = new HTable(conf, baseTableName);
		/*
		for (long i = 0; i < numOfOperations; i++) {
			
			log.info(TestVersioning.class, "putting entry: ");
			Put put = new Put(Bytes.toBytes("k0001"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x"+numOfOperations));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes(numOfOperations+""));
			baseTable.put(put);

		}
		
		Delete delete = new Delete(Bytes.toBytes("k0001"));
		baseTable.delete(delete);
		
		Put put = new Put(Bytes.toBytes("k0001"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x11"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes("11"));
		baseTable.put(put);
		
		delete = new Delete(Bytes.toBytes("k0001"));
		baseTable.delete(delete);
		
		put = new Put(Bytes.toBytes("k0001"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x12"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes("12"));
		baseTable.put(put);
		
		put = new Put(Bytes.toBytes("k0001"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"), Bytes.toBytes("x13"));
		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggVal"), Bytes.toBytes("13"));
		baseTable.put(put);
		
		*/
		

		Get get = new Get(Bytes.toBytes("k0381"));
//		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("aggKey"));
//		get.setMaxVersions();
//		get.setTimeStamp(1417171365574l);
		get.setTimeRange(0, 1417171354198l);
		Result result = baseTable.get(get);
	
//		for (Cell cell2 : result.rawCells()) {
//			System.out.println(cell2.toString());
//			System.out.println(cell2.getTimestamp());
//		}

		
		System.out.println("-------------------------");
		List<Cell> cells = result.getColumnCells(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggKey"));
		
		for (Cell cell2 : cells) {
			System.out.println(cell2.toString());
			System.out.println(cell2.getTimestamp());
			System.out.println(Bytes.toString(cell2.getValue()));
		}
		
		System.out.println("-------------------------");
		cells = result.getColumnCells(Bytes.toBytes("colfam1"), Bytes.toBytes("colAggVal"));
		
		for (Cell cell2 : cells) {
			System.out.println(cell2.toString());
			System.out.println(cell2.getTimestamp());
			System.out.println(Bytes.toString(cell2.getValue()));
		}
		
		baseTable.close();


	}






	private Put generatePut(TableDefinition tableDefinition, String rowKey) {

		Put put = new Put(Bytes.toBytes(rowKey));
		
		for (ColumnDefinition columnDefinition : tableDefinition.getColumns()) {
			
			
			int digits = String.valueOf(columnDefinition.getEndRange()).length();
			
			int zahl = (int)(columnDefinition.getStartRange()+(Math.random() * columnDefinition.getNumOfValues() + 1));
			
			String value="";
			if(columnDefinition.getPrefix() != null)value = columnDefinition.getPrefix();
			
			for(int x = 0; x < (digits - String.valueOf(zahl).length());x++)value+="0";
			value+=zahl;

			
			put.add(Bytes.toBytes(columnDefinition.getFamily()), Bytes.toBytes(columnDefinition.getName()), Bytes.toBytes(value));	
			
		}
		

		return put;
	}
	
	


	private Delete generateDelete(String rowKey) {
		
		
		
		Delete delete = new Delete(Bytes.toBytes(rowKey));

		

		return delete;
	}	

	




}
