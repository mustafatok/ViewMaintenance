package com.lin.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HelloCoprocessorTest {
	HTable testTable;

	@Before
	public void setUp() throws Exception {
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "HB");

        try {
			testTable = new HTable(config, "users");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        // set coprocessor
        FileSystem fs = null;
		try {
			fs = FileSystem.get(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        // get location of jar file
        Path path = new Path(fs.getUri()+Path.SEPARATOR+"user"+Path.SEPARATOR+"jeff"+Path.SEPARATOR+"HelloCoprocessor.jar");
        // define a table descriptor
        HTableDescriptor htd = new HTableDescriptor("users");
        htd.addFamily(new HColumnDescriptor("colfam1"));
        htd.setValue("coprocessor1", "hdfs:///user/jeff/HelloCoprocessor.jar|"+HelloCoprocessor.class.getCanonicalName()+"|"+Coprocessor.PRIORITY_USER);
        HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			admin.modifyTable("users", htd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
			System.out.println(admin.getTableDescriptor(Bytes.toBytes("users")));
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@After
	public void tearDown() throws Exception {
		testTable.close();
	}

	@Test
	public void test(){
		Exception ex =null;
		
		Get get = new Get(Bytes.toBytes("jeff"));
		try {
			Result result = testTable.get(get);
			System.out.println("get result is "+result.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		
		assert(ex == null);
	}

}
