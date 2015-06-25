package com.lin.test;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lin.coprocessor.SelectCoprocessorImpl;

public class SelectCoprocessorImplTest {
	Configuration conf = null;
	HTable table = null;
	@Before
	public void setUp(){
		// Define a table
		conf.set("hbase.zookeeper.quorum", "HB");
		try {
			table = new HTable(conf,"bt1");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void test() {
		Exception exception = null;
		try {
			// Get the location of the JAR file containing the coprocessor implementation.
			FileSystem fs = FileSystem.get(conf);
			Path path =  new Path(fs.getUri()+Path.SEPARATOR+"BSVM.jar");
			
			// Define a table descriptor
			HTableDescriptor htd = new HTableDescriptor("bt1");
			htd.addFamily(new HColumnDescriptor("colfam1"));
			// Add the coprocessor definition to the descriptor.
			htd.setValue("COPROCESSOR$1", path.toString()+
					"|"+SelectCoprocessorImpl.class.getCanonicalName()+
					"|"+Coprocessor.PRIORITY_USER);
			
			// Instantiate an administrative API to the cluster and add the table.
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.createTable(htd);
			
			// Verify if the definition has been applied as expected.
			System.out.println(admin.getTableDescriptor(Bytes.toBytes("bt1")));
			
		} catch (IOException e) {
			exception = e;
			e.printStackTrace();
		}
		
		if(exception != null){
			fail("Exception not empty!");
		}
	}
	
	@After
	public void tearDown(){
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
