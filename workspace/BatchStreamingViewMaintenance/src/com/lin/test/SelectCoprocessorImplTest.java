package com.lin.test;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
	public void setUp() {
		try {
			// Define a table
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "HB");
			table = new HTable(conf, "bt1");

			// Get the location of the JAR file containing the coprocessor
			// implementation.
			FileSystem fs = FileSystem.get(conf);

			// disable table
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable("bt1");
			// Define a table descriptor
			HTableDescriptor htd = table.getTableDescriptor();
			// htd.addFamily(new HColumnDescriptor("colfam1"));
			// Add the coprocessor definition to the descriptor.
			htd.setValue("COPROCESSOR$1", "/BSVM.jar" + "|"
					+ SelectCoprocessorImpl.class.getCanonicalName() + "|"
					+ Coprocessor.PRIORITY_USER);

			// Instantiate an administrative API to the cluster and add the
			// table.
			admin.modifyTable("bt1", htd);
			admin.enableTable("bt1");

			// Verify if the definition has been applied as expected.
			System.out.println(admin.getTableDescriptor(Bytes.toBytes("bt1")));

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	@Test
	public void test() {
		Exception exception = null;

		if (exception != null) {
			fail("Exception not empty!");
		}
	}

	@After
	public void tearDown() {
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
