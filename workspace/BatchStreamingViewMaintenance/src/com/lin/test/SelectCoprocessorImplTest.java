package com.lin.test;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.lin.coprocessor.SelectCoprocessorImpl;
import com.lin.coprocessor.generated.SelectCoprocessor.Select;
import com.lin.coprocessor.generated.SelectCoprocessor.SelectRequest;
import com.lin.coprocessor.generated.SelectCoprocessor.SelectResponse;

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

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "HB");

		final SelectRequest req = SelectRequest.newBuilder()
				.setFamily(ByteString.copyFromUtf8("bt1"))
				.setProjection(ByteString.copyFromUtf8("colAggKey")).build();

		try {
			Map<byte[], ByteString> re = table.coprocessorService(Select.class,
					null, null, new Batch.Call<Select, ByteString>() {

						@Override
						public ByteString call(Select instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<SelectResponse> rpccall = new BlockingRpcCallback<SelectResponse>();
							instance.performSelect(controller, req, rpccall);
							SelectResponse resp = rpccall.get();

							System.out.println("there are "
									+ resp.getResultRowsCount() + " results");

							// result
							System.out.println("resp:" + resp);

							return ByteString.copyFromUtf8(resp + "");
						}
					});
			// TODO combine 
		} catch (IOException e) {
			e.printStackTrace();
			exception = e;
		} catch (ServiceException e) {
			e.printStackTrace();
			exception = e;
		} catch (Throwable e) {
			e.printStackTrace();
		}

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
