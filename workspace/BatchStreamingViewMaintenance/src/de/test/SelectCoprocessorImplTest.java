package de.test;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.lin.coprocessor.generated.SelectCoprocessor.Select;
import com.lin.coprocessor.generated.SelectCoprocessor.SelectRequest;
import com.lin.coprocessor.generated.SelectCoprocessor.SelectResponse;
import com.lin.coprocessor.generated.SumCoprocessor.Sum;
import com.lin.coprocessor.generated.SumCoprocessor.SumResponse;

public class SelectCoprocessorImplTest {

	@Test
	public void test() {
		HTable testTable;
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "HB");

		final SelectRequest req = SelectRequest.newBuilder()
				.setFamily(ByteString.copyFromUtf8("bt1")).setProjection(ByteString.copyFromUtf8("colAggKey")).build();
		
		SelectResponse resp = null;
		try {
			HTable table = new HTable(config, "bt1");
			Map<byte[], ByteString> re = table.coprocessorService(
					Select.class, null, null,
					new Batch.Call<Select, ByteString>() {

						@Override
						public ByteString call(Select instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<SelectResponse> rpccall = new BlockingRpcCallback<SelectResponse>();
							instance.performSelect(controller, req, rpccall);
							SelectResponse resp = rpccall.get();
							
							System.out.println("there are "+resp.getResultRowsCount()+" results");
							
							// result
							System.out.println("resp:"
									+ resp);

							return ByteString.copyFromUtf8(resp+"");
						}

						

					});
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ServiceException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}
