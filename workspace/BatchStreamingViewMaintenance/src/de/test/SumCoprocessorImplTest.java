package de.test;

import static org.junit.Assert.*;

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
import com.lin.coprocessor.generated.SumCoprocessor.Sum;
import com.lin.coprocessor.generated.SumCoprocessor.SumRequest;
import com.lin.coprocessor.generated.SumCoprocessor.SumResponse;

public class SumCoprocessorImplTest {

	@Test
	public void test() {
		HTable testTable;
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "HB");

		final SumRequest req = SumRequest.newBuilder()
				.setFamily(ByteString.copyFromUtf8("f1")).setColumn(ByteString.copyFromUtf8("score")).build();
		
		SumResponse resp = null;
		try {
			HTable table = new HTable(config, "t1");
			Map<byte[], ByteString> re = table.coprocessorService(
					Sum.class, null, null,
					new Batch.Call<Sum, ByteString>() {

						@Override
						public ByteString call(Sum instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<SumResponse> rpccall = new BlockingRpcCallback<SumResponse>();
							instance.getSum(controller, req, rpccall);
							SumResponse resp = rpccall.get();

							// result
							System.out.println("resp:"
									+ resp.getSum());

							return ByteString.copyFromUtf8(resp.getSum()+"");
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
