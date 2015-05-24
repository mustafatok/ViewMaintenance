package com.gzhdi.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.gzhdi.coprocessor.generated.ServerHelloworld.AnsResponse;
import com.gzhdi.coprocessor.generated.ServerHelloworld.AskRequest;
import com.gzhdi.coprocessor.generated.ServerHelloworld.HelloRequest;
import com.gzhdi.coprocessor.generated.ServerHelloworld.HelloResponse;
import com.gzhdi.coprocessor.generated.ServerHelloworld.HelloWorld;

public class HelloWorldEndPoint extends HelloWorld implements Coprocessor,
		CoprocessorService {

	private RegionCoprocessorEnvironment env;

	@Override
	public void sendHello(RpcController controller, HelloRequest request,
			RpcCallback<HelloResponse> done) {
		System.out.println("request HelloRequest:" + request.getAskWord());
		HelloResponse resp = HelloResponse.newBuilder()
				.setRetWord(ByteString.copyFromUtf8("hello world!!!")).build();

		done.run(resp);
	}

	@Override
	public void question(RpcController controller, AskRequest request,
			RpcCallback<AnsResponse> done) {
		System.out.println("request question:" + request.getAsk());
		AnsResponse resp = AnsResponse
				.newBuilder()
				.setAns(ByteString.copyFromUtf8("helloworld,"
						+ request.getAsk().toStringUtf8())).build();
		done.run(resp);
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {

	}

}
