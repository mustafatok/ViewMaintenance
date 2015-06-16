package com.lin.test;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.gzhdi.coprocessor.generated.ServerHelloworld.AnsResponse;
import com.gzhdi.coprocessor.generated.ServerHelloworld.AskRequest;
import com.gzhdi.coprocessor.generated.ServerHelloworld.HelloWorld;
import com.lin.coprocessor.generated.SumCoprocessor.Sum;
import com.lin.coprocessor.generated.SumCoprocessor.SumRequest;
import com.lin.coprocessor.generated.SumCoprocessor.SumResponse;

public class MainTest {

	public static void main(String[] args) {
		
	}

}
