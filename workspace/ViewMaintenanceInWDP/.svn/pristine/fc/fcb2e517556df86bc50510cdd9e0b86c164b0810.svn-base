package de.webdataplatform.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.BytesUtil;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.view.TableService;

public class TestIndex {

	/**
	 * @param args
	 */
	public static void main(String[] args) {


		Log log = new Log("TestClient");
		SystemConfig.load(log);
		NetworkConfig.load(log);
		DatabaseConfig.load(log);
		EvaluationConfig.load(log);
		
		TableService ts = new TableService(log);
		
		
//		Map<byte[], byte[]> map = ts.get(Bytes.toBytes("min1_index"), Bytes.toBytes("x095"), new ArrayList<byte[]>());
		
//		System.out.println(BytesUtil.mapToString(BytesUtil.eliminateSignatures(map)));
		
//		long startTime = new Date().getTime();
//		for (byte[] key : BytesUtil.eliminateSignatures(map).keySet()) {
//		
//			Map<byte[], byte[]> minKeys = ts.get(Bytes.toBytes("bt1"), key, new ArrayList<byte[]>());
//			System.out.println(BytesUtil.mapToString(BytesUtil.eliminateSignatures(minKeys)));
//		}
	
		long endTime = new Date().getTime();
//		System.out.println("Time to get results: "+(endTime - startTime));
		
		
//		startTime = new Date().getTime();
//		Integer scanResult = ts.scanMinimum("bt1", "colAggKey", "colAggVal", "x095");
		endTime = new Date().getTime();
		
//		System.out.println("scanResult: "+scanResult);

//		System.out.println("Time to get results: "+(endTime - startTime));
		
	}
	

}
