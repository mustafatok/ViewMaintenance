package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

public class BytesUtil {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static Map<byte[], byte[]> convertMap(Map<String, String> inputMap){
		
		Map<byte[], byte[]> outputMap = new HashMap<byte[], byte[]>();
		
		
		for (String key : inputMap.keySet()) {
			outputMap.put(Bytes.toBytes(key), Bytes.toBytes(inputMap.get(key)));
		}
		return outputMap;
		
	}

	public static Map<String, String> convertMapBack(Map<byte[], byte[]> inputMap){
		
		Map<String, String> outputMap = new HashMap<String, String>();
		
		if(inputMap == null)return outputMap;
		
		for (byte[] key : inputMap.keySet()) {
			outputMap.put(Bytes.toString(key), Bytes.toString(inputMap.get(key)));
		}
		return outputMap;
		
	}
	
	public static List<byte[]> convertList(List<String> inputList){
		
		List<byte[]> outputList = new ArrayList<byte[]>();
		
		
		for (String item : inputList) {
			outputList.add(Bytes.toBytes(item));
		}
		return outputList;
		
	}
	
	public static List<String> convertListBack(List<byte[]> inputList){
		
		List<String>  outputList = new ArrayList<String>();
		
		
		for (byte[] key : inputList) {
			outputList.add(Bytes.toString(key));
		}
		return outputList;
		
	}
	
	public static List<byte[]> convertMapToList(Map<byte[], byte[]> inputMap){
		
		List<byte[]> outputList = new ArrayList<byte[]>();
		if(inputMap == null)return outputList;
		
		for (byte[] entry : inputMap.keySet()) {
			outputList.add(entry);
		}
		return outputList;
		
	}


	public static Map<byte[], byte[]> eliminateSignatures(Map<byte[], byte[]> inputMap){
		
		Map<byte[], byte[]> outputMap = new HashMap<byte[], byte[]>();
		if(inputMap == null)return outputMap;
		
		for (byte[] key : inputMap.keySet()) {
			if(!Bytes.toString(key).contains("_") && !Bytes.toString(key).equals("")){
				outputMap.put(key, inputMap.get(key));
			}
		}
		return outputMap;
		
	}

	public static boolean mapContains(byte[] key, Map<byte[], byte[]> inputMap){
		
		if(inputMap == null)return false;
		for (byte[] keyTemp : inputMap.keySet()) {
			if(Bytes.compareTo(key, keyTemp) == 0)return true;
		}
		return false;
		
	}

	public static String listToString(List<byte[]> inputMap){
		
		String result = "[";
		if(inputMap != null){
			for (byte[] key : inputMap) {
				result += Bytes.toString(key)+",";
			}
		}
		result+= "]";
		return result;
		
	}

	public static String mapToString(Map<byte[], byte[]> inputMap){
		
		
		String result = "[";
		if(inputMap != null){
			for (byte[] key : inputMap.keySet()) {
				result += Bytes.toString(key)+"="+Bytes.toString(inputMap.get(key))+",";
			}
		}
		result+= "]";
		return result;
		
	}

}
