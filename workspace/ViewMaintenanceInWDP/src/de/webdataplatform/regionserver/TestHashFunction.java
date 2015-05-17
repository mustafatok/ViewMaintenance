package de.webdataplatform.regionserver;

import java.util.ArrayList;
import java.util.List;

public class TestHashFunction {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		ArrayList<String> al = new ArrayList<String>(); 
        al.add("viewmanager1");
        al.add("viewmanager2");
        al.add("viewmanager3");
        al.add("viewmanager4");

        
        List<String> keys = new ArrayList<String>();
        
        for (int i = 0; i < 1000; i++) {
			keys.add("k"+i);
		}

        
        
		HashFunction hf = new MD5();
//		357297
//		661785
		
		ConsistentHash<String> consistentHash = new ConsistentHash<String>(hf, 30, al);
		
		int v1 = 0;
		int v2 = 0;
		int v3 = 0;
		int v4 = 0;
		
//		consistentHash.remove("viewmanager4", 250);
		
		for(Integer key : consistentHash.getCircle().keySet()){
			
			System.out.println("key: "+key+", value: "+consistentHash.getCircle().get(key));
		}
		
		
		for (String key : keys) {
			
			String hash = consistentHash.get(key);
			
			if(hash.equals("viewmanager1"))v1++;
			if(hash.equals("viewmanager2"))v2++;
			if(hash.equals("viewmanager3"))v3++;
			if(hash.equals("viewmanager4"))v4++;
			
            System.out.println(hash);
        }
		
		
		
		
		System.out.println("---------------------");
		System.out.println("viewmanager1: "+v1);
		System.out.println("viewmanager2: "+v2);
		System.out.println("viewmanager3: "+v3);
		System.out.println("viewmanager4: "+v4);
		
		
	}

}
