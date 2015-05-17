package de.webdataplatform.regionserver;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class MD5 implements HashFunction{

	@Override
	public Integer hash(String value) {
		
		

		return Bytes.toInt(DigestUtils.md5( Bytes.toBytes(value) ));
	}


	
	

}
