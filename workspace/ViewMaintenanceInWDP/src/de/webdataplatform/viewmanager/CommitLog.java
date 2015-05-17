package de.webdataplatform.viewmanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;

public class CommitLog implements ICommitLog{

	private String vmName;
	
	private Configuration configuration;
	
	private FileSystem fs;
	
	private Writer walWriter;
	
	private Reader walReader;
	
	private long logId;
	
	private Log log;
	
	
	public CommitLog(Log log, String vmName){
		
		this.log = log;
		this.vmName = vmName;
		
	}
	
	
	public void openConfiguration(){
	
		
//		System.setProperty("HADOOP_USER_NAME", Constants.HDFS_USERNAME);
//		System.out.println("prop: "+System.getProperty("HADOOP_USER_NAME"));
		
		configuration = new Configuration();

		configuration.set("fs.default.name", NetworkConfig.HDFS);
		
//		System.out.println("openconfig");
		try {
			fs = FileSystem.get( configuration);
		} catch (IOException e) {
			
			log.error(this.getClass(), e);
		}
//		System.out.println("openconfig");
	}
	
	

	public void createLogDirectory() {

		
		if(fs == null)openConfiguration();
		
		
		String pathString = SystemConfig.VIEWMANAGER_HDFSPATH+"/"+vmName;
		Path path = new Path(pathString);

		try {
			
			if(!fs.exists(path)){
				fs.mkdirs(path);
				log.info(this.getClass(),"log directory for "+vmName+" created");
			}else{
				log.info(this.getClass(),"log directory for "+vmName+" exists");
			}
			
		} catch (IOException e) {

			log.error(this.getClass(), e);
		}


		
	}

	@Override
	public void createLogFile() {

		String pathString = SystemConfig.VIEWMANAGER_HDFSPATH+"/"+vmName+"/logfile";
		Path path = new Path(pathString);
		
		try {
			if(fs.exists(path))fs.delete(path,true);
		} catch (IOException e1) {
			
			 log.error(this.getClass(), e1);
		}
		
		logId=0l;
		
		
        Configuration configuration = NetworkConfig.getHBaseConfiguration(log);
        
        try {
			walWriter = HLogFactory.createWALWriter(fs, path, configuration);
		} catch (IOException e) {

			log.error(this.getClass(), e);
		}


		
        log.info(this.getClass(), "Commit log writer created");

		
	}
	
	@Override
	public void append(String vmName, BaseTableUpdate btu) {
		

		HLogKey hlogKey = new HLogKey(Bytes.toBytes(btu.getRegion()), TableName.valueOf(Bytes.toBytes(btu.getBaseTable())), Long.parseLong(btu.getSeqNo()), 1l, HConstants.DEFAULT_CLUSTER_ID);
		
		WALEdit waledit = new WALEdit();
		
		KeyValue keyValue = new KeyValue(Bytes.toBytes(btu.getKey()),Bytes.toBytes("colfam1"), Bytes.toBytes(btu.getColumns().toString()),Bytes.toBytes("value"));
		
		waledit.add(keyValue);
		
		Entry entry = new Entry(hlogKey, waledit);

		
		try {

			walWriter.append(entry);
			walWriter.sync();

			logId++;
		} catch (IOException e) {

			log.error(this.getClass(), e);
		}

		log.update(this.getClass(),"log entry added: "+entry);
		
		
		
	}
	
	@Override
	public boolean openLogFile() {

		
		  
		
		if(fs == null)openConfiguration();
		
		String pathString = SystemConfig.VIEWMANAGER_HDFSPATH+"/"+vmName+"/logfile";
		Path path = new Path(pathString);
		
		logId=0l;
		

		
		try {
			if(!fs.exists(path)){
				log.info(this.getClass(),"commit log of view manager"+vmName+" cannot be recovered, path:"+pathString);
				return false;
			}else{
				
				
				Configuration configuration = NetworkConfig.getHBaseConfiguration(log);
				walReader = HLogFactory.createReader(fs, path, configuration);
				log.info(this.getClass(),"commit log of view manager"+vmName+" opened");
				return true;
				
			}
		} catch (IOException e1) {

			log.error(this.getClass(), e1);
		}
		

		return false;
		
	}


	


	@Override
	public Map<String, Long> readHighestSeqNos(){
		
	    log.info(this.getClass(), "reading commit log of view manager: "+vmName);
		
		Map<String, Long> result = new HashMap<String, Long>();
		
		Entry iterate=null;
		try {
			
			
			while ((iterate = walReader.next()) != null) {
				
				String region = Bytes.toString(iterate.getKey().getEncodedRegionName());
				Long seqNo = iterate.getKey().getLogSeqNum();
				
				result.put(region, seqNo);


			}
		} catch (IOException e) {

			log.error(this.getClass(), e);
		}	
		

		
		
		return result;
		
	}


	@Override
	public void closeLogFile() {
		
		try {
			walReader.close();
			walWriter.close();
		} catch (IOException e) {
			
			log.error(this.getClass(), e);
		}
		
		
		
	}






	
	

}
