package de.webdataplatform.test;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.regionserver.WALReader;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.view.ViewDefinitions;
import de.webdataplatform.viewmanager.CommitLog;

public class TestCommitLog {

	
	
    private static HLog.Writer writer;
    private static Path logFile;
    
    private static Configuration conf;
    private static FileSystem fs;

	
	
	public static void main(String[] args) throws IOException {
		
//		String uri = args[0];
//		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "jan");
		
		Configuration configuration = new Configuration();
//		configuration.set("fs.defaultFS", "hdfs://192.168.26.135");
		configuration.set("fs.default.name", "hdfs://deltaserver1:8020");
		
		FileSystem fs = FileSystem.get( configuration);
		
		
//		String pathString = "/user/jan/log/wal.log";
//		/hbase/.logs/deltaserver1.local,60020,1386663685458/deltaserver1.local%2C60020%2C1386663685458.1386663694018
//		String pathString = "/hbase/.logs/deltaserver1.local,60020,1386663685458/";
//		String pathString = "/hbase/.logs/deltaserver1.local,60020,1386663685458/deltaserver1.local%2C60020%2C1386663685458.1386663694018";
		String pathString = "/hbase/WALs";
		Path path = new Path(pathString);
		
//		fs.delete(path,true);
//		String dirString = "/user/jan/log";
//		Path pathDir = new Path(dirString);
//		r
//		fs.mkdirs(path);
		Log log = new Log("evaluation.log");

		SystemConfig.load(log);
		NetworkConfig.load(log);
		

		CommitLog commitLog = new CommitLog(log, "vm1");
		
		commitLog.openConfiguration();
		commitLog.openLogFile();
		
		System.out.println("lastEntry: "+commitLog.readHighestSeqNos());
	
		
	}
	
}
