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

public class TestWAL {

	
	
    private static HLog.Writer writer;
    private static Path logFile;
    
    private static Configuration conf;
    private static FileSystem fs;
	/**
	 * @param args
	 */
	/**public static void main(String[] args) {
		
		
		String localSrc = "c://setup.log";
		String dst = "hdfs://192.168.26.135/user/jan/lala.txt";
		InputStream in;

		System.setProperty("HADOOP_USER_NAME", "jan");
		
		Configuration configuration = new Configuration();
//		configuration.set("fs.defaultFS", "hdfs://192.168.26.135");
		configuration.set("fs.default.name", "hdfs://192.168.26.135");
		String hpath = "/user/jan/log/bla.txt";
//        conf.set("hadoop.job.ugi", "hbase");fs.default.name
        
		FileSystem hdfs;
		try {
			
			hdfs = FileSystem.get( configuration );
//			Path file = new Path(hpath);
			Path filenamePath = new Path(hpath);
			
//			FSPermission permisson = new FSPermission();FileUtil.

//			System.out.println(hdfs.mkdirs(filenamePath));
			
            FSDataOutputStream out = hdfs.create(filenamePath);
            out.writeBytes("DUPA DUPA DUPA\n");
            
//			if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
//			OutputStream os = hdfs.create( file, new Progressable() {
//				public void progress() {
//					System.out.println("...bytes written: [ ]");
//				} });
			
//			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
//			br.write("Hello World");
//			br.close();
//			hdfs.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		Configuration conf = new Configuration();
//		FileSystem fs=null;
//		OutputStream out;
//		try {
//			in = new BufferedInputStream(new FileInputStream(localSrc));
//			fs = FileSystem.get(URI.create(dst), conf);
//			out = fs.create(new Path(dst), new Progressable() {
//				public void progress() {
//					System.out.print(".");
//				}
//			});
//			IOUtils.copyBytes(in, out, 4096, true);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

			

		}*/
	

	private static final String[] DATA = {
		"One, two, buckle my shoe",
		"Three, four, shut the door",
		"Five, six, pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, a big fat hen"
	};
	
//	private static final String[] DATA = {
//		"jetzt","testen","wir","das","wal","von",
//		"hadoop"
//	};
	
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
		
		
		SystemID systemID = new SystemID("rs1", "deltaserver1", 3343,4433);
		
		AtomicLong updatesRetrieved = new AtomicLong();
		
		
		Queue incomingUpdates = new ConcurrentLinkedQueue<BaseTableUpdate>();
		
		ViewDefinitions getViews = new ViewDefinitions(log, "viewdefinitions");
		getViews.loadViewDefinitions();
		
		WALReader walReader = new WALReader(log, systemID.getIp(),systemID.getName(), getViews, incomingUpdates);
		
		
		Thread t = new Thread(walReader);
		
		t.start();
		
		/*
		 try{
          
             FileStatus[] status = fs.listStatus(path);  // you need to pass in your hdfs path
             for (int i=0;i<status.length;i++){
            	 
            	 System.out.println(status[i].getPath());
//                     BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
//                     String line;
//                     line=br.readLine();
//                     while (line != null){
//                             System.out.println(line);
//                             line=br.readLine();
//                     }
             }
     }catch(Exception e){
             System.out.println("File not found");
     }*/
		
		
//		SequenceFileLogWriter sflw = new SequenceFileLogWriter();
//		
//		sflw.init(fs, path, configuration);
//		
//		
//		HLogKey hlogKey = new HLogKey(Bytes.toBytes("region1"), Bytes.toBytes("basetable1"), 1, 1, HConstants.DEFAULT_CLUSTER_ID);
//		
//		WALEdit waldeit = new WALEdit();
//		
//		Entry entry = new Entry(hlogKey, waldeit);
//		sflw.append(entry);
		
		
/*		SequenceFileLogReader sflr = new SequenceFileLogReader();
		
		sflr.init(fs, path, configuration,null);
		
		
		
		Entry iterate;
		int i=0;
		while ((iterate = sflr.next()) != null) {
			
			System.out.println(iterate);
			System.out.println(iterate.getKey().getLogSeqNum());
			System.out.println(Bytes.toString(iterate.getKey().getTablename().getName()));
			System.out.println(Bytes.toString(iterate.getKey().getEncodedRegionName()));
//			System.out.println(iterate.getKey().getClusterId());
			for(KeyValue keyValue : iterate.getEdit().getKeyValues()){
				System.out.println(Type.codeToType(keyValue.getType()));
				
			}
			System.out.println(i);
			System.out.println("----------------------------");
			i++;
		}	*/
		
		
		
//		String oldpathString = "/user/jan/log/oldwal.log";
//		Path oldpath = new Path(oldpathString);		
//		
//		HLog hlog = new HLog(fs, path, oldpath, configuration);
//		
//		hlog.a
		
		
//		WRITE!!!!!!!!!!!!!
//	    IntWritable key = new IntWritable();
//		Text value = new Text();
//		SequenceFile.Writer writer = null;
//		
//		try {
//		writer = SequenceFile.createWriter(fs, configuration, path, key.getClass(), value.getClass());
//	
//		
//			for (int i = 0; i < 100; i++) {
//				key.set(i);
//				value.set(DATA[i % DATA.length]);
//				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
//				writer.append(key, value);
//			}
//			
//		} finally {
//		IOUtils.closeStream(writer);
//		}
		
		
		
//		READ!!!!!!!!!!!!!
//		SequenceFile.Reader reader = null;
//		try {
//			reader = new SequenceFile.Reader(fs, path, configuration);
//			Writable key = (Writable)
//			ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
//			Writable value = (Writable)
//			ReflectionUtils.newInstance(reader.getValueClass(), configuration);
////			long position = reader.getPosition();
//			long position = 0l;
////			reader.seek(1790l);
//			
//			while (reader.next(key, value)) {
//				String syncSeen = reader.syncSeen() ? "*" : "";
//				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
//				position = reader.getPosition(); // beginning of next record
//			}
//		} finally {
//		IOUtils.closeStream(reader);
//		}
		
	}
	
}
