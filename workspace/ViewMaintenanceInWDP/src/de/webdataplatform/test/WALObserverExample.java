package de.webdataplatform.test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.regionserver.RegionServer;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;



public class WALObserverExample  extends BaseRegionObserver implements WALObserver{
	


	private RegionServer regionServer;
	
	private Queue<BaseTableUpdate> btupdates = new ConcurrentLinkedQueue<BaseTableUpdate>();
    
//	
//	public WALObserverExample(){
//		super();
//		getRegionServer();
//	}
	


	public RegionServer getRegionServer() {
		
		
		if(regionServer == null){
			
//			SystemConfig.load();

			System.out.println("initializing new RegionServer");
//	        regionServer = new RegionServer(Constants.REGIONSERVER_NAME, Constants.REGIONSERVER_ADDRESS, Constants.REGIONSERVER_PORT, Constants.ZOOKEEPER_ADDRESS);
	        
	        regionServer.initialize();
			
		}
		return regionServer;
	}

	public void setRegionServer(RegionServer regionServer) {
		this.regionServer = regionServer;
	}
	
	

	@Override
	public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> arg0, HRegionInfo arg1, HLogKey arg2, WALEdit arg3) throws IOException {
		
	}

	
	private Set<String> basetables;
	
	
	private Map<TableName, HTableInterface> tables = new HashMap<TableName, HTableInterface>();
	
	@Override
	public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> arg0, HRegionInfo arg1, HLogKey arg2, WALEdit arg3) {
		
//			if(arg3.getKeyValues().size() > 0){
//				System.out.println(Bytes.toString(arg3.getKeyValues().get(0).getRow()));
//				System.out.println(Bytes.toString(arg1.getTableName()));
//			}
//			
	
			if(!Bytes.toString(arg1.getTableName()).contains("meta") && !Bytes.toString(arg1.getTableName()).contains("root")&& !Bytes.toString(arg1.getTableName()).contains("viewdefinitions")){
			
					if(basetables == null || basetables.size() == 0){
					
						System.out.println("starting to load basetables");
						try{
							basetables = new HashSet<String>();
							
			
							SystemConfig.LOGGING_CONSOLE = true;
							SystemConfig.LOGGING_FILE = false;
							
							
							HTableInterface table = arg0.getEnvironment().getTable(TableName.valueOf(Bytes.toBytes("viewdefinitions")));
							
			
							
							Scan scan1 = new Scan();
							ResultScanner scanner1 = table.getScanner(scan1);
							
						
							
							for (Result res : scanner1) {
							
								basetables.add(Bytes.toString(res.getRow()));
							}
							
							if(basetables.size() == 0){
								
								basetables.add("dummy");
							}
							
							scanner1.close();
							table.close();
							System.out.println(basetables);
							
						}catch(Exception e){
							e.printStackTrace();
						}
					}
			}		

		
			if(basetables != null){
				boolean viewDefined = false;
				
				for (String basetable : basetables) {
//					System.out.println(basetable);
//					System.out.println(Bytes.toString(arg2.getTablename().getName()));
//					System.out.println("-----------------------");
					if (basetable.equals(Bytes.toString(arg2.getTablename().getName()))) {
						viewDefined = true;
//						System.out.println("Matching!!1");
					}
				}
			
				if(viewDefined){
				
//					System.out.println("Next WAL Write");
					System.out.println("--------------------------------------------------------");
//					synchronized(this){
					long startTime = new Date().getTime();
					String rowKey = "";
	
					byte type=0;
					long timestamp=0;
					for(KeyValue keyValue : arg3.getKeyValues()){
						
						
						rowKey = Bytes.toString(keyValue.getRow());
	//					String qualifier = Bytes.toString(keyValue.getQualifier());
	//					String value = Bytes.toString(keyValue.getValue());
	
						timestamp = keyValue.getTimestamp();
						type = keyValue.getType();
						
					}
					System.out.println("rowkey: "+rowKey);
					System.out.println("Operation type: "+KeyValue.Type.codeToType(type).toString());
					
	//				Map<String, String> stringMap = new HashMap<String, String>();
					if(rowKey != null && !rowKey.equals("")){
						
	//					System.out.println("rowkey: "+rowKey);
						HTableInterface table;
						Result result=null; 
						
//						System.out.println("Get with:"+rowKey);
						Get get = new Get(Bytes.toBytes(rowKey));

						try {
							
							
							table = tables.get(arg2.getTablename());
							System.out.println("table: "+table);
							if(table == null){
								
							  long startTime2 = new Date().getTime();	
							  System.out.println("initializing: "+table);
							  table = arg0.getEnvironment().getTable(arg2.getTablename());
							  tables.put(arg2.getTablename(), table);
							  System.out.println("time to scan: "+(new Date().getTime()-startTime2));
							}
							long startTime3 = new Date().getTime();	
//							result = table.get(get);
							System.out.println("time to get record: "+(new Date().getTime()-startTime3));
//							System.out.println("Result:"+result);
							

						} catch (Exception e) {

							e.printStackTrace();
						}
						
						
						
						
					}
					
					System.out.println("time to scan: "+(new Date().getTime()-startTime));
						
						
					}
				}
//			}
			
			
		
		return false;
	}
	
	
	
	
	
}