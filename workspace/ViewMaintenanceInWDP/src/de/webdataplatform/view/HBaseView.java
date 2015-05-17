package de.webdataplatform.view;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.storage.Update;

public class HBaseView implements IView, Serializable{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4438734976045880199L;
	private static HTable table;
	
	
	public HBaseView(){
		
	}
	

	private static void initTable() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.127.129");
		try {
			table = new HTable(conf, "viewtable_aggregation");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}	
	
	
	
	
	public static HTable getTable() {
		if(table == null) initTable();
		return table;
	}




	public static void setTable(HTable table) {
		HBaseView.table = table;
	}




	public void initialize(List<Update<IViewRecord>> config) throws RemoteException{
		
		
//		initTable();
		
		delete("", "x1");
		delete("", "x2");
		delete("", "x3");
		delete("", "x4");
		
		for(Update<IViewRecord> update : config){
			
			try {
				put("", update.getKey(), update.getNewColumn());
			} catch (RemoteException e) {

				e.printStackTrace();
			}
			
		}
	}


	
	
	@Override
	public void put(String viewManager, String key, IViewRecord value) throws RemoteException {
		
		Put put = new Put(Bytes.toBytes(key));
//		put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"), Bytes.toBytes(value.getAggregatedValue()));	
		try {

			getTable().put(put);

		} catch (IOException e) {

			e.printStackTrace();
		}
		
	}

	@Override
	public IViewRecord get(String key) throws RemoteException {
		
		
		Get get = new Get(Bytes.toBytes(key));
		Result result=null;
		int s=0;
		try {

			System.out.println("Table: "+table);
			
			result = getTable().get(get);
			
			if(result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue")) != null)
			s = Bytes.toInt(result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue")));
			else return null;

			
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		IViewRecord vtr = new ViewRecord(s);		
		
		return vtr;
	}

	@Override
	public void delete(String viewManager, String x) throws RemoteException {
		
		Delete delete = new Delete(Bytes.toBytes(x));
		
		try {
			getTable().delete(delete);
			
		} catch (IOException e) {
	
			e.printStackTrace();
		}
		
	
	}
	@Override
	public boolean updateTAS(String viewManager, String x, IViewRecord setValue, Signature testSignature) throws RemoteException {
		
		
		put(viewManager, x, setValue);
		System.out.println("View updated by:"+viewManager+", key="+x+", SetValue="+setValue);
		showView();
		
		return true;
	}

	@Override
	public boolean insertTAS(String viewManager, String x, IViewRecord setValue) throws RemoteException {
		
		put(viewManager, x, setValue);
		System.out.println("View updated by:"+viewManager+", key="+x+", SetValue="+setValue);
		showView();
		
		return true;
	}

	@Override
	public boolean deleteTAS(String viewManager, String x, Signature testSignature) throws RemoteException {

		delete(viewManager, x);
		
		return true;
	}

	@Override
	public void showView() throws RemoteException {
	
		Scan scan1 = new Scan();
		ResultScanner scanner1=null;
		try {
			scanner1 = getTable().getScanner(scan1);
			
			
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		for (Result res : scanner1) {
			System.out.print("Key: "+Bytes.toString(res.getRow())+",  ");
			System.out.print("AggValue: "+Bytes.toInt(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("aggregatedValue"))));
			System.out.println();

		}
		scanner1.close();
		
		
	}


}
