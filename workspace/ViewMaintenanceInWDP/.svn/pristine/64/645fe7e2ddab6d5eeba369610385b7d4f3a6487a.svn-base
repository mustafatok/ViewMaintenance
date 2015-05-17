package de.webdataplatform.view;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.webdataplatform.storage.StorageUtil;
import de.webdataplatform.storage.Update;


public class View  implements Serializable{
	
	
	/**
	private static final long serialVersionUID = 7481947086820528375L;
	private Map<String, Map<String, Integer>> viewTable = new HashMap<String, IViewRecord>();
	
	


	public View(){
	}
	
	
//	public View(List<Update<IViewTableRecord>> config) throws RemoteException{
//		super();
//		View.java
//		for(Update<IViewTableRecord> update : config){
//			
//			viewTable.put(update.getKey(), update.getNewColumn());
//			
//		}
//		
//	}
	
	@Override
	public void initialize(List<Update<IViewRecord>> config) {
		
		for(Update<IViewRecord> update : config){
			
			viewTable.put(update.getKey(), update.getNewColumn());
			
		}
		
	}

	

	@Override
	public void put(String viewManager, String key, IViewRecord value) throws RemoteException {
		
		viewTable.put(key, value);
		System.out.println("View updated by:"+viewManager+", key="+key+", SetValue="+value);
		showView();
//		StorageUtil.printViewTable(viewTable);
		
		
		
	}
	

	@Override
	public IViewRecord get(String tableName, String key) throws RemoteException {

		
		return viewTable.get(key);
	}
	


	@Override
	public void showView() throws RemoteException {
		StorageUtil.printView(viewTable);
		
	}

	

	@Override
	public synchronized boolean updateTAS(String viewManager, String x, IViewRecord setValue, Signature testSignature)throws RemoteException {
		
		
		Signature currentSignature = viewTable.get(x).getSignature();

		
		if(currentSignature != null && currentSignature.equals(testSignature)){
			
			viewTable.put(x, setValue);
			
			System.out.println("View TAS updated by:"+viewManager+", key="+x+" TestSig="+testSignature+", CurrentSig="+currentSignature);
			showView();
			return true;
		}
		
		System.out.println("View TAS updated by:"+viewManager+", key="+x+" TestSig="+testSignature+", CurrentSig="+currentSignature+" failed!");
		return false;

	}

	

	@Override
	public synchronized boolean insertTAS(String viewManager, String x, IViewRecord setValue) throws RemoteException {

		System.out.println("setValue: "+setValue);
		if(viewTable.get(x) == null){
			
			viewTable.put(x, setValue);
			System.out.println("View TAS inserted by:"+viewManager+", key="+x+" CurrentSig="+setValue.getSignature());
			showView();
			return true;
		}
		System.out.println("View TAS inserted by:"+viewManager+", key="+x+" CurrentSig="+setValue.getSignature()+" failed!");
		return false;
	}


	@Override
	public synchronized boolean deleteTAS(String viewManager, String x, Signature testSignature)throws RemoteException {

		
		Signature currentSignature = viewTable.get(x).getSignature();		
		
		if(currentSignature != null && currentSignature.equals(testSignature)){
			
			viewTable.remove(x);
			
			System.out.println("View TAS deleted by:"+viewManager+", key="+x+" TestSig="+testSignature);
			showView();
			return true;
		}
		
		System.out.println("View TAS deleted by:"+viewManager+", key="+x+" TestSig="+testSignature+", CurrentSig="+currentSignature+" failed!");
		return false;
	}


	@Override
	public void delete(String viewManager, String x) throws RemoteException {
		// TODO Auto-generated method stub
		
	}


*/

	
	

}
