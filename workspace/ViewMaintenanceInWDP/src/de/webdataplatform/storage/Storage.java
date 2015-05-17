package de.webdataplatform.storage;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.webdataplatform.viewmanager.IViewManager;

public class Storage  extends UnicastRemoteObject  implements IStorage, Serializable{
	

	
	public Storage() throws RemoteException {
		super();

	}
	
	public Storage(List<Update<IBaseRecord>> config) throws RemoteException {
		super();
		for(Update<IBaseRecord> update : config){
			
			baseTable.put(update.getKey(), update.getNewColumn());
			
		}
	}
	


	/**
	 * 
	 */
	private static final long serialVersionUID = -6742238873165232306L;

	
	/** Storage Tables */
	
	private Map<String, IBaseRecord> baseTable = new HashMap<String, IBaseRecord>();
	
	private Map<String, IBaseRecord> joinTableLeft = new HashMap<String, IBaseRecord>();
	
	private Map<String, IBaseRecord> joinTableRight = new HashMap<String, IBaseRecord>();
	
	
	
	
	private Map<String, IViewManager> viewManagers= new HashMap<String, IViewManager>();
	
	private List<Update<IBaseRecord>> updateQueue = new ArrayList<Update<IBaseRecord>>();
	
	private long updateCount=0;
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.storage.IStorage#register(java.lang.String, de.webdataplatform.viewmanager.IViewManager)
	 */
	@Override
	public void register(String name, IViewManager viewManager) throws RemoteException {
		
		if(viewManagers == null){			
			viewManagers = new HashMap<String, IViewManager>();
		}
		if(viewManagers.keySet().contains(name)){
			System.out.println("Message from Storage: ViewManager "+name+" bereits registriert");
		}
		else{
			
			viewManagers.put(name, viewManager);
			System.out.println("Message from Storage: ViewManager "+name+" wurde registriert");
			System.out.println("Anzahl ViewManagers: "+viewManagers.size());
			
		}
		
		
	}


	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.storage.IStorage#deregister(java.lang.String)
	 */
	@Override
	public void deregister(String name) throws RemoteException {
		
		if(viewManagers == null){			
			System.out.println("Message from Storage: ViewManager "+name+" nicht registriert ");
		}
		if(!viewManagers.keySet().contains(name)){
			System.out.println("Message from Storage: ViewManager nicht registriert");
		}
		else{
			
			viewManagers.remove(name);
			System.out.println("Message from Storage: ViewManager "+name+" wurde deregistriert");
		}
		
		
	}


	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.storage.IStorage#put(java.lang.String, de.webdataplatform.storage.BaseTableColumn)
	 */
	@Override
	public void put(String tableName, String key, IBaseRecord newValue) throws Exception {

		System.out.println("Update key value store");
		
		IBaseRecord oldValue = baseTable.get(key);
		
		boolean tableFound = false;
		if(tableName.equals("basetable")){
			baseTable.put(key, newValue);
			tableFound = true;
		}
		if(tableName.equals("jointableleft")){
			joinTableLeft.put(key, newValue);
			tableFound = true;
		}		
		if(tableName.equals("jointableright")){
			joinTableRight.put(key, newValue);
			tableFound = true;
		}	
		if(!tableFound) throw new Exception("No such table: "+tableName);
			
		synchronized(updateQueue){
			
			
			Update<IBaseRecord> update = new Update<IBaseRecord>("", key, newValue, oldValue, updateCount);
			updateCount++;
			updateQueue.add(update);
			
			System.out.println("Update added to updateQueue, queueCount="+updateQueue.size());
			System.out.println("Key: "+key+", newValue: "+newValue+", oldValue: "+oldValue);
			
		}
		
//		notifyViewManagers();
	
		
	}

	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.storage.IStorage#get(java.lang.String)
	 */
	@Override
	public IBaseRecord get(String tableName, String key) throws Exception {

		if(tableName.equals("basetable"))return baseTable.get(key);
		if(tableName.equals("jointableleft"))joinTableLeft.get(key);
		if(tableName.equals("jointableright"))joinTableRight.get(key);


		throw new Exception("No such table: "+tableName);
		
	}	
	

	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.storage.IStorage#shutdownStore()
	 */
	public void shutdownStore() throws RemoteException{
		

		System.out.println("Key value store down");
		System.exit(0);
		
	}

	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.storage.IStorage#showStorage()
	 */
	@Override
	public void showStorage() {
		StorageUtil.printStorage(baseTable);
		
	}
	
	
//	@Override	
//	public void notifyViewManagers(){
//		
//		System.out.println("Notifying ViewManagers");
//		
//		for(IViewManager viewManager : viewManagers.values()){
//			
//			try {
//				viewManager.notifyVM();
//			} catch (RemoteException e) {
//
//				e.printStackTrace();
//			}
//			
//		}
//		
//		
//	}
//	
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.storage.IStorage#requestUpdate(java.lang.String)
	 */
	@Override
	public synchronized Update<IBaseRecord> requestUpdate(String viewManagerName) throws RemoteException {

		synchronized(updateQueue){
			if(updateQueue.size() > 0 && viewManagers.containsKey(viewManagerName)){
				Update<IBaseRecord> update = updateQueue.get(0).copy();
				updateQueue.remove(0);
				System.out.println("Update removed from updateQueue, queueCount="+updateQueue.size());
				System.out.println("Propagating Update to "+viewManagerName);
				return update;
						
			}
		}	
			
		
		return null;
	}
	
	



	
}
