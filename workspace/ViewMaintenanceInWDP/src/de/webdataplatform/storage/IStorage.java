package de.webdataplatform.storage;

import java.rmi.Remote;
import java.rmi.RemoteException;

import de.webdataplatform.viewmanager.IViewManager;

public interface IStorage extends Remote{
	

	
	

	/**
	 * Inserts/Updates a key/value-pair in the base table of the key value storage. 
	 * 
	 * @param key
	 * @param value
	 * @throws RemoteException
	 */
	public void put(String tableName, String key, IBaseRecord value) throws Exception;
	
	/**
	 * Receives a value from the base table of the key value storage 
	 * 
	 * @param key
	 * @return
	 * @throws RemoteException
	 */
	public IBaseRecord get(String tableName, String key) throws Exception;	
	
	/**
	 * Registers a view manager to the storage. Only a registered view manager is able to 
	 * request Updates from the storage
	 * 
	 * @param name
	 * @param viewManager
	 * @throws RemoteException
	 */
	public void register(String name, IViewManager viewManager) throws RemoteException;

	/**
	 * Deregisters a view manager from the storage.
	 * 
	 * @param name
	 * @throws RemoteException
	 */
	void deregister(String name) throws RemoteException;	

	
	
//	/**
//	 * Informs all registered view managers that an update has been processed so 
//	 * that they can call the requestUpdate method to retrieve the update
//	 * 
//	 * @throws RemoteException
//	 */
//	public void notifyViewManagers() throws RemoteException;
//	
	
	/**
	 * Requests an update from the storage. If an update is in the update queue then it
	 * is delivered, otherwise the return value is null.
	 * 
	 * @param viewManagerName
	 * @return
	 * @throws RemoteException
	 */
	public Update<IBaseRecord> requestUpdate(String viewManagerName) throws RemoteException;
	
	
	/**
	 * Prints out the storage
	 * 
	 * @throws RemoteException
	 */
	public void showStorage() throws RemoteException;
	
	/**
	 * Shuts down the storage
	 * 
	 * @throws RemoteException
	 */
	public void shutdownStore() throws RemoteException;
	
	

}
