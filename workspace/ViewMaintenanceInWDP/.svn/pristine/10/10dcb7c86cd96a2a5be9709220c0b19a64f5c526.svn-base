package de.webdataplatform.view;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import de.webdataplatform.storage.Update;


public interface IView extends Remote{

	
	
	public void initialize(List<Update<IViewRecord>> config)throws RemoteException;
	
	
	/**
	 *  Inserts/Updates a key/value-pair in the view table. 
	 * 
	 * @param viewManager
	 * @param key
	 * @param value
	 * @throws RemoteException
	 */
	public void put(String viewManager, String key, IViewRecord value) throws RemoteException;
	
	
	/**
	 * Receives a value from the view table. 
	 * 
	 * @param key
	 * @return
	 * @throws RemoteException
	 */
	public IViewRecord get(String key) throws RemoteException;
	
	
	
	public void delete(String viewManager, String x) throws RemoteException;
	
	/**
	 * Works like the put method and is used to update a key/value-pair of the view table. In addition 
	 * to the put method the inserted value is tested whether it has been modified by another process in the meantime. 
	 * The key/value-pair is represented by the parameters x/setValue. The testSignature contains the number of
	 * updates processed at the time the view record had been retrieved.
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws RemoteException
	 */
	public boolean updateTAS(String viewManager, String x, IViewRecord setValue, Signature testSignature) throws RemoteException;	
	
	/**
	 * Works like the put method and is used to insert a key/value-pair into the view table. In addition to the 
	 * put method it checks whether the key/value-pair has already been inserted by another process.
	 * 
	 * @param viewManager
	 * @param x
	 * @param setValue
	 * @return
	 * @throws RemoteException
	 */
	public boolean insertTAS(String viewManager, String x, IViewRecord setValue) throws RemoteException;	
	
	/**
	 * Is used to remove a key/value-pair from the view table. It tests whether the record has already been 
	 * deleted/modified by another process in the meantime. The key/value-pair is represented by the parameters 
	 * x/setValue. The testSignature contains the number of updates processed at the time the view 
	 * record had been retrieved.
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws RemoteException
	 */
	public boolean deleteTAS(String viewManager, String x, Signature testSignature) throws RemoteException;	
	
	
	/**
	 * Prints out the view table.
	 * 
	 * @throws RemoteException
	 */
	public void showView() throws RemoteException;
	
}
