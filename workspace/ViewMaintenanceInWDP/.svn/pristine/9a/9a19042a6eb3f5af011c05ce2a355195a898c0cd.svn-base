package de.webdataplatform.viewmanager;

import java.rmi.Remote;
import java.rmi.RemoteException;

import de.webdataplatform.storage.BaseRecord;
import de.webdataplatform.storage.IBaseRecord;
import de.webdataplatform.storage.IBaseRecordAggregation;
import de.webdataplatform.storage.IBaseRecordJoin;
import de.webdataplatform.storage.IBaseRecordJoinLeft;
import de.webdataplatform.storage.IBaseRecordJoinRight;
import de.webdataplatform.storage.IBaseRecordProjection;
import de.webdataplatform.storage.IStorage;
import de.webdataplatform.storage.Update;
import de.webdataplatform.view.IView;
import de.webdataplatform.view.IViewRecord;
import de.webdataplatform.view.IViewRecordAggregation;
import de.webdataplatform.view.IViewRecordJoin;
import de.webdataplatform.view.Signature;
import de.webdataplatform.view.ViewMode;
import de.webdataplatform.view.ViewRecord;

public interface IViewManager extends Remote{
	

	
	
	/**
	 * Returns the name of the view manager.
	 * 
	 * @return
	 * @throws RemoteException
	 */
	public String getVMName();
	
	public String toString();
	

	
	/**
	 * Registers the view manager to Zookeeper
	 * 
	 * @throws RemoteException
	 */
	public void register() throws RemoteException;
	
	/**
	 * Registers the view manager to Zookeeper
	 * 
	 * @throws RemoteException
	 */
	public void deregister() throws RemoteException;
	
	
	/**
	 * Process an update with the viewmanager
	 */
	public void processUpdate();
	

	/**
	 * Retrieve the last processed Update
	 */
	public void lastProcessedUpdate();
	
	
//	
//	/**
//	 * 
//	 * @throws RemoteException
//	 */
//	public void pollUpdates() throws RemoteException ;
	
	
	/**
	 * 
	 * @param k
	 * @param baseTableColumn
	 * @throws RemoteException
	 */
	public void propagateInsertAggregation(String k, IBaseRecordAggregation baseTableColumn, long eid, ViewMode viewMode)throws RemoteException;
	
	/**
	 * 
	 * @param k
	 * @param oldBaseTableColumn
	 * @param newBaseTableColumn
	 * @throws RemoteException
	 */
	public void propagateUpdateAggregation(String k, IBaseRecordAggregation oldBaseTableColumn, IBaseRecordAggregation newBaseTableColumn, long eid, ViewMode viewMode)throws RemoteException;
	
	
	/**
	 * 
	 * @param k
	 * @param baseTableColumn
	 * @throws RemoteException
	 */
	public void propagateDeleteAggregation(String k, IBaseRecordAggregation baseTableColumn, long eid, ViewMode viewMode)throws RemoteException;
	
	

	

	public void propagateInsertProjection(String k, IBaseRecordProjection projectionRecord, long eid)throws RemoteException;
	
	public void propagateUpdateProjection(String k, IBaseRecordProjection oldProjectionRecord, IBaseRecordProjection newProjectionRecord, long eid)throws RemoteException;

	public void propagateDeleteProjection(String k, IBaseRecordProjection projectionRecord, long eid)throws RemoteException;
	
	
	

	public void propagateInsertJoin(String k, IBaseRecord joinRecord, long eid)throws RemoteException;	
		
	public void propagateDeleteJoin(String k, IBaseRecord joinRecord, long eid)throws RemoteException;
	
	public void propagateUpdateJoin(String k, IBaseRecordJoin oldJoinRecord, IBaseRecordJoin newJoinRecord, long eid)throws RemoteException;
	
	


	
	/**
	 * Reads the values from the view table for a given key.
	 * 
	 * @param key
	 * @return
	 */
	public boolean read(String key);
	
	/**
	 * Checks  if a timestamp of an Update so has already been processed. If the 
	 * timestamp is already included in the signature the Update is denied
	 * 
	 * @param signature
	 * @param eid
	 * @return
	 */
	public boolean hasProcessed(Signature signature, long eid);

	/**
	 * Generates a new signature with a given timestamp(eid). If the the parameter
	 * signature is null a new signature is generated and the timestamp(eid) is added. If the 
	 * parameter signature is not null the parameter is copied and the timestamp(eid) is added to it.
	 * 
	 * @param signature
	 * @param eid
	 * @return
	 */
	public Signature generateSignature(Signature signature, long eid);

	
	


}
