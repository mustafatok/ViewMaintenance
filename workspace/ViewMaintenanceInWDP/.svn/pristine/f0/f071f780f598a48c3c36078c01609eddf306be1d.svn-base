package de.webdataplatform.viewmanager;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

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

public class CopyOfViewManager  implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 201204933834777882L;

	private String name;
	
	private IStorage storage;
	
	private IView view;

	private IViewRecord updateColumn;
	
	private Signature signature;
	
	private String ip;
	
	private int port;
	
	
	

	public CopyOfViewManager(String name, String ip, int port){

		
		this.name = name;
		this.ip = ip;
		this.port = port;
		
		
		
//		this.storage = storage;
//		this.view = view;
	}


	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#getVMName()
	 */
	public String getVMName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#registerVM()
	 */
	public void register() {
		
		
		
//		try {
////			storage.register(name, this);
//		} catch (RemoteException e) {
//
//			e.printStackTrace();
//		}
		
	}

	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateInsert(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateInsertAggregation(String k, IBaseRecordAggregation aggregationRecord, long eid, ViewMode viewMode)throws RemoteException {
		
		String x = aggregationRecord.getAggregationIdentifier();
		
		
		boolean succeed = false;
		IViewRecordAggregation viewRecord = null;

		do{
		
			boolean foundEntry = read(x);
			
			if(foundEntry){
				
				viewRecord = (IViewRecordAggregation)updateColumn;
				
				int c = viewRecord.getAggregatedValue();

				if(hasProcessed(signature, eid)){
					System.out.println("Update key="+k+", column="+aggregationRecord+", timestamp="+eid+" rejected --> It has already been processed");
					break;
				}
				
				Signature newSignature = generateSignature(signature, eid);
				viewRecord.setSignature(newSignature);
				
				int result=0;
				if(viewMode.equals(ViewMode.AGGREGATION_SUM))result = c + aggregationRecord.getAggregationValue();
				if(viewMode.equals(ViewMode.AGGREGATION_COUNT))result = c + 1;
				if(viewMode.equals(ViewMode.AGGREGATION_MIN)){
						if(aggregationRecord.getAggregationValue() < c)result = aggregationRecord.getAggregationValue();
						else result = c;
				}
				if(viewMode.equals(ViewMode.AGGREGATION_MAX)){
					if(aggregationRecord.getAggregationValue() > c)result = aggregationRecord.getAggregationValue();
					else result = c;
				}				
				viewRecord.setAggregatedValue(result);


				
				/** Code with test and set, signature testing */
				succeed = view.updateTAS(getVMName(), x, viewRecord, signature);
				
				/** Code without Test and set. Provokes errors during concurrent updates*/
//				view.put(getVMName(), x, updateColumn);
//				succeed = true;
				System.out.println(getVMName()+": Updating ViewTableRecord [key="+x+", value="+c+"-->"+(result)+"]");
				
			}else{

				Signature newSignature = generateSignature(null, eid);
				
				if(viewMode.equals(ViewMode.AGGREGATION_SUM))
				viewRecord = new ViewRecord(aggregationRecord.getAggregationValue());
				
				if(viewMode.equals(ViewMode.AGGREGATION_COUNT))
				viewRecord = new ViewRecord(1);
				
				
				viewRecord.setSignature(newSignature);
				succeed = view.insertTAS(getVMName(), x, viewRecord);		
				System.out.println(getVMName()+": Inserting ViewTableRecord [key="+x+", value="+aggregationRecord.getAggregationValue()+"]");
			}

			if(!succeed)System.out.println("Update failed");
			 
			
		}while(!succeed);
		
	}	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateInsert(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateInsertProjection(String k, IBaseRecordProjection projectionRecord, long eid)throws RemoteException {
		

		
		
		boolean succeed = false;
//		boolean isMatching = baseTableRecord.isMatching(400, IProjectionRecord.EQUAL);				

		do{
		
			boolean foundEntry = read(k);
			
			if(foundEntry){
				

				if(hasProcessed(signature, eid)){
					System.out.println("Update key="+k+", column="+projectionRecord+", timestamp="+eid+" rejected --> It has already been processed");
					break;
				}
				
				Signature newSignature = generateSignature(signature, eid);
				projectionRecord.setSignature(newSignature);
				
				System.out.println(getVMName()+": Updating ViewTableRecord Projection [key="+k+", value="+ projectionRecord+"]");
				
				/** Code with test and set, signature testing */
				succeed = view.updateTAS(getVMName(), k, projectionRecord, signature);
				
				/** Code without Test and set. Provokes errors during concurrent updates*/
//				view.put(getVMName(), x, updateColumn);
//				succeed = true;

				
			}else{


				Signature newSignature = generateSignature(null, eid);
			
				projectionRecord.setSignature(newSignature);
				succeed = view.insertTAS(getVMName(), k, projectionRecord);	
				
				System.out.println(getVMName()+": Inserting ViewTableRecord Projection [key="+k+", value="+projectionRecord+"]");
			}

			if(!succeed)System.out.println("Update failed");
			 
			
		}while(!succeed);
		
	}	
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateInsert(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateInsertJoin(String k, IBaseRecord joinRecord, long eid)throws RemoteException {
		
		
		boolean succeed = false;
		IViewRecordJoin viewRecord = null;

		do{
		
			boolean foundEntry = read(k);
			
				
//				viewRecord = (IViewRecordJoin)updateColumn;
				

//				if(hasProcessed(signature, eid)){
//					System.out.println("Update key="+k+", column="+joinRecord+", timestamp="+eid+" rejected --> It has already been processed");
//					break;
//				}
				
				Signature newSignature = generateSignature(signature, eid);
//				viewRecord.setSignature(newSignature);

				IBaseRecordJoinLeft baseRecordLeft=null;
				IBaseRecordJoinRight baseRecordRight=null;
				
				if(joinRecord instanceof IBaseRecordJoinLeft){
					
					baseRecordLeft = (IBaseRecordJoinLeft)joinRecord;
						
					try {
						baseRecordRight = (IBaseRecordJoinRight)storage.get("jointableright", baseRecordLeft.getForeignKey());
					} catch (Exception e) {
		
						e.printStackTrace();
					}
					
					
				}
				
				if(joinRecord instanceof IBaseRecordJoinRight){
					
					baseRecordRight = (IBaseRecordJoinRight)joinRecord;
						
					try {
						baseRecordLeft = (IBaseRecordJoinLeft)storage.get("jointableleft", k);
					} catch (Exception e) {
		
						e.printStackTrace();
					}
				}
				
				viewRecord = new ViewRecord(baseRecordLeft.getForeignKey(), baseRecordRight.getValue());
				
				succeed = view.insertTAS(getVMName(), k, viewRecord);	
					

			if(!succeed)System.out.println("Update failed");
			 
			
		}while(!succeed);
		
	}		
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateDelete(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateDeleteAggregation(String k, IBaseRecordAggregation aggregationRecord, long eid, ViewMode viewMode)throws RemoteException {
		
		String x = aggregationRecord.getAggregationIdentifier();
		
		
		boolean succeed = false;
		IViewRecordAggregation viewRecord = null;

		do{
		
			boolean foundEntry = read(x);
			
			if(foundEntry){

				viewRecord = (IViewRecordAggregation)updateColumn;
				int c = viewRecord.getAggregatedValue();

				if(hasProcessed(signature, eid)){
					System.out.println("Update key="+k+", column="+aggregationRecord+", timestamp="+eid+" rejected --> It has already been processed");
					break;
				}
				
				Signature newSignature = generateSignature(signature, eid);
				viewRecord.setSignature(newSignature);

				
				int result=0;
				if(viewMode.equals(ViewMode.AGGREGATION_SUM))result = c - aggregationRecord.getAggregationValue();
				if(viewMode.equals(ViewMode.AGGREGATION_COUNT))result = c - 1;
				if(viewMode.equals(ViewMode.AGGREGATION_MIN)){
					if(aggregationRecord.getAggregationValue() == c)System.out.println("Neuen MIN-Wert ermitteln");
					else result = c;
				}
				if(viewMode.equals(ViewMode.AGGREGATION_MAX)){
					if(aggregationRecord.getAggregationValue() == c)System.out.println("Neuen MAX-Wert ermitteln");
					else result = c;
				}						
				viewRecord.setAggregatedValue(result);
				
				
				if(viewRecord.getAggregatedValue() == 0){
					succeed = view.deleteTAS(getVMName(), x, signature);
					if(succeed)System.out.println(getVMName()+": Deleting ViewTableRecord [key="+k+", value="+aggregationRecord+"]");
					
				}else{
					
					/** Code with test and set, signature testing */
					succeed = view.updateTAS(getVMName(), x, viewRecord, signature);
					if(succeed)System.out.println(getVMName()+": Updating ViewTableRecord [key="+x+", value="+c+"-->"+(result)+"]");
					
					/** Code without test and set. Provokes errors during concurrent updates*/
//					view.put(getVMName(), x, updateColumn);
//					succeed = true;
				}
				
			}else{
				

				Signature newSignature = generateSignature(null, eid);
				viewRecord.setSignature(newSignature);
				succeed = view.insertTAS(getVMName(), x, viewRecord);		
				
			}

			if(!succeed)System.out.println("Update failed");
			
			
		}while(!succeed);
		
	}		
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateDelete(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateDeleteProjection(String k, IBaseRecordProjection projectionRecord, long eid)throws RemoteException {
		
		
		boolean succeed = false;

		do{
		
			boolean foundEntry = read(k);

			if(foundEntry){


				if(hasProcessed(signature, eid)){
					System.out.println("Update key="+k+", column="+projectionRecord+", timestamp="+eid+" rejected --> It has already been processed");
					break;
				}
				
				Signature newSignature = generateSignature(signature, eid);

				projectionRecord.setSignature(newSignature);

				
				System.out.println(getVMName()+": Deleting ViewTableRecord Projection [key="+k+", value="+projectionRecord+"]");
				

				succeed = view.deleteTAS(getVMName(), k, signature);

				
			}else{
				

				
				System.out.println(getVMName()+": Record not in View Projection [key="+k+", value="+projectionRecord+"]");
//				Signature newSignature = generateSignature(null, eid);
//				updateColumn.setSignature(newSignature);
//				succeed = view.updateTAS(getVMName(), k, updateColumn);		
				succeed = true;
			}

			if(!succeed)System.out.println("Update failed");
//			else System.out.println(getVMName()+": Updating ViewTableRecord [key="+x+", value="+c+"-->"+(c - aggregationRecord.getAggregationValue())+"]");
			
		}while(!succeed);
		
	}	
	
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateDelete(java.lang.String, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateDeleteJoin(String k, IBaseRecord joinRecord, long eid)throws RemoteException {
		
		
		boolean succeed = false;

		do{
		
			boolean foundEntry = read(k);

			if(foundEntry){


				if(hasProcessed(signature, eid)){
					System.out.println("Update key="+k+", column="+joinRecord+", timestamp="+eid+" rejected --> It has already been processed");
					break;
				}
				
//				Signature newSignature = generateSignature(signature, eid);


				
				System.out.println(getVMName()+": Deleting ViewTableRecord join [key="+k+", value="+joinRecord+"]");
				

				succeed = view.deleteTAS(getVMName(), k, signature);

				
			}else succeed = true;

			if(!succeed)System.out.println("Update failed");

			
		}while(!succeed);
		
	}
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#propagateUpdate(java.lang.String, de.webdataplatform.storage.BaseTableColumn, de.webdataplatform.storage.BaseTableColumn, long)
	 */
	public void propagateUpdateAggregation(String k, IBaseRecordAggregation oldBaseTableRecord, IBaseRecordAggregation newBaseTableRecord, long eid, ViewMode viewMode)throws RemoteException {
		
		
		String xold = null;
		if(oldBaseTableRecord != null)xold = oldBaseTableRecord.getAggregationIdentifier();
		String xnew = newBaseTableRecord.getAggregationIdentifier();
		
		if(xold != null && !xold.equals(xnew)){
			System.out.println("propagateDelete: key="+k+", oldBaseTableColumn="+oldBaseTableRecord+", Eid="+eid);
			propagateDeleteAggregation(k, oldBaseTableRecord, eid, viewMode);
				
		}
		System.out.println("propagateInsert: key="+k+", newBaseTableColumn="+newBaseTableRecord+", Eid="+eid);
		propagateInsertAggregation(k, newBaseTableRecord,eid, viewMode);
		
		
	}
	

	
	public void propagateUpdateProjection(String k, IBaseRecordProjection oldProjectionRecord, IBaseRecordProjection newProjectionRecord, long eid)throws RemoteException {
		
		int projectionCondition = 250;
		
		boolean isMatching = newProjectionRecord.isMatching(projectionCondition, IBaseRecordProjection.GREATER_THAN);
		
		if(isMatching){
			
			
			
			System.out.println("New record is matching criteria: >"+projectionCondition);
			
			System.out.println("propagateInsert: key="+k+", newBaseTableColumn="+newProjectionRecord+", Eid="+eid);
			propagateInsertProjection(k, newProjectionRecord,eid);
		}
		else{
			
			System.out.println("New record is NOT matching criteria: >"+projectionCondition+" "+newProjectionRecord);
			
			System.out.println("propagateDelete: key="+k+", oldBaseTableColumn="+oldProjectionRecord+", Eid="+eid);
			propagateDeleteProjection(k, oldProjectionRecord, eid);
			
		}

		
		
		
		
	}
	
	public void propagateUpdateJoin(String k, IBaseRecordJoin oldJoinRecord, IBaseRecordJoin newJoinRecord, long eid)throws RemoteException {
		

		
		String xold = null;
		
		
		
		if((newJoinRecord != null && newJoinRecord.isLeftSide()) || (oldJoinRecord != null && oldJoinRecord.isLeftSide())){
			
			/** Update */ 
			if(newJoinRecord != null && oldJoinRecord != null && !oldJoinRecord.equals(newJoinRecord)){
				
				System.out.println("Update in left table: updating view record");
				propagateDeleteJoin(k, (IBaseRecordJoinLeft)oldJoinRecord, eid);
				propagateInsertJoin(k, (IBaseRecordJoinLeft)newJoinRecord, eid);
					
			}
			
			/** Insert */
			if(newJoinRecord != null && oldJoinRecord == null){
				
				System.out.println("Insert in left table: updating view record");		
				propagateInsertJoin(k, (IBaseRecordJoinLeft)newJoinRecord,eid);
				
			}
			
			/** Delete */
			if(newJoinRecord == null && oldJoinRecord != null){
				
				System.out.println("Delete in left table: updating view record");	
				propagateDeleteJoin(k, (IBaseRecordJoinLeft)oldJoinRecord, eid);
				
			}
		}

		
		if((newJoinRecord != null && newJoinRecord.isRightSide()) || (oldJoinRecord != null && oldJoinRecord.isRightSide())){
			
			/** Update */ 
			if(newJoinRecord != null && oldJoinRecord != null && !oldJoinRecord.equals(newJoinRecord)){

				System.out.println("Update in right table: updating view record");
				propagateDeleteJoin(k, (IBaseRecordJoinRight)oldJoinRecord, eid);
				propagateInsertJoin(k, (IBaseRecordJoinRight)newJoinRecord, eid);
					
			}

			
			/** Insert */
			if(newJoinRecord != null && oldJoinRecord == null){

				System.out.println("Insert in right table: do nothing");
			}
			
			/** Delete */
			if(newJoinRecord == null && oldJoinRecord != null){
				
				System.out.println("Delete in right table: do nothing");
				
			}
		}
		
		
		
		
	}	
	
	

	
//	@Override
//	public void notifyVM() throws RemoteException {
//		
//
//		pollUpdate();
//		
//	}
	
	
	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#pollUpdates()
	 */
	public synchronized void pollUpdates()throws RemoteException {
		
		
		ViewMode viewMode = ViewMode.AGGREGATION_COUNT;
		
		
		while(true){
			
			Update<IBaseRecord> update=null;
			try {
				update = storage.requestUpdate(getVMName());
			} catch (RemoteException e) {
	
				e.printStackTrace();
			}
			
			if(update != null){
				

//				System.out.println("Updating View in ViewMode: "+viewMode.getName());
				
				try {
					System.out.println("------------------------------------------------------------");
					System.out.println(getVMName()+": Propagting "+update);
					
					if(viewMode.equals(ViewMode.AGGREGATION_SUM) 
					   || viewMode.equals(ViewMode.AGGREGATION_COUNT)
					   || viewMode.equals(ViewMode.AGGREGATION_MIN)
					   || viewMode.equals(ViewMode.AGGREGATION_MAX))
							propagateUpdateAggregation(update.getKey(), (IBaseRecordAggregation)update.getOldColumn(), (IBaseRecordAggregation)update.getNewColumn(), update.getEid(), viewMode);

					
					if(viewMode.equals(ViewMode.SELECTION))
							propagateUpdateProjection(update.getKey(), (IBaseRecordProjection)update.getOldColumn(), (IBaseRecordProjection)update.getNewColumn(), update.getEid());
					
					
					if(viewMode.equals(ViewMode.JOIN))
						propagateUpdateJoin(update.getKey(), (IBaseRecordJoin)update.getOldColumn(), (IBaseRecordJoin)update.getNewColumn(), update.getEid());
					
					
					/** Produce duplicates for testing reasons*/
//					System.out.println(getVMName()+": Propagting duplication"+update);
//					propagateUpdate(update.getKey(), update.getOldColumn(), update.getNewColumn(), update.getEid());
					
				} catch (RemoteException e) {
					
					e.printStackTrace();
				}
			}else{
			
			}
		}
		
	}

	
	
	/**
	 * Reads the values from the view table for a given key.
	 * 
	 * @param key
	 * @return
	 */
	public boolean read(String key){
		
		IViewRecord viewTableColumn=null;
		try {
			viewTableColumn = view.get(key);
		} catch (RemoteException e) {

			e.printStackTrace();
		}
		
		if(viewTableColumn != null){
			
			updateColumn = viewTableColumn;
			signature = viewTableColumn.getSignature();
			return true;
		}else{
			
			updateColumn = null;
			signature = null;		
			
		}
		
		return false;
		
	}
	
	/**
	 * Checks  if a timestamp of an Update so has already been processed. If the 
	 * timestamp is already included in the signature the Update is denied
	 * 
	 * @param signature
	 * @param eid
	 * @return
	 */
	public boolean hasProcessed(Signature signature, long eid){
		
		
		if(signature != null && signature.getEids().contains(eid))return true;
		
		return false;
	}

	/**
	 * Generates a new signature with a given timestamp(eid). If the the parameter
	 * signature is null a new signature is generated and the timestamp(eid) is added. If the 
	 * parameter signature is not null the parameter is copied and the timestamp(eid) is added to it.
	 * 
	 * @param signature
	 * @param eid
	 * @return
	 */
	public Signature generateSignature(Signature signature, long eid){
		
		Signature newSignatur = new Signature();
		if(signature != null){
			for(Long tempeid : signature.getEids()){				
				newSignatur.addEid(tempeid);				
			}	
		}

		newSignatur.addEid(eid);
		
		return newSignatur;
	}





	@Override
	public String toString() {
		return getVMName();
	}


	public String getIp() {
		return ip;
	}


	public void setIp(String ip) {
		this.ip = ip;
	}


	public int getPort() {
		return port;
	}


	public void setPort(int port) {
		this.port = port;
	}



	
	
	






	
	
}
