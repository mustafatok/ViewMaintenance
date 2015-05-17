package de.webdataplatform.viewmanager;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import de.webdataplatform.log.Log;
import de.webdataplatform.log.StatisticLog;
import de.webdataplatform.message.Server;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.storage.IBaseRecord;
import de.webdataplatform.storage.IBaseRecordAggregation;
import de.webdataplatform.storage.IBaseRecordJoin;
import de.webdataplatform.storage.IBaseRecordProjection;
import de.webdataplatform.view.Signature;
import de.webdataplatform.view.ViewMode;
import de.webdataplatform.viewmanager.processing.PreProcessing;
import de.webdataplatform.viewmanager.processing.Processing;
import de.webdataplatform.zookeeper.IZooKeeperService;
import de.webdataplatform.zookeeper.ZookeeperService;

public class ViewManager  implements Serializable, IViewManager{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 201204933834777882L;
	


	
	
	private String condition;
	
	
	private SystemID systemID;
	
	private IZooKeeperService zooKeeperService;
	
	private Server updateServer ;
	
	private Server messageServer ;
	
	
	
	private Queue<String> incomingMessages;
	
	private Queue<String> incomingUpdates;
	
	private Queue<BaseTableUpdate> preprocessedUpdates;
	
	
	
	private VMController vmController;
	
	private PreProcessing preProcessing ;
	
	private Processing processing;
	
	
	private SystemID master;
	
	private SystemID regionServer;
	
	private AtomicLong updatesReceived;
	

	private Log log;
	
	public ViewManager(String name, String ip, int updatePort, int messagePort){

		this.systemID = new SystemID(name, ip, messagePort, updatePort);
		

	}
	
	public ViewManager(Log log, String name, String ip, int updatePort, int messagePort){

		this.log = log;
		this.systemID = new SystemID(name, ip, messagePort, updatePort);
		

	}


	/*
	 * (non-Javadoc)
	 * @see de.webdataplatform.viewmanager.IViewManager#getVMName()
	 */
	public String getVMName() {
		return systemID.getName();
	}

	
	
	public void initialize() {
		
//		log = new Log(systemID.getName()+".log");
		StatisticLog.name = systemID.getName();
		StatisticLog.targetDirectory = "logs/";
		
		log.info(this.getClass(),"initializing new View Manager with config:");
		log.info(this.getClass(),"name: "+systemID.getName());
		log.info(this.getClass(),"address: "+systemID.getIp());
		log.info(this.getClass(),"port: "+systemID.getMessagePort());

		
		/** components for inter-component communication */
		
		incomingMessages = new ConcurrentLinkedQueue<String>();
		
		messageServer = new Server(new ServerMessageHandlerFactory(log, incomingMessages), systemID.getMessagePort());
		
		/** components for base table updates */
		
		incomingUpdates = new ConcurrentLinkedQueue<String>();
		
		preprocessedUpdates = new ConcurrentLinkedQueue<BaseTableUpdate>();
				
		updatesReceived = new AtomicLong();
		
		updateServer = new Server(new ServerUpdateHandlerFactory(log, incomingUpdates, updatesReceived), systemID.getUpdatePort());

		/** components for update processing */
		
		preProcessing = new PreProcessing(log, incomingUpdates, preprocessedUpdates);
		
		processing = new Processing(log, preprocessedUpdates, getVMName());
		
		zooKeeperService = new ZookeeperService(log);
		
		vmController = new VMController(log, this);
		
		Thread controllerThread = new Thread(vmController);
		
		controllerThread.start();
		
		/*Scanner scanner = new Scanner(System.in);
		
		log.info(this.getClass(),"Type quit to remove view manager");

        
        String eingabe = scanner.next();
        
//        scanner.match();
        
        log.info(this.getClass(),"View Manager wird beendet");
        
        scanner.close();
        
		setCondition(Condition.SHUTTING_DOWN);
		if(getRegionServer() != null)
			vmController.withdrawViewManager(getRegionServer().toString(), getSystemID().toString());
		else vmController.viewManagerShutdown(master.toString(), systemID.toString());
		
		*/

	}



	public IZooKeeperService getZooKeeperService() {
		return zooKeeperService;
	}


	public void setZooKeeperService(IZooKeeperService zooKeeperService) {
		this.zooKeeperService = zooKeeperService;
	}


	public Server getUpdateServer() {
		return updateServer;
	}


	public void setUpdateServer(Server updateServer) {
		this.updateServer = updateServer;
	}


	public Server getMessageServer() {
		return messageServer;
	}


	public void setMessageServer(Server messageServer) {
		this.messageServer = messageServer;
	}


	public Queue<String> getIncomingMessages() {
		return incomingMessages;
	}


	public void setIncomingMessages(Queue<String> incomingMessages) {
		this.incomingMessages = incomingMessages;
	}


	public Queue<String> getIncomingUpdates() {
		return incomingUpdates;
	}


	public void setIncomingUpdates(Queue<String> incomingUpdates) {
		this.incomingUpdates = incomingUpdates;
	}


	public Queue<BaseTableUpdate> getPreprocessedUpdates() {
		return preprocessedUpdates;
	}


	public void setPreprocessedUpdates(Queue<BaseTableUpdate> preprocessedUpdates) {
		this.preprocessedUpdates = preprocessedUpdates;
	}


	public VMController getVmController() {
		return vmController;
	}


	public void setVmController(VMController vmController) {
		this.vmController = vmController;
	}


	public PreProcessing getPreProcessing() {
		return preProcessing;
	}


	public void setPreProcessing(PreProcessing preProcessing) {
		this.preProcessing = preProcessing;
	}


	public Processing getProcessing() {
		return processing;
	}


	public void setProcessing(Processing processing) {
		this.processing = processing;
	}


	public static long getSerialversionuid() {
		return serialVersionUID;
	}


	

	public SystemID getMaster() {
		return master;
	}


	public void setMaster(SystemID master) {
		this.master = master;
	}


	public SystemID getRegionServer() {
		return regionServer;
	}


	public void setRegionServer(SystemID regionServer) {
		this.regionServer = regionServer;
	}


	public SystemID getSystemID() {
		return systemID;
	}


	public void setSystemID(SystemID systemID) {
		this.systemID = systemID;
	}


	@Override
	public void register() throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void deregister() throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void processUpdate() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void lastProcessedUpdate() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateInsertAggregation(String k,
			IBaseRecordAggregation baseTableColumn, long eid, ViewMode viewMode)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateUpdateAggregation(String k,
			IBaseRecordAggregation oldBaseTableColumn,
			IBaseRecordAggregation newBaseTableColumn, long eid,
			ViewMode viewMode) throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateDeleteAggregation(String k,
			IBaseRecordAggregation baseTableColumn, long eid, ViewMode viewMode)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateInsertProjection(String k,
			IBaseRecordProjection projectionRecord, long eid)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateUpdateProjection(String k,
			IBaseRecordProjection oldProjectionRecord,
			IBaseRecordProjection newProjectionRecord, long eid)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateDeleteProjection(String k,
			IBaseRecordProjection projectionRecord, long eid)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateInsertJoin(String k, IBaseRecord joinRecord, long eid)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateDeleteJoin(String k, IBaseRecord joinRecord, long eid)
			throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void propagateUpdateJoin(String k, IBaseRecordJoin oldJoinRecord,
			IBaseRecordJoin newJoinRecord, long eid) throws RemoteException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public boolean read(String key) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean hasProcessed(Signature signature, long eid) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public Signature generateSignature(Signature signature, long eid) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((systemID == null) ? 0 : systemID.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ViewManager other = (ViewManager) obj;
		if (systemID == null) {
			if (other.systemID != null)
				return false;
		} else if (!systemID.equals(other.systemID))
			return false;
		return true;
	}


	public String getCondition() {
		return condition;
	}


	public void setCondition(String condition) {
		this.condition = condition;
	}


	public AtomicLong getUpdatesReceived() {
		return updatesReceived;
	}


	public void setUpdatesReceived(AtomicLong updatesReceived) {
		this.updatesReceived = updatesReceived;
	}


	@Override
	public String toString() {
		return "ViewManager [systemID=" + systemID + "]";
	}









	
	
}
