package de.webdataplatform.system;

public class Event {

	public final static String REGIONSERVER_ADDED="rsAdded";
	public final static String REGIONSERVER_CRASHED="rsCrashed";
	public final static String REGIONSERVER_SHUTDOWN="rsShutdown";
	public final static String WRITEAHEADLOG_REPLAYED="walReplayed";
//	public final static String PERIODIC_LOADBALANCING="periodicLoadBalancing";
	
	public final static String VIEWMANAGER_ADDED="vmAdded";
	public final static String VIEWMANAGER_ASSIGNED="vmAssigned";
	public final static String VIEWMANAGER_WITHDRAWN="vmWithdrawn";
	public final static String CRASHED_VIEWMANAGER_WITHDRAWN="crashedVmWithdrawn";
	public final static String VIEWMANAGER_REASSIGNED="vmReassigned";
	public final static String VIEWMANAGER_SHUTDOWN="vmShutdown";
	public final static String VIEWMANAGER_CRASHED="vmCrashed";
	
	public final static String STATUS_REPORT_VIEWMANAGER="statusReportVm";
	public final static String STATUS_REPORT_REGIONSERVER="statusReportRs";
	public final static String VIEWMANAGER_MARKER_RECEIVED="mRec";
	
	public final static String BALANCE_LOAD="balanceLoad";
	public final static String PROCESS_FINISHED="pFinished";
	
	private String type;
	
	private String viewManager;
	
	private String regionServer;


	public Event(String type, String viewManager) {
		super();
		this.viewManager = viewManager;
		this.type = type;
	}

	public Event(String type, String viewManager, String regionServer) {
		super();
		this.type = type;
		this.viewManager = viewManager;
		this.regionServer = regionServer;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getViewManager() {
		return viewManager;
	}

	public void setViewManager(String viewManager) {
		this.viewManager = viewManager;
	}

	public String getRegionServer() {
		return regionServer;
	}

	public void setRegionServer(String regionServer) {
		this.regionServer = regionServer;
	}

	@Override
	public String toString() {
		return "Event [type=" + type + ", viewManager=" + viewManager
				+ ", regionServer=" + regionServer + "]";
	}
	
	
	
	
	
}
