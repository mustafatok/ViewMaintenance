package de.webdataplatform.system;

public class Command {


	public final static String ASSIGN_VIEWMANAGER="assignVm";
	public final static String WITHDRAW_VIEWMANAGER="withdrawVm";
	public final static String WITHDRAW_CRASHED_VIEWMANAGER="withdrawCrashedVm";
	public final static String REASSIGN_VIEWMANAGER="reassignVm";
	public final static String SHUTDOWN_VIEWMANAGER="shutdownVm";
	

	
	public final static String REPLAY_WRITEAHEADLOG="replayWAL";
	public final static String START_UPDATE_PROCESSING="startUP";
	
//	public final static String STATUS_REPORT_VIEWMANAGER="statusReportViewManager";
//	public final static String STATUS_REPORT_REGIONSERVER="statusReportRegionServer";
	
//	public final static String SHUTDOWN_REQUEST_VIEWMANAGER="shutdownRequestViewmanager";
//	public final static String VIEWMANAGER_REMOVED="viewManagerRemoved";

	
	private String type;

	private String viewManager;
	
	private String regionServer;

	private String content;
	
	private long executionTimestamp;
	
	private int retries=0;
	
	
	
	public Command(String type, String viewManager) {
		super();
		this.viewManager = viewManager;
		this.type = type;
	}

	public Command(String type, String viewManager, String regionServer) {
		super();
		this.viewManager = viewManager;
		this.type = type;
		this.regionServer = regionServer;
	}

	public Command(String type, String viewManager, String regionServer, String content) {
		super();
		this.viewManager = viewManager;
		this.type = type;
		this.regionServer = regionServer;
		this.content = content;
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

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	

	
	
	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public long getExecutionTimestamp() {
		return executionTimestamp;
	}

	public void setExecutionTimestamp(long executionTimestamp) {
		this.executionTimestamp = executionTimestamp;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	@Override
	public String toString() {
		return "Command [type=" + type + ", viewManager=" + viewManager
				+ ", regionServer=" + regionServer + "]";
	}


	
	

}
