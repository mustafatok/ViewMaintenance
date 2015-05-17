package de.webdataplatform.master;

public interface IMaster {

	
	
	public void setTriggerRegionServer();
	
	public void setTriggerViewManager();
	
	public void callRegionServerAdded();
	
	public void callViewManagerAdded(String viewManager);
	
	public void callRegionServerRemoved();
	
	public void callViewManagerRemoved();
	
	public void callRegionServerCrashed();
	
	public void callViewManagerCrashed(String viewManager);
	
	public void addViewManager();
	
	public void assignViewManager();
	
	public void withdrawViewManager();
	
	public void reassignViewManager();
	
	public void removeViewManager();
	
	
	
	
	
	
	
}
