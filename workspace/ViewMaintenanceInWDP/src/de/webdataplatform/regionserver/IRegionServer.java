package de.webdataplatform.regionserver;

import de.webdataplatform.viewmanager.IViewManager;

public interface IRegionServer {

	
	public boolean addViewManager(IViewManager viewManager);
	
	public boolean removeViewManager(String viewManagerName);
	
	
	public boolean setTrigger();
	
	
}
