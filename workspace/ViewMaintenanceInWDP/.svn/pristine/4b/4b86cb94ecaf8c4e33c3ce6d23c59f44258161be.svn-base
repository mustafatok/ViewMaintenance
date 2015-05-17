package de.webdataplatform.regionserver;

import java.util.List;
import java.util.Map;
import java.util.Set;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.viewmanager.IViewManager;
import de.webdataplatform.viewmanager.ViewManager;

public class UpdateAssigner implements IUpdateAssigner{

	

	private ConsistentHash<ViewManager> consistentHashring;
	
	private Log log;
	
	public UpdateAssigner(Log log, List<ViewManager> viewManager){
		
		
		HashFunction hf = new MD5();
		consistentHashring = new ConsistentHash<ViewManager>(hf, SystemConfig.REGIONSERVER_MAXREPLICASHASHRING, viewManager);
		this.log = log;
		
	}




	
	@Override
	
	public void addViewManager(ViewManager viewManager) {

		consistentHashring.add(viewManager);
		log.info(this.getClass(),"vm added, number of view managers on hash ring: "+numOfVms());
	}


	@Override
	public void removeViewManager(ViewManager viewManager) {

		
		consistentHashring.remove(viewManager);
		log.info(this.getClass(),"vm removed, number of view managers on hash ring: "+numOfVms());
	}


	@Override
	public ViewManager assignUpdate(String key) {
		
		return consistentHashring.get(key);
	}


	public Set<ViewManager> getViewManager() {
		
		return consistentHashring.getNodes();
	}
	

	public int numOfVms(){
		
		return consistentHashring.numOfElements();
	}
	

}
