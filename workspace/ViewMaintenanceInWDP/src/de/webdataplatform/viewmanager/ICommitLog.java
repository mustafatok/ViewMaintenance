package de.webdataplatform.viewmanager;

import java.util.Map;

import de.webdataplatform.storage.BaseTableUpdate;

public interface ICommitLog {
	
	public void createLogDirectory();
	
	public void createLogFile();
	
	public boolean openLogFile();
	
	public void closeLogFile();
	
	public void append(String vmName, BaseTableUpdate baseTableUpdate);
	
	public Map<String, Long> readHighestSeqNos();
	
	
	

}
