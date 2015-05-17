package de.webdataplatform.master;

import de.webdataplatform.system.Command;

public interface ICommandCaller {

	public void executeCommand(Command command)throws Exception;
	
	public void callbackExecuteCommand(Command command);
	
	public String toString();
	
	
}
