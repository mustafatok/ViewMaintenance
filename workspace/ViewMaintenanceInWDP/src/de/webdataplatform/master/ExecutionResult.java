package de.webdataplatform.master;

import de.webdataplatform.system.Command;

public class ExecutionResult {


	private Command command;

	public ExecutionResult(Command command) {
		super();
		this.command = command;
	}

	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}
	
	
	

}
