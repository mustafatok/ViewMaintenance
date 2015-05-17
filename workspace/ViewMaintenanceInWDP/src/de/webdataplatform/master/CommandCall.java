package de.webdataplatform.master;

import de.webdataplatform.system.Command;

public class CommandCall {


	private ICommandCaller commandCaller;
	
	private Command command;

	public CommandCall(ICommandCaller commandCaller, Command command) {
		super();
		this.commandCaller = commandCaller;
		this.command = command;
	}

	public ICommandCaller getCommandCaller() {
		return commandCaller;
	}

	public void setCommandCaller(ICommandCaller commandCaller) {
		this.commandCaller = commandCaller;
	}

	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}

	@Override
	public String toString() {
		return "CommandCall [commandCaller=" + commandCaller + ", command="
				+ command + "]";
	}
	

	
	
	
}
