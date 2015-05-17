package de.webdataplatform.message;

import java.io.Serializable;

import de.webdataplatform.settings.SystemConfig;


public class Message implements Serializable{


	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9013050318641185187L;


	public Message(){
		
		
	}
	
	public Message(String updateString){
		
		String[] messageParts = updateString.replace(SystemConfig.MESSAGES_STARTSEQUENCE, "").replace(SystemConfig.MESSAGES_ENDSEQUENCE, "").split(SystemConfig.MESSAGES_SPLITSEQUENCE);
		
		component = messageParts[0].trim();
		
		name = messageParts[1].trim();
		
		operation = messageParts[2].trim();
		
		if(messageParts.length == 4)content = messageParts[3].trim();
		else content = "";
		
		

		
	}




	public Message(String component, String name, String operation, String content) {
		super();
		this.component = component;
		this.name = name;
		this.operation = operation;
		this.content = content;
	}

	private String component;
	
	private String name;
	
	private String operation;

	private String content;
	
	

	


	public String convertToString(){
		return SystemConfig.MESSAGES_STARTSEQUENCE+component+SystemConfig.MESSAGES_SPLITSEQUENCE+name+SystemConfig.MESSAGES_SPLITSEQUENCE+operation+SystemConfig.MESSAGES_SPLITSEQUENCE+content+SystemConfig.MESSAGES_ENDSEQUENCE;
	}


	

	public String getComponent() {
		return component;
	}

	public void setComponent(String component) {
		this.component = component;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	

	@Override
	public String toString() {
		return "Message [component=" + component + ", name=" + name
				+ ", operation=" + operation + ", content=" + content + "]";
	}

	public Message copy(){
		
		Message message = new Message();
		
		message.component = this.component;
		message.name = this.name;
		message.operation = this.operation;
		message.content = this.content;

		
		return message;
		
	}
	
	
	
	
}
