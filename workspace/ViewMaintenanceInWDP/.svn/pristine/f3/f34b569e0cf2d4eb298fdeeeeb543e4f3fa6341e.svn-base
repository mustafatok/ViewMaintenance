package de.webdataplatform.settings;

import de.webdataplatform.ssh.SSHConnection;

public class Node {

	private String ipAddress;
	
	private int messagePort;
	
	private int updatePort;

	private SSHConnection sshConnection;

	
	
	public Node(String ipAddress, int messagePort, int updatePort,
			SSHConnection sshConnection) {
		super();
		this.ipAddress = ipAddress;
		this.messagePort = messagePort;
		this.updatePort = updatePort;
		this.sshConnection = sshConnection;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public int getMessagePort() {
		return messagePort;
	}

	public void setMessagePort(int messagePort) {
		this.messagePort = messagePort;
	}

	public int getUpdatePort() {
		return updatePort;
	}

	public void setUpdatePort(int updatePort) {
		this.updatePort = updatePort;
	}

	public SSHConnection getSshConnection() {
		return sshConnection;
	}

	public void setSshConnection(SSHConnection sshConnection) {
		this.sshConnection = sshConnection;
	}

	@Override
	public String toString() {
		return "Node [ipAddress=" + ipAddress + ", messagePort=" + messagePort
				+ ", updatePort=" + updatePort + ", sshConnection="
				+ sshConnection + "]";
	}

	
	
	



}
