package de.webdataplatform.test;

import java.util.ArrayList;
import java.util.List;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.ssh.SSHConnection;
import de.webdataplatform.ssh.SSHService;

public class TestSSH {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		
		Log log = new Log("yeah");
		SystemConfig.load(log);
		NetworkConfig.load(log);
		
		SSHConnection sshC = new SSHConnection("deltaserver1", 22, "jan", "MTVdvrDV$15");
		
//		List<String> startupCommand = new ArrayList<String>();
//		startupCommand.add("cd /");
//		startupCommand.add("cd "+SystemConfig.DIRECTORY_HDFS+"bin");
//		startupCommand.add("./start-dfs.sh");
//		
//		SSHService.sendCommand(log, NetworkConfig.HBASE_MASTER.getSshConnection(), startupCommand);

		
		List<String> startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		startupCommand.add("java  -Xmx1000m -cp vmsystem.jar de.webdataplatform.viewmanager.TestViewManager vm1 deltaserver1 4445 4446");
		SSHService.sendCommand(log, sshC, startupCommand);
		
/*
//		
		startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		startupCommand.add("nohup java  -Xmx1000m -cp vmsystem.jar de.webdataplatform.viewmanager.TestViewManager vm2 deltaserver1 4447 4448  &");
		SSHService.sendCommand(log, sshC, startupCommand);
		
		startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		startupCommand.add("nohup java  -Xmx1000m -cp vmsystem.jar de.webdataplatform.viewmanager.TestViewManager vm3 deltaserver1 4449 4450  &");
		SSHService.sendCommand(log, sshC, startupCommand);
		
		
		startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		startupCommand.add("nohup java  -Xmx1000m -cp vmsystem.jar de.webdataplatform.viewmanager.TestViewManager vm4 deltaserver1 4451 4452  &");
		SSHService.sendCommand(log, sshC, startupCommand);
		
		
		startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		startupCommand.add("nohup java  -Xmx1000m -cp vmsystem.jar de.webdataplatform.viewmanager.TestViewManager vm5 deltaserver1 4453 4454  &");
		SSHService.sendCommand(log, sshC, startupCommand);
		
		
		startupCommand = new ArrayList<String>();
		startupCommand.add("cd /");
		startupCommand.add("cd "+SystemConfig.DIRECTORY_VMSYSTEM);
		startupCommand.add("nohup java  -Xmx1000m -cp vmsystem.jar de.webdataplatform.viewmanager.TestViewManager vm6 deltaserver1 4455 4456  &");
		SSHService.sendCommand(log, sshC, startupCommand);*/
		
		SSHService.closeSessions();
	}

}
