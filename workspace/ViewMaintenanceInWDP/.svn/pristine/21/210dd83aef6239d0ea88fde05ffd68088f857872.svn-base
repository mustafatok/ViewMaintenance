package de.webdataplatform.ssh;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;


public class SSHService {

	public static void retrieveFile(Log log, SSHConnection sshConnection, String sourceDirectory, String targetDirectory, String file){
		
		String SFTPHOST = sshConnection.getAddress();
		int SFTPPORT = sshConnection.getPort();
		String SFTPUSER = sshConnection.getUser();
		String SFTPPASS = sshConnection.getPassword();
//		System.out.println(sshConnection);
		

		Session session = null;
		Channel channel = null;
		ChannelSftp sftpChannel = null;
		
		 JSch jsch = new JSch();
	    try {
	    	if(SystemConfig.SSH_PASSWORDLESSLOGIN){
		    	jsch.setKnownHosts(SystemConfig.SSH_KNOWNHOSTS);
		    	jsch.addIdentity(SystemConfig.SSH_PRIVATEKEY);
	    	}
	        session = jsch.getSession(SFTPUSER, SFTPHOST, SFTPPORT);
	        session.setConfig("StrictHostKeyChecking", "no");
	        if(!SystemConfig.SSH_PASSWORDLESSLOGIN)session.setPassword(SFTPPASS);
	        session.connect();

	        channel = session.openChannel("sftp");
	        channel.connect();
	        sftpChannel = (ChannelSftp) channel;

	        sftpChannel.get(sourceDirectory+"/"+file, targetDirectory+"/"+file);
	        sftpChannel.exit();
	        session.disconnect();
	        
	    } catch (JSchException e) {
			log.error(SSHService.class, e);
			log.info(SSHService.class, sshConnection.toString());
	    } catch (SftpException e) {
			log.error(SSHService.class, e);
			log.info(SSHService.class, sshConnection.toString());
	    } catch (Exception e){
			log.error(SSHService.class, e);
			log.info(SSHService.class, sshConnection.toString());
	    }
		
	}
	
	public static void copyFile(Log log, SSHConnection sshConnection, String sourceDirectory, String targetDirectory, String file) {
		
		String SFTPHOST = sshConnection.getAddress();
		int SFTPPORT = sshConnection.getPort();
		String SFTPUSER = sshConnection.getUser();
		String SFTPPASS = sshConnection.getPassword();
//		System.out.println(sshConnection);
		

		Session session = null;
		Channel channel = null;
		ChannelSftp channelSftp = null;
		
		try {
		    JSch jsch = new JSch();
		    
		    if(SystemConfig.SSH_PASSWORDLESSLOGIN){
				jsch.setKnownHosts(SystemConfig.SSH_KNOWNHOSTS);
				jsch.addIdentity(SystemConfig.SSH_PRIVATEKEY);    
		    }	
			session = jsch.getSession(SFTPUSER, SFTPHOST, SFTPPORT);
			if(!SystemConfig.SSH_PASSWORDLESSLOGIN)session.setPassword(SFTPPASS);
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			channelSftp = (ChannelSftp) channel;
	//		channelSftp.cd("/usr/local/hbase-0.94.6.1/conf");
			channelSftp.cd(targetDirectory);
	
			File f1 = new File(sourceDirectory+"/" + file);
			channelSftp.put(new FileInputStream(f1), f1.getName(), ChannelSftp.OVERWRITE);
	
	
			channelSftp.exit();
			
			session.disconnect();
			
		} catch (Exception e) {
			
			log.error(SSHService.class, e);
			log.info(SSHService.class, sshConnection.toString());
		}
	
	}

	static Map<String, Session> sessions = new HashMap<String, Session>();
	
	public static List<String> sendCommand(Log log, SSHConnection sshConnection, List<String> commands) {
		
		String SFTPHOST = sshConnection.getAddress();
		int SFTPPORT = sshConnection.getPort();
		String SFTPUSER = sshConnection.getUser();
		String SFTPPASS = sshConnection.getPassword();
//		System.out.println(sshConnection);
		List<String> result = new ArrayList<String>();

		Session session = null;
		Channel channel = null;
		ChannelSftp sftpChannel = null;

		
		
		try {
			
			if(sessions.get(SFTPHOST)==null || !sessions.get(SFTPHOST).isConnected()){
				JSch jsch = new JSch();
				 if(SystemConfig.SSH_PASSWORDLESSLOGIN){
					jsch.setKnownHosts(SystemConfig.SSH_KNOWNHOSTS);
					jsch.addIdentity(SystemConfig.SSH_PRIVATEKEY);
				 }	
				session = jsch.getSession(SFTPUSER, SFTPHOST, SFTPPORT);
				if(!SystemConfig.SSH_PASSWORDLESSLOGIN)session.setPassword(SFTPPASS);
				java.util.Properties config = new java.util.Properties();
				config.put("StrictHostKeyChecking", "no");
				session.setConfig(config);
				session.connect();
				sessions.put(SFTPHOST, session);
			}else{
				session = sessions.get(SFTPHOST);
			}
			
			
			long time = new Date().getTime();
			
			String command ="nohup sh -c \"";
			
			int x = 1;
            for (String commandx : commands) {
//           	 log.info(SSHService.class, "command: "+command);
            	command += commandx;
            	if(x != commands.size())command += " && ";
            	x++;
			 }
            command += "\" > /dev/null 2>&1 &";
//            System.out.println(command);
			
			ChannelExec channelExec=(ChannelExec) session.openChannel("exec");
			   BufferedReader in=new BufferedReader(new InputStreamReader(channelExec.getInputStream()));
			   channelExec.setCommand(command);
			   channelExec.connect();

//			   String msg=null;
//			   while((msg=in.readLine())!=null){
//			    System.out.println(msg);
//			   }

			   channelExec.disconnect();
			   
			
			/*channel=session.openChannel("shell");
            OutputStream ops = channel.getOutputStream();
            PrintStream ps = new PrintStream(ops, true);

             channel.connect();


//             ps.println("/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'");
             
			
			
			
             for (String command : commands) {
//            	 log.info(SSHService.class, "command: "+command);
            	 ps.println(command);
			 }
             


             ps.close();

             InputStream in=channel.getInputStream();
             byte[] bt=new byte[1024];
             int f = 0;
             String str=null;
             boolean transmissionStarted=false;
             while(true)
             {

            	 System.out.println("------");
            	 int x = in.available(); 
            	 System.out.println(x);
	             while(x>0)
	             {
		             int i=in.read(bt, 0, 1024);
		             System.out.println("i:"+i);
		             if(i<0)
		              break;
		             str=new String(bt, 0, i);
		             System.out.println(str);
		             transmissionStarted=true;
		                Thread.sleep(100);
		                x = in.available();
		          }
		             
	             if(x == 0 && transmissionStarted)
	             {
	
	                 break;
	             }
	          
//	             System.out.println("str:"+str);
	             System.out.println("blabla"+f);
	             f++;
//	             Thread.sleep(1000);
//	             channel.disconnect();
//	             session.disconnect();   
             }
//             boolean isResult=false;
             for(String line : str.split("\n")){


            	 result.add(line);
//            	 log.info(SSHService.class, line);

             }
             log.info(SSHService.class, "-----------------------------------------");

 
			 channel.disconnect();*/
	        
			   System.out.println("time: "+(new Date().getTime() - time+" ms"));
		} catch (Exception e) {
			
			log.error(SSHService.class, e);
			log.info(SSHService.class, sshConnection.toString());

		}
		
		return null;

	}
	
	public void executeCommand(Session session, String command)throws Exception{
	   ChannelExec channel=(ChannelExec) session.openChannel("exec");
	   BufferedReader in=new BufferedReader(new InputStreamReader(channel.getInputStream()));
	   channel.setCommand(command);
	   channel.connect();

	   String msg=null;
	   while((msg=in.readLine())!=null){
	    System.out.println(msg);
	   }

	   channel.disconnect();
	}
//
//    protected static Channel createChannel(Session session, String theCommand) throws JSchException {
//        ChannelExec echannel = (ChannelExec) session.openChannel("exec"); // NOI18N
//        echannel.setCommand(theCommand);
//        echannel.setInputStream(null);
//        echannel.setErrStream(System.err);
//        echannel.connect();
//        return echannel;
//    }

	public static void closeSessions(){
		for (String host : sessions.keySet()) {
			sessions.get(host).disconnect();
		}
	}

		

}
