package de.webdataplatform.message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.storage.BaseTableUpdate;
import de.webdataplatform.system.Command;
import de.webdataplatform.system.Component;
import de.webdataplatform.viewmanager.ViewManager;

public class UpdateClient extends Thread {
	
//	  String ip = "127.0.0.1";  //localhost
//	  int port = 3144;
	
	  private SystemID rsNetworkAddress;	
	  private ViewManager viewManager;
	  private Queue<String> updateQueue;
	  private AtomicLong updatesSent;
	  private volatile boolean running =false;
	  private Log log; 
	  
	  
	 
	  public boolean isRunning() {
		return running;
	  }

	public void terminate() {
		this.running = false;
	}

	public UpdateClient(Log log, SystemID rsNetworkAddress, ViewManager viewManager, Queue<String> updateQueue, AtomicLong updatesSent) {

//	    this.ip = viewManager.getIp();
//	    this.port = viewManager.getPort();
		this.rsNetworkAddress = rsNetworkAddress;
		this.viewManager = viewManager;
	    this.updateQueue = updateQueue;
	    this.updatesSent = updatesSent;
	    this.log = log;

	  }

	  public void run() {  //run the service

		  try{
		  
			running = true;  
			  
		    Socket socket = new Socket(viewManager.getSystemID().getIp(),viewManager.getSystemID().getUpdatePort());  //verbindet sich mit Server

		    log.info(this.getClass(),"starting handshake for "+viewManager.getVMName()+":"+viewManager.getSystemID().getUpdatePort());
		    
			Message message = new Message(Component.regionServer, rsNetworkAddress.getName(), Command.START_UPDATE_PROCESSING, rsNetworkAddress.toString());
		    
		    sendMessage(socket, message.convertToString());
		  
		    String empfangeneNachricht = leseNachricht(socket);
		    
		    log.info(this.getClass(), "receiving handshake from "+viewManager.getVMName()+":  "+empfangeneNachricht);
//		    if(empfangeneNachricht.equals("ok"));
		    
		    String update=null;
		    
		    while(running){
		    	
		
		    	
		    	if(update == null)update = updateQueue.poll();
		    	
		    	if(update != null){
		    		
		    		log.update(this.getClass(), "Polled new update: sending update:"+update+" to "+viewManager.getVMName()+" update queue size: "+updateQueue.size());
		    		try {
						sendMessage(socket, update);
						updatesSent.incrementAndGet();
						update = null;
						log.update(this.getClass(),"socket state: "+socket.isClosed());
					} catch (Exception e) {
						
						log.error(this.getClass(),e);
					}
		    	
		    		
		    	}
		    	
				if(updateQueue.size() == 0){
					try {
						Thread.sleep(SystemConfig.VIEWMANAGER_UPDATEPOLLINGINTERVAL);
					} catch (InterruptedException e) {
		
						e.printStackTrace();
					}
				}
		    	
		    	
		    }
		  }catch (Exception e){
			  log.error(this.getClass(),e);
			  running =false;
		  }
	  }
	 

	  void sendMessage(Socket socket, String nachricht) throws IOException {
	    PrintWriter printWriter =
	      new PrintWriter(
	        new OutputStreamWriter(
	          socket.getOutputStream()));
	    printWriter.print(nachricht);
	    printWriter.flush();
	  }
	  String leseNachricht(Socket socket) throws IOException {
		    BufferedReader bufferedReader =
		      new BufferedReader(
		        new InputStreamReader(
		          socket.getInputStream()));
		    char[] buffer = new char[SystemConfig.MESSAGES_LENGTH];
		    //blockiert bis Nachricht empfangen
		    int anzahlZeichen = bufferedReader.read(buffer, 0, SystemConfig.MESSAGES_LENGTH);
		    String nachricht = new String(buffer, 0, anzahlZeichen);
		    return nachricht;
	 }

	}