package de.webdataplatform.viewmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.Message;
import de.webdataplatform.message.ServerHandler;
import de.webdataplatform.settings.SystemConfig;

public class ServerUpdateHandler extends ServerHandler implements Runnable { //oder 'extends Thread'
	
	
//	  private RegionServer regionServer;
	private AtomicLong updatesReceived;
	 
	private Log log; 
	
	  public ServerUpdateHandler(Log log, Queue<String> updateQueue, AtomicLong updatesReceived, Socket client) { //Server/Client-Socket
		  
		 super(updateQueue, client); 
		 this.updatesReceived = updatesReceived;
		 this.log = log;

//		 this.regionServer = regionServer;

	  }
	  

	  
		
		@Override
		public void run() {
		  
	    StringBuffer sb = new StringBuffer();
	    PrintWriter out = null;
	    MessageResolver messageResolver = new MessageResolver();
	    
	    try {
	    	
	    	
	    	
	      out = new PrintWriter( client.getOutputStream(), true );
	      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
	      char[] buffer = new char[SystemConfig.MESSAGES_LENGTH];
	      int anzahlZeichen = bufferedReader.read(buffer, 0, SystemConfig.MESSAGES_LENGTH); // blockiert bis Nachricht empfangen
	      String nachricht = new String(buffer, 0, anzahlZeichen);
	      
	      Message message = new Message(nachricht);
	   
	      
	      
//	      regionServer.setSystemID(new SystemID(message.getContent()));
	      
//	      log.info(this.getClass(),"updates from: "+regionServer+"");
	      
//	      SystemID rSSystemID = new SystemID(message.getContent());
	   
	      log.info(this.getClass(),"starting handshake: "+message+", sending back ok");
	      sb.append("ok");
	      out.println(sb);
//	      String previousMessage="";
	      while(true){
	    	  
		      anzahlZeichen = bufferedReader.read(buffer, 0, SystemConfig.MESSAGES_LENGTH); // blockiert bis Nachricht empfangen
		      nachricht = new String(buffer, 0, anzahlZeichen);
		      log.update(this.getClass(),"update: "+nachricht);
		      
		     
		      
		      List<String> updates = messageResolver.resolve(nachricht);
		      log.update(this.getClass()," resolved updates: "+updates);

		      for (String update : updates) {
		    	  updatesReceived.incrementAndGet();
		    	  updateQueue.add(update);
		    	  
		      }
		      
		      
	    	  
	      }
	      
	    } catch (IOException e) {
	    	  log.error(this.getClass(),e);
	    }
	    
	    
	    finally { 
	      out.println(sb);  //Rückgabe Ergebnis an den Client
	      if ( !client.isClosed() ) {
	        System.out.println("****** Handler:Client close");
	        try {
	          client.close();
	        } catch ( IOException e ) {   log.error(this.getClass(),e);}
	      } 
	    }
	  }  //Ende run


	 

}
