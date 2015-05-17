package de.webdataplatform.viewmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.Queue;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.ServerHandler;

public class ServerMessageHandler extends ServerHandler implements Runnable { //oder 'extends Thread'
	
	
	  private Log log;
	  
	  public ServerMessageHandler(Log log, Queue<String> updateQueue, Socket client) { //Server/Client-Socket
		  
		 super(updateQueue, client); 
		 this.log = log;


	  }
	  

	  
		
		@Override
		public void run() {
		  
//	    StringBuffer sb = new StringBuffer();
	    PrintWriter writer = null;
	    MessageResolver messageResolver = new MessageResolver();
	    
	    try {
	    	
	    
	    	
	      writer = new PrintWriter( client.getOutputStream());
	      BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
//	      char[] buffer = new char[Constants.MESSAGE_LENGTH];
//	      int anzahlZeichen = bufferedReader.read(buffer, 0, Constants.MESSAGE_LENGTH); // blockiert bis Nachricht empfangen
//	      String nachricht = new String(buffer, 0, anzahlZeichen);
	      
	      String nachricht = reader.readLine();
	      log.message(this.getClass()," message: "+nachricht);
	      
	      List<String> messages = messageResolver.resolve(nachricht);
	      log.message(this.getClass()," receiving message: "+messages);
	      
	      for (String message : messages) {
	    	  updateQueue.add(message);
	      }
	      
	   
	
	      writer.println("ok");
	      writer.flush();
	      writer.close();
	      client.close();	  

//		      log.info(this.getClass(),"update: "+nachricht);
		      
		     
		      

	      
	      
	    } catch (IOException e) {
	    	  log.error(this.getClass(),e);
	    }
	    
	    
	    finally { 
	      if ( !client.isClosed() ) {
	    	  log.message(this.getClass(), "****** Handler:Client close");
	        try {
	        	writer.close();
	        	client.close();
	        } catch ( IOException e ) {  log.error(this.getClass(),e); }
	      } 
	    }
	  }  //Ende run


	 

}
