package de.webdataplatform.message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.SystemConfig;

public class MessageClient{
	
//	  String ip = "127.0.0.1";  //localhost
//	  int port = 3144;
//	  Message message;
//
//	  private Log log;
//	 
//	  public MessageClient(Log log, String ip, int port, Message message) {
//
//	    this.ip = ip;
//	    this.port = port;
//	    this.message = message;
//	    this.log = log;
//
//	  }

	  public static synchronized void send(Log log, String ip, int port, Message message) {  //run the service

		  boolean send = false;
		  int x = 0;
		  while(!send && x < 10){
			  try{
			  
			    Socket socket = new Socket(ip,port);  //verbindet sich mit Server
	
			    socket.setSoTimeout(5000);
			    
			    log.message(MessageClient.class,"sending message:"+message+" to "+ip+":"+port);
			    
			    log.message(MessageClient.class,"sending message:"+message.convertToString());
			    
	//		    schreibeNachricht(socket, message.convertToString());
	
			    PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
			    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			    
			    printWriter.println(message.convertToString());
			    printWriter.flush();
			   
			    String response = reader.readLine();
			    
			    log.message(MessageClient.class, "receiving handshake from "+ip+":"+port+", "+response);
			    
			    printWriter.close();
			    reader.close();
			    
	//		    String empfangeneNachricht = leseNachricht(socket);
	//
			    socket.close();
			    
			    if(response.equals("ok"))send=true;
			    else{
			    	
			    	throw new Exception("message could not be send: "+message+" answer was:"+response);
//			    	 Log.info(this.getClass(), "send failed, retrying after timeout");
//			    	try {
//			    		Thread.sleep(1000);
//			    	} catch (InterruptedException e1) {
//			    		e1.printStackTrace();
//			    	}
//			    	x++;
			    }
			    
			    
			    
			    
			  }catch (Exception e){
				  x++;
				  log.error(MessageClient.class,e);
				  log.info(MessageClient.class, "send failed of message:"+message+" failed, retrying after timeout");
				  
				  try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				  
			  }
		  }
	  }
	 

//	  void schreibeNachricht(Socket socket, String nachricht) throws IOException {
//	    PrintWriter printWriter = new PrintWriter(socket.getOutputStream());
//	    printWriter.println(nachricht);
//	    printWriter.flush();
//	    printWriter.close();
//	  }
//	  String leseNachricht(Socket socket) throws IOException {
//		    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//		    char[] buffer = new char[Constants.MESSAGE_LENGTH];
//		    //blockiert bis Nachricht empfangen
//		    int anzahlZeichen = bufferedReader.read(buffer, 0, Constants.MESSAGE_LENGTH);
//		    String nachricht = new String(buffer, 0, anzahlZeichen);
//		    bufferedReader.close();
//		    return nachricht;
//	 }

	}