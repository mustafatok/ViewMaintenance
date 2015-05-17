package de.webdataplatform.message;

import java.net.Socket;
import java.util.Queue;

public abstract class ServerHandler implements Runnable { //oder 'extends Thread'
	
	  protected Socket client;

	  protected Queue<String> updateQueue;
	  
	  
	  public ServerHandler(Queue<String> updateQueue, Socket client) { //Server/Client-Socket
	
	    this.updateQueue = updateQueue;
	    this.client = client;

	  }
	  

}
