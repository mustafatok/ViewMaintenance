package de.webdataplatform.viewmanager;

import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.ServerHandler;
import de.webdataplatform.message.ServerHandlerFactory;

public class ServerUpdateHandlerFactory implements ServerHandlerFactory {


	private Log log;
	
	private Queue<String> incomingUpdates;
	
	private AtomicLong updatesReceived;
	
	

	public ServerUpdateHandlerFactory(Log log, Queue<String> incomingUpdates, AtomicLong updatesReceived) {
		super();
		this.log = log;
		this.incomingUpdates = incomingUpdates;
		this.updatesReceived = updatesReceived;
	}





	@Override
	public ServerHandler getServerHandler(Socket cs) {
		
		return new ServerUpdateHandler(log, incomingUpdates, updatesReceived, cs);
	}

}
