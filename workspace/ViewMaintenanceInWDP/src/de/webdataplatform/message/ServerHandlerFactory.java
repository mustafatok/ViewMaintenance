package de.webdataplatform.message;

import java.net.Socket;

public interface ServerHandlerFactory {
	
	public ServerHandler getServerHandler(Socket cs);
	

}
