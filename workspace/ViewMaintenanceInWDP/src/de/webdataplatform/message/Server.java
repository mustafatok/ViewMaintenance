package de.webdataplatform.message;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.webdataplatform.log.Log;




public class Server{ 

		private ServerHandlerFactory serverHandlerFactory;
		private int port;
//		private Log log;
	
		public Server(ServerHandlerFactory serverHandlerFactory, int port){
			this.serverHandlerFactory = serverHandlerFactory;
			this.port = port;
//			this.log = log;
			
		}
	
	  	public void start() throws IOException {
			  
		    final ExecutorService pool;
		    final ServerSocket serverSocket;
		    

		    pool = Executors.newCachedThreadPool();

		    serverSocket = new ServerSocket(port);

		    Thread t1 = new Thread(new NetworkService(pool,serverSocket, serverHandlerFactory));
		    System.out.println("Start UpdateServer on Port:"+ port+", Thread: "+Thread.currentThread());

		    t1.start();


		  }
		}
		 
		//Thread bzw. Runnable zur Entgegennahme der Client-Anforderungen
		class NetworkService implements Runnable { //oder extends Thread
			
		  private final ServerSocket serverSocket;
		  private final ExecutorService pool;
		  private final ServerHandlerFactory serverHandlerFactory;
//		  private Log log;
		  
		  public NetworkService(ExecutorService pool, ServerSocket serverSocket, ServerHandlerFactory serverHandlerFactory) {
		    this.serverSocket = serverSocket;
		    this.pool = pool;
		    this.serverHandlerFactory = serverHandlerFactory;
//		    this.log = log;

		    
		  }
		  public void run() { // run the service
		    try {

		      while ( true ) {

		    	synchronized(this){  
		    	  
			        Socket cs = serverSocket.accept();  //warten auf Client-Anforderung
			 
			        ServerHandler serverHandler = serverHandlerFactory.getServerHandler(cs);
			        		
//			        		serverHandler.setClient(cs);
	
			        Thread t = new Thread(serverHandler);
			        
	//		        Log.info(this.getClass(),"accepting new region server connection ");
	
			        t.start();
			        
	//		        pool.execute(serverHandler);
			        
		    	}
		      }
		    } catch (IOException ex) {
		      System.out.println("--- Interrupt NetworkService-run");
		    }
		    finally {
		      System.out.println("--- Ende NetworkService(pool.shutdown)");
		      pool.shutdown();  //keine Annahme von neuen Anforderungen
		      try {

		        pool.awaitTermination(4L, TimeUnit.SECONDS);
		        if ( !serverSocket.isClosed() ) {
		          System.out.println("--- Ende NetworkService:ServerSocket close");
		          serverSocket.close();
		        }
		      } catch ( IOException e ) {
//		    	  log.error(this.getClass(), e); 
		    	  }
		      catch ( InterruptedException ei ) { 
//		    	  log.error(this.getClass(), ei);
		    	  }
		    }
		  }
		}


