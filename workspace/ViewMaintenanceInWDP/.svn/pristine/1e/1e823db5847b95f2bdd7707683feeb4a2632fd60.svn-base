package de.webdataplatform.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.generated.master.zk_jsp;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

public class ViewManagerTestClient implements Watcher, ChildrenCallback {

	/**
	 * @param args
	 */

		private ZooKeeper zk;
		/**
		 * @param args
		 */
		
		
		
		
		public static void main(String[] args) {

			
			ViewManagerTestClient testClient = new ViewManagerTestClient();
			
//			testClient.run();
			
			String znode = "/hbase/rs";
			
			
//		    testClient.getChildren(znode);
//				
//			testClient.nodeExists("/hbase/vm");
//			testClient.createNode("/hbase/vm");
		    testClient.createNode("/hbase/vm/"+args[0]);
		    
		    while(true){
		    	
		    	
		    }
		
		    
		    
		    
		}

		public ViewManagerTestClient() {
			
		    String hostPort = "192.168.26.133:2181";

		    try {
				zk = new ZooKeeper(hostPort, 30000, this);
				
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		}

		public void createNode(String znode) {
			try {
				
				String result = zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				
				System.out.println("result: "+result);
				
				
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void getChildren(String znode) {
			try {
				List<String> children = zk.getChildren(znode, true);
				
				System.out.println(children);
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		public void nodeExists(String znode) {
			try {
				Stat stat = zk.exists(znode, true);
				
				System.out.println(stat);
				
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		
//	    public void run() {
//	    	
//		    String hostPort = "192.168.26.132:2181";
//		    String znode = "/hbase/rs";
//		    ZooKeeper zk=null;
//		    try {
//				zk = new ZooKeeper(hostPort, 30000, this);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		    
//
//			zk.getChildren(znode, true, this, null);
//			
//	        try {
//	            synchronized (this) {
////	                while (!dm.dead) {
//	                    wait();
////	                }
//	            }
//	        } catch (InterruptedException e) {
//	        }
//	    }
//
//	@Override
//	public void processResult(int rc, String path, Object ctx, Stat stat) {
//
//        switch (rc) {
//        case Code.Ok:
//        	System.out.println("Ok");
//            break;
//        case Code.NoNode:
//        	System.out.println("NoNode");
//            break;
//        case Code.SessionExpired:
//        	System.out.println("SessionEx");
//        	break;
//        case Code.NoAuth:
//            System.out.println("NoAuth");
//            break;
////        default:
////            // Retry errors
////            zk.exists(znode, true, this, null);
////            return;
//        }
//		
//	}
	@Override
	public void processResult(int arg0, String arg1, Object arg2, List<String> nodes) {
		
		
		System.out.println("children: "+nodes);
		
	}
	

	@Override
	public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with 
                // server and any watches triggered while the client was 
                // disconnected will be delivered (in order of course)
                break;
            case Expired:
                // It's all over
                break;
            }
        } else {
        	
        	System.out.println(path);
//            if (path != null && path.equals(znode)) {
//                // Something has changed on the node, let's find out
//            }
        }
		
		
		
	}


}
