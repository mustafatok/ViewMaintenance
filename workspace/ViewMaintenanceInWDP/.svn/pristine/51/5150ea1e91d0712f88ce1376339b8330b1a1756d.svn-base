package de.webdataplatform.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.NetworkConfig;

public class ZookeeperService implements Watcher, IZooKeeperService {

	/**
	 * @param args
	 */

		private ZooKeeper zk;
		
		/**
		 * @param args
		 */
		
		private ChildrenCallback childrenCallback;
		
		private List<String> triggers;
		
		private Log log;
		
		public ZookeeperService(Log log) {
			
			this.log = log;
			this.triggers = new ArrayList<String>();
	
		    
		    
		}

		public ZookeeperService(Log log, List<String> triggers, ChildrenCallback childrenCallback) {
			
//		    String hostPort = "192.168.26.133:2181";
			this.log = log;
			this.childrenCallback = childrenCallback;
			this.triggers = triggers;
			

			
			
		}
		
		public void startup() throws IOException{
			
			if(NetworkConfig.ZOOKEEPER_CLIENTPORT != 0){
				
				String connectionString="";
				int i = 1;
				for (String address : NetworkConfig.ZOOKEEPER_QUORUM.split(",")) {
					
					connectionString += address +":"+NetworkConfig.ZOOKEEPER_CLIENTPORT;
					if(i != NetworkConfig.ZOOKEEPER_QUORUM.split(",").length)connectionString += ",";
					
				}
				
				zk = new ZooKeeper(connectionString, 30000, this);
			}
			else zk = new ZooKeeper(NetworkConfig.ZOOKEEPER_QUORUM, 30000, this);
		}
		
		
		
		public void setTriggerOnChildren(String znode){
			
			if(!triggers.contains(znode)){
				
				triggers.add(znode);
	
			}	
			zk.getChildren(znode, true, childrenCallback, null);
		}
		

		public boolean createPersistentNode(String znode) {

				String result;
				try {
					result = zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					if(result != null)return true;
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					log.error(this.getClass(), e);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					log.error(this.getClass(), e);
				}
				
				
				return false;
			
		}
		public void deleteNode(String znode) {


		
			List<String> children;
			try {
				children = zk.getChildren(znode, false);
			for (String child : children) {
				zk.delete(znode+"/"+child,-1);
			}
			zk.delete(znode, -1);
			
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			}
			
			
		
	}
		
		public boolean createSessionNode(String znode){

				
				String result;
				try {
					result = zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					if(result != null)return true;
				} catch (KeeperException e) {
				
					log.error(this.getClass(), e);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					log.error(this.getClass(), e);
				}
				
				
			
			return false;
			
		}
		
		public List<String> getChildren(String znode) {
			try {
				List<String> children = zk.getChildren(znode, false);
				
				return children;
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			}
			return null;
		}
	
		public boolean nodeExists(String znode) {
			try {
				Stat stat = zk.exists(znode, false);

				if(stat != null)return true;
//				System.out.println("Stat: "+stat);
				
			
				
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log.error(this.getClass(), e);
			}
			
			return false;
		}		
		

	@Override
	public void process(WatchedEvent event) {
        
		
		String path = event.getPath();
        

            	
    	 log.info(this.getClass(), "zookeeper event: "+event);
    	 log.info(this.getClass(), "triggers: "+triggers);
    	 for(String trigger : triggers)
    	 zk.getChildren(trigger, true, childrenCallback, null);
                // Something has changed on the node, let's find out
//            }
//        }
		
		
		
	}



	
//public void run() {
//
//String hostPort = "192.168.26.132:2181";
//String znode = "/hbase/rs";
//ZooKeeper zk=null;
//try {
//	zk = new ZooKeeper(hostPort, 30000, this);
//} catch (IOException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//}
//
//
//zk.getChildren(znode, true, this, null);
//
//try {
//    synchronized (this) {
////        while (!dm.dead) {
//            wait();
////        }
//    }
//} catch (InterruptedException e) {
//}
//}
//
//@Override
//public void processResult(int rc, String path, Object ctx, Stat stat) {
//
//switch (rc) {
//case Code.Ok:
//System.out.println("Ok");
//break;
//case Code.NoNode:
//System.out.println("NoNode");
//break;
//case Code.SessionExpired:
//System.out.println("SessionEx");
//break;
//case Code.NoAuth:
//System.out.println("NoAuth");
//break;
////default:
////// Retry errors
////zk.exists(znode, true, this, null);
////return;
//}
//
//}	



}
