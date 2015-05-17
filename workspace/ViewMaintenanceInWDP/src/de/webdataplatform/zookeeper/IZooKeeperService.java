package de.webdataplatform.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;


public interface IZooKeeperService {
	

	public void startup() throws IOException;
	
	public boolean createPersistentNode(String znode);
	
	public boolean createSessionNode(String znode) ;
	
	public boolean nodeExists(String znode);
	
	public List<String> getChildren(String znode);
	
	public void setTriggerOnChildren(String znode);
	
	public void deleteNode(String znode);
	
//	public void registerNode();
//	
//	public void deregisterNode();
//	
//	public void setTrigger();

}
