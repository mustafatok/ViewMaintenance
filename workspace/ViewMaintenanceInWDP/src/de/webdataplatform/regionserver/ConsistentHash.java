package de.webdataplatform.regionserver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash<T> {

 private final HashFunction hashFunction;
 private final int numberOfReplicas;
 private final SortedMap<Integer, T> circle = new TreeMap<Integer, T>();
// private final Map<T, Integer> replicas = new HashMap<T, Integer>();

 public ConsistentHash(HashFunction hashFunction, int numberOfReplicas, Collection<T> nodes) {
   this.hashFunction = hashFunction;
   this.numberOfReplicas = numberOfReplicas;

   for (T node : nodes) {
     add(node);
   }
 }

 public void add(T node) {
   for (int i = 0; i < numberOfReplicas; i++) {
	   
	 synchronized(this){  
		 circle.put(hashFunction.hash(node.toString() + i), node);
	 }
   }
 }

 public void remove(T node) {
   for (int i = 0; i < numberOfReplicas; i++) {
	   synchronized(this){
		   circle.remove(hashFunction.hash(node.toString() + i));
	   }
   }
 }

 public T get(Object key) {
	 synchronized(this){
	   if (circle.isEmpty()) {
	     return null;
	   }
	   int hash = hashFunction.hash(key.toString());
	   if (!circle.containsKey(hash)) {
	     SortedMap<Integer, T> tailMap = circle.tailMap(hash);
	     hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
	   }
	   return circle.get(hash);
	 }  
 }
 
 public int numOfElements(){
	 synchronized(this){
	   return circle.keySet().size()/numberOfReplicas;
	 }  
 }

public SortedMap<Integer, T> getCircle() {
	
	return circle;
}

public Set<T> getNodes(){
	
	 Set<T> hashsetList = new HashSet<T>(circle.values());
	 
	 return hashsetList;
}
 
 
 

}