package de.webdataplatform.storage;

import java.io.Serializable;


public class Update<T> implements Serializable{


	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9013050318641185187L;


	public Update() {
		super();
	}


	public Update(String key, T newColumn) {
		super();
		this.key = key;
		this.newColumn = newColumn;
	}

	public Update(String table, String key, T newColumn) {
		super();
		this.table = table;
		this.key = key;
		this.newColumn = newColumn;
	}




	public Update(String table, String key, T newColumn, T oldColumn, long eid) {
		super();
		this.table = table;
		this.key = key;
		this.newColumn = newColumn;
		this.oldColumn = oldColumn;
		this.eid = eid;
	}



	private String table;

	private String key;
	
	private T newColumn;
	
	private T oldColumn;
	
	long eid;

	
	public Update<T> copy(){
		
		Update<T> update = new Update<T>();
		
		update.setTable(table);
		
		update.setKey(key);
		
		update.setNewColumn(newColumn);
		
		update.setOldColumn(oldColumn);
		
		update.setEid(eid);
		
		return update;
		
	}
	
	
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}

	public T getNewColumn() {
		return newColumn;
	}

	public void setNewColumn(T newColumn) {
		this.newColumn = newColumn;
	}

	public T getOldColumn() {
		return oldColumn;
	}

	public void setOldColumn(T oldColumn) {
		this.oldColumn = oldColumn;
	}



	public long getEid() {
		return eid;
	}



	public void setEid(long eid) {
		this.eid = eid;
	}


	public String getTable() {
		return table;
	}



	public void setTable(String table) {
		this.table = table;
	}



	@Override
	public String toString() {
		return "Update [key=" + key + ", newColumn=" + newColumn
				+ ", oldColumn=" + oldColumn + ", eid=" + eid + "]";
	}
	
	
	
	
	
}
