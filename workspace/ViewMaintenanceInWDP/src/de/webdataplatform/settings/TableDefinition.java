package de.webdataplatform.settings;

import java.util.List;



public class TableDefinition {


	private String name; 
	private KeyDefinition primaryKey;
	private List<ColumnDefinition> columns;
	
	
	public TableDefinition(String name, KeyDefinition primaryKey,
			List<ColumnDefinition> columns) {
		super();
		this.name = name;
		this.primaryKey = primaryKey;
		this.columns = columns;
	}
	
	
	
	public KeyDefinition getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(KeyDefinition primaryKey) {
		this.primaryKey = primaryKey;
	}
	public List<ColumnDefinition> getColumns() {
		return columns;
	}
	public void setColumns(List<ColumnDefinition> columns) {
		this.columns = columns;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}



	@Override
	public String toString() {
		return "TableDefinition [name=" + name + ", primaryKey=" + primaryKey
				+ ", columns=" + columns + "]";
	}


	
	
	

}
