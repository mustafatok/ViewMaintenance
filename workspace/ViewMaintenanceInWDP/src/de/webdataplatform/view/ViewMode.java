package de.webdataplatform.view;

public enum ViewMode {

	AGGREGATION_SUM("AGGREGATION_SUM"), 
	AGGREGATION_COUNT("AGGREGATION_COUNT"), 
	AGGREGATION_MAX("AGGREGATION_MAX"), 
	AGGREGATION_MIN("AGGREGATION_MIN"), 
	AGGREGATION_AVERAGE("AGGREGATION_AVERAGE"), 
	INDEX("INDEX"), 
	SELECTION("SELECTION"),
	REVERSE_JOIN("REVERSE_JOIN"),
	JOIN("JOIN"),
	DELTA("DELTA");
	
	private String name;
	
	private ViewMode(String name){
		this.name = name;
	}
	
	public String getName(){
		return name;
	}
	
}
