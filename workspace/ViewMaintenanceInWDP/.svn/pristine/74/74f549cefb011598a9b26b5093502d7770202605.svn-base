package de.webdataplatform.settings;

import de.webdataplatform.view.ViewMode;


public class CreateAggregationView implements ICreateTable, ICreateView{

	
	private String name;
	
	private String type;
	
	private String basetable;
	
	private String aggregationKey;
	
	private String aggregationValue;
	
	private int numOfRegions;
	
	private String controlTable;

//	private boolean useIndex;
	

	
	public CreateAggregationView() {
		super();
	}



	public CreateAggregationView(String name, String type, String basetable, String aggregationKey, String aggregationValue, int numOfRegions, String controlTable) {
		super();
		this.name = name;
		this.type = type;
		this.basetable = basetable;
		this.aggregationKey = aggregationKey;
		this.aggregationValue = aggregationValue;
		this.numOfRegions = numOfRegions;
		this.controlTable = controlTable;
//		this.useIndex = useIndex;
	}
	
	
	
	public String getViewDefinition(){
		
		String viewDefinition="";
		
		if(getType().equals("count")){
			viewDefinition += ViewMode.AGGREGATION_COUNT.toString();
		}
		if(getType().equals("sum")){
			viewDefinition += ViewMode.AGGREGATION_SUM.toString();
		}
		if(getType().equals("min")){
			viewDefinition += ViewMode.AGGREGATION_MIN.toString();
		}
		if(getType().equals("max")){
			viewDefinition += ViewMode.AGGREGATION_MAX.toString();
		}
		viewDefinition += ","+getName()+","+getAggregationKey()+","+getAggregationValue()+","+getNumOfRegions()+","+isControlTable();
		
		return viewDefinition;
		
	}
	
	public static CreateAggregationView parse(String viewDefintion){
		
		CreateAggregationView cAV = new CreateAggregationView();
		
		cAV.setType(viewDefintion.split(",")[0]);
		
		cAV.setName(viewDefintion.split(",")[1]);
		
		cAV.setAggregationKey(viewDefintion.split(",")[2]);
		
		cAV.setAggregationValue(viewDefintion.split(",")[3]);
		
		if(viewDefintion.split(",")[4] != null && !viewDefintion.split(",")[4].equals(""))cAV.setNumOfRegions(Integer.parseInt(viewDefintion.split(",")[4]));
		
		if(viewDefintion.split(",")[5] != null && !viewDefintion.split(",")[5].equals(""))cAV.setControlTable(viewDefintion.split(",")[5]);
		
		
		return cAV;
		
	}
	
	
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getBasetable() {
		return basetable;
	}

	public void setBasetable(String basetable) {
		this.basetable = basetable;
	}

	public String getAggregationKey() {
		return aggregationKey;
	}

	public void setAggregationKey(String aggregationKey) {
		this.aggregationKey = aggregationKey;
	}

	public String getAggregationValue() {
		return aggregationValue;
	}

	public void setAggregationValue(String aggregationValue) {
		this.aggregationValue = aggregationValue;
	}

	public int getNumOfRegions() {
		return numOfRegions;
	}

	public void setNumOfRegions(int numOfRegions) {
		this.numOfRegions = numOfRegions;
	}

	public String isControlTable() {
		return controlTable;
	}

	public void setControlTable(String controlTable) {
		this.controlTable = controlTable;
	}



	
	

	@Override
	public String toString() {
		return "CreateAggregationView [name=" + name + ", type=" + type
				+ ", basetable=" + basetable + ", aggregationKey="
				+ aggregationKey + ", aggregationValue=" + aggregationValue
				+ ", numOfRegions=" + numOfRegions + ", controlTable="
				+ controlTable + "]";
	}



	@Override
	public String getControlTables() {
		return controlTable;
	}



	@Override
	public CreateBaseTable copy() {
		// TODO Auto-generated method stub
		return null;
	}



	



}
