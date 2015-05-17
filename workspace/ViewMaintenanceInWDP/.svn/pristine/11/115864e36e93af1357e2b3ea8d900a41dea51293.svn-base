package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.List;

import de.webdataplatform.view.ViewMode;


public class CreateDeltaView implements ICreateTable, ICreateView{

	
	private String name;
	
	private String type;
	
	private String basetable;
	
	private List<String> columns;
	
	private int numOfRegions;
	
	private String controlTable;

	

	
	public CreateDeltaView() {
		super();
	}



	public CreateDeltaView(String name, String type, String basetable, List<String> columns, int numOfRegions, String controlTable) {
		super();
		this.name = name;
		this.type = type;
		this.basetable = basetable;
		this.columns = columns;
		this.numOfRegions = numOfRegions;
		this.controlTable = controlTable;
	}
	
	
	
	public String getViewDefinition(){
		
		String viewDefinition="";
		
		
		String columnsString="";
		
		for (int i = 0; i < columns.size(); i++) {
			if(i < (columns.size()-1))columnsString += columns.get(i)+":";
			else columnsString += columns.get(i);
			
		}
		
		
		viewDefinition += ViewMode.DELTA.toString()+","+getName()+","+columnsString+","+getNumOfRegions()+","+isControlTable();
		
		return viewDefinition;
		
	}
	
	public static CreateDeltaView parse(String viewDefintion){
		
		CreateDeltaView cAV = new CreateDeltaView();
		
		cAV.setType(viewDefintion.split(",")[0]);
		
		cAV.setName(viewDefintion.split(",")[1]);
		
		List<String> columns = new ArrayList<String>();
		
		for (String string : viewDefintion.split(",")[2].split(":")) {
			
			columns.add(string);
		}
		
		cAV.setColumns(columns);
		
		if(viewDefintion.split(",")[3] != null && !viewDefintion.split(",")[3].equals(""))cAV.setNumOfRegions(Integer.parseInt(viewDefintion.split(",")[3]));
		
		if(viewDefintion.split(",")[4] != null && !viewDefintion.split(",")[4].equals(""))cAV.setControlTable(viewDefintion.split(",")[4]);
		
		
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

	public List<String> getColumns() {
		return columns;
	}



	public void setColumns(List<String> columns) {
		this.columns = columns;
	}





	@Override
	public String toString() {
		return "CreateDeltaView [name=" + name + ", type=" + type
				+ ", basetable=" + basetable + ", columns=" + columns
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
