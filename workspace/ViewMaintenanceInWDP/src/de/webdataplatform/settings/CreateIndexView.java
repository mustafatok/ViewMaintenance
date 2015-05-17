package de.webdataplatform.settings;

import de.webdataplatform.view.ViewMode;


public class CreateIndexView implements ICreateTable, ICreateView{

	
	private String name;
	
	private String basetable;
	
	private String indexKey;

	
	private int numOfRegions;
	
	private String controlTable;
	
	

	public int getNumOfRegions() {
		return numOfRegions;
	}




	public void setNumOfRegions(int numOfRegions) {
		this.numOfRegions = numOfRegions;
	}




	public String getControlTables() {
		return controlTable;
	}




	public void setControlTable(String controlTable) {
		this.controlTable = controlTable;
	}




	public CreateIndexView() {
		super();
	}


	
	
	public CreateIndexView(String name, String basetable, String indexKey,
			int numOfRegions, String controlTable) {
		super();
		this.name = name;
		this.basetable = basetable;
		this.indexKey = indexKey;
		this.numOfRegions = numOfRegions;
		this.controlTable = controlTable;
	}






	public String getViewDefinition(){
		
		String viewDefinition="";
		
		viewDefinition = ViewMode.INDEX.toString()+","+getBasetable()+","+getIndexKey();
		
		return viewDefinition;
		
	}
	
	public static CreateIndexView parse(String viewDefintion){
		
		CreateIndexView cIV = new CreateIndexView();
		
		cIV.setBasetable(viewDefintion.split(",")[1]);
		
		cIV.setIndexKey(viewDefintion.split(",")[2]);
		
		
		return cIV;
		
	}
	
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getBasetable() {
		return basetable;
	}

	public void setBasetable(String basetable) {
		this.basetable = basetable;
	}




	public String getIndexKey() {
		return indexKey;
	}




	public void setIndexKey(String indexKey) {
		this.indexKey = indexKey;
	}



	@Override
	public String toString() {
		return "CreateIndexView [name=" + name + ", basetable=" + basetable
				+ ", indexKey=" + indexKey + "]";
	}






	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public CreateBaseTable copy() {
		// TODO Auto-generated method stub
		return null;
	}




	
	
	
	
}
