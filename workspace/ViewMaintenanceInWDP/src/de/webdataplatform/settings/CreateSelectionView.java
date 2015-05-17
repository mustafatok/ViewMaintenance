package de.webdataplatform.settings;

import de.webdataplatform.view.ViewMode;


public class CreateSelectionView implements ICreateTable, ICreateView{

	
	private String name;
	
	private String basetable;
	
	private String selectionColumn;
	
	private String selectionOperation;
	
	private String selectionValue;
	
	private int numOfRegions;
	
	private String controlTable;


	public CreateSelectionView() {
		super();
	}

	public CreateSelectionView(String name, String basetable,
			String selectionColumn, String selectionOperation,
			String selectionValue, int numOfRegions, String controlTable) {
		super();
		this.name = name;
		this.basetable = basetable;
		this.selectionColumn = selectionColumn;
		this.selectionOperation = selectionOperation;
		this.selectionValue = selectionValue;
		this.numOfRegions = numOfRegions;
		this.controlTable = controlTable;
	}
	
	public String getViewDefinition(){
		
		String viewDefinition="";
		
		viewDefinition = ViewMode.SELECTION.toString()+","+getSelectionColumn()+","+getSelectionOperation()+","+getSelectionValue();
		
		return viewDefinition;
		
	}
	
	public static CreateSelectionView parse(String viewDefintion){
		
		CreateSelectionView cSV = new CreateSelectionView();
		
		cSV.setSelectionColumn(viewDefintion.split(",")[1]);
		
		cSV.setSelectionOperation(viewDefintion.split(",")[2]);
		
		cSV.setSelectionValue(viewDefintion.split(",")[3]);
		
		return cSV;
		
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

	public String getSelectionColumn() {
		return selectionColumn;
	}

	public void setSelectionColumn(String selectionColumn) {
		this.selectionColumn = selectionColumn;
	}

	public String getSelectionOperation() {
		return selectionOperation;
	}

	public void setSelectionOperation(String selectionOperation) {
		this.selectionOperation = selectionOperation;
	}

	public String getSelectionValue() {
		return selectionValue;
	}

	public void setSelectionValue(String selectionValue) {
		this.selectionValue = selectionValue;
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
		return "CreateSelectionView [name=" + name + ", basetable=" + basetable
				+ ", selectionColumn=" + selectionColumn
				+ ", selectionOperation=" + selectionOperation
				+ ", selectionValue=" + selectionValue + ", numOfRegions="
				+ numOfRegions + ", controlTable=" + controlTable + "]";
	}


	@Override
	public String getControlTables() {
		return controlTable;
	}


	@Override
	public String getType() {
		return "selection";
	}

	@Override
	public CreateBaseTable copy() {
		// TODO Auto-generated method stub
		return null;
	}


	
	
	
	
}
