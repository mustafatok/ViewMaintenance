package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.List;

import de.webdataplatform.view.ViewMode;


public class CreateReverseJoinView implements ICreateTable, ICreateView{


	
	private String name;
	
	private List<JoinTable> joinTables;
	
	private int numOfRegions;
	
	private String controlTable;

	
	
	public CreateReverseJoinView(String name,  List<JoinTable> joinTables, int numOfRegions, String controlTable) {
		super();
		this.name = name;
		this.joinTables = joinTables;
		this.numOfRegions = numOfRegions;
		this.controlTable = controlTable;
	}
	public CreateReverseJoinView() {

	}

	
	public String getViewDefinition(){
		
		String viewDefinition="";
		

		viewDefinition += ViewMode.REVERSE_JOIN.toString()+","+getName()+","+getNumOfRegions()+","+isControlTable()+",";
		
		for (JoinTable joinTable : joinTables) {
			
			viewDefinition += joinTable.getTableName()+":"+joinTable.getJoinKey()+",";
		}
		

		return viewDefinition;
		
	}
	
	public static CreateReverseJoinView parse(String viewDefintion){
		
		CreateReverseJoinView cJV = new CreateReverseJoinView();
		
		
		cJV.setName(viewDefintion.split(",")[1]);
		
		cJV.setNumOfRegions(Integer.parseInt(viewDefintion.split(",")[2]));
		
		cJV.setControlTable(viewDefintion.split(",")[3]);
		
		
		String[] listTokens = viewDefintion.split(",");
		
		List<JoinTable> joinTables = new ArrayList<JoinTable>();
		
		for (int i = 4; i < listTokens.length; i++) {
			
			JoinTable joinTable = new JoinTable(listTokens[i].split(":")[0], listTokens[i].split(":")[1]);

			joinTables.add(joinTable);
			
		}
	
		cJV.setJoinTables(joinTables);
		
		
		return cJV;
		
	}
	
	

	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<JoinTable> getJoinTables() {
		return joinTables;
	}

	public void setJoinTables(List<JoinTable> joinTables) {
		this.joinTables = joinTables;
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
		return "CreateReverseJoinView [name=" + name + ", joinTables=" + joinTables
				+ ", numOfRegions=" + numOfRegions + ", controlTable="
				+ controlTable + ", useIndex=" + "]";
	}


	@Override
	public String getControlTables() {
		return controlTable;
	}


	@Override
	public String getType() {
		return "join";
	}


	@Override
	public String getBasetable() {

			return joinTables.get(0).getTableName();
		
	}




	@Override
	public CreateBaseTable copy() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	
	
	
	
}
