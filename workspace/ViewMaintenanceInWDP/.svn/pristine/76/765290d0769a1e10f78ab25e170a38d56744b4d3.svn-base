package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import de.webdataplatform.view.ViewMode;


public class CopyOfCreateJoinView implements ICreateTable, ICreateView{


	
	private String name;
	
	private List<JoinTablePair> joinPairs;
	
	private int numOfRegions;
	
	private String controlTable;

	
	
	public CopyOfCreateJoinView(String name, List<JoinTablePair> joinPairs, int numOfRegions, String controlTable) {
		super();
		this.name = name;
		this.joinPairs = joinPairs;
		this.numOfRegions = numOfRegions;
		this.controlTable = controlTable;
	}
	public CopyOfCreateJoinView() {

	}

	
	public String getViewDefinition(){
		
		String viewDefinition="";
		

		viewDefinition += ViewMode.JOIN.toString()+","+getName()+","+getNumOfRegions()+","+isControlTable()+",";
		
		for(JoinTablePair joinTablePair : joinPairs){
			
			viewDefinition += joinTablePair.getLeftTable().getTableName()+":"+joinTablePair.getLeftTable().getJoinKey()+"="+joinTablePair.getRightTable().getTableName()+":"+joinTablePair.getRightTable().getJoinKey()+",";
		
		}
		return viewDefinition;
		
	}
	
	public static CopyOfCreateJoinView parse(String viewDefintion){
		
		CopyOfCreateJoinView cJV = new CopyOfCreateJoinView();
		
		
		cJV.setName(viewDefintion.split(",")[1]);
		
		cJV.setNumOfRegions(Integer.parseInt(viewDefintion.split(",")[2]));
		
		cJV.setControlTable(viewDefintion.split(",")[3]);
		
		List<JoinTablePair> joinTablePairs = new ArrayList<JoinTablePair>();
		
		String[] listTokens = viewDefintion.split(",");
		
		for (int i = 4; i < listTokens.length; i++) {
			
			String[] pairTokens = listTokens[i].split("=");
			
			
			for (int j = 0; j < pairTokens.length; j+=2) {
				
				
				JoinTable joinTableLeft = new JoinTable(pairTokens[j].split(":")[0], pairTokens[j].split(":")[1]);
				
				JoinTable joinTableRight = new JoinTable(pairTokens[j+1].split(":")[0], pairTokens[j+1].split(":")[1]);
				
				JoinTablePair joinTablePair = new JoinTablePair(joinTableLeft, joinTableRight);

				joinTablePairs.add(joinTablePair);
			}
			
		}
		cJV.setJoinPairs(joinTablePairs);
		
		
		return cJV;
		
	}
	
	

	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<JoinTablePair> getJoinPairs() {
		return joinPairs;
	}

	public void setJoinPairs(List<JoinTablePair> joinPairs) {
		this.joinPairs = joinPairs;
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
		return "CreateJoinView [name=" + name + ", joinPairs=" + joinPairs
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
		if(joinPairs.size()> 0){
			return joinPairs.get(0).getLeftTable().getTableName();
		}
		return null;
	}




	@Override
	public CreateBaseTable copy() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	
	
	
	
}
