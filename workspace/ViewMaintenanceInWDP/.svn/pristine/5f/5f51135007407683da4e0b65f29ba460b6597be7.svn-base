package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import de.webdataplatform.view.ViewMode;

public class JoinTablePair {

	
	
	private JoinTable leftTable;
	
	private JoinTable rightTable;
	
	

	
	
	public JoinTablePair(JoinTable leftTable, JoinTable rightTable) {
		super();
		this.leftTable = leftTable;
		this.rightTable = rightTable;
	}
	

	
	public static List<String> getJoinTableNames(List<JoinTable> joinTables){
		
		
		List<String> joinTableNames = new ArrayList<String>();
		
		for (JoinTable joinTable : joinTables) {
			
			if(!joinTableNames.contains(joinTable.getTableName()))joinTableNames.add(joinTable.getTableName());

			
		}
		
		
		return joinTableNames;
		
	}	
	
	public static List<JoinTablePair> getJoinTables(List<JoinTablePair> joinTablePairs, String baseTableName){
		
		
		List<JoinTablePair> joinTables = new ArrayList<JoinTablePair>();
		
		for (JoinTablePair joinTablePair : joinTablePairs) {
			
			if(joinTablePair.getLeftTable().getTableName().equals(baseTableName))joinTables.add(joinTablePair);
			if(joinTablePair.getRightTable().getTableName().equals(baseTableName))joinTables.add(joinTablePair);
			
		}
		
		
		return joinTables;
		
	}	
	
	

	
	
	

	public JoinTable getLeftTable() {
		return leftTable;
	}

	public void setLeftTable(JoinTable leftTable) {
		this.leftTable = leftTable;
	}

	public JoinTable getRightTable() {
		return rightTable;
	}

	public void setRightTable(JoinTable rightTable) {
		this.rightTable = rightTable;
	}

	@Override
	public String toString() {
		return "JoinTablePair [leftTable=" + leftTable + ", rightTable="
				+ rightTable + "]";
	}

	

	
	
	

}
