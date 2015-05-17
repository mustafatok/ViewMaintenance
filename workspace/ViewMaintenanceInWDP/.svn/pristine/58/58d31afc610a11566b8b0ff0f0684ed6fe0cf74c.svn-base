package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.webdataplatform.view.ViewMode;


public class CreateJoinView implements ICreateTable, ICreateView{


	
	private String name;
	
	private String joinPairView;
	
	private List<String> compositeKey;
	
	private int numOfRegions;
	
	private String controlTable;

	
	
	public CreateJoinView(String name, String joinPairView, List<String> compositeKey, int numOfRegions, String controlTable) {
		super();
		this.name = name;
		this.joinPairView = joinPairView;
		this.numOfRegions = numOfRegions;
		this.controlTable = controlTable;
		this.compositeKey = compositeKey;
	}
	public CreateJoinView() {

	}

	
	public String convertListToString(List<String> list){

		String result = "";
		
		if(list != null && !list.isEmpty()){
			for (String key : list) {
				
				result += key+SystemConfig.MESSAGES_SPLITCONTENTSEQUENCE;
				
			}
			result = result.substring(0, result.length()-1);
		}else{
			result = " ";
		}
		
		return result;
		
	}

	
	
	public static List<String> readListFromString(String inputString){

		
		List<String> result = new ArrayList<String>();
		
		
//		Log.info(this.getClass(), "columnsString: "+columnsString);

		if(!inputString.equals(" ") && !inputString.equals("") && !inputString.equals("=")){
		
			String[] columnsKVs = inputString.split(SystemConfig.MESSAGES_SPLITCONTENTSEQUENCE);
			
			
			for (int i = 0; i < columnsKVs.length; i++) {
				
				result.add(columnsKVs[i]);
				
			}
		}
		return result;
		
	}
	
	
	
	public String getViewDefinition(){
		
		String viewDefinition="";
		

		viewDefinition += ViewMode.JOIN.toString()+","+getName()+","+joinPairView+","+convertListToString(compositeKey)+","+getNumOfRegions()+","+isControlTable();
		

		return viewDefinition;
		
	}
	
	public static CreateJoinView parse(String viewDefintion){
		
		CreateJoinView cJV = new CreateJoinView();
		
		
		cJV.setName(viewDefintion.split(",")[1]);
		
		cJV.setJoinPairView(viewDefintion.split(",")[2]);
		
		cJV.setCompositeKey(readListFromString(viewDefintion.split(",")[3]));

		cJV.setNumOfRegions(Integer.parseInt(viewDefintion.split(",")[4]));

		cJV.setControlTable(viewDefintion.split(",")[5]);
		
		
		
		return cJV;
		
	}
	
	

	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getJoinPairView() {
		return joinPairView;
	}

	public void setJoinPairView(String joinPairView) {
		this.joinPairView = joinPairView;
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


	public List<String> getCompositeKey() {
		return compositeKey;
	}
	public void setCompositeKey(List<String> compositeKey) {
		this.compositeKey = compositeKey;
	}
	

	@Override
	public String toString() {
		return "CreateJoinView [name=" + name + ", joinPairView="
				+ joinPairView + ", compositeKey=" + compositeKey
				+ ", numOfRegions=" + numOfRegions + ", controlTable="
				+ controlTable + "]";
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

		return joinPairView;
	}




	@Override
	public CreateBaseTable copy() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	
	
	
	
}
