package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.List;


public class Experiment {

	
	public int numOfRegionServers;
	public int numOfViewManagers;
	public int numOfClients;
	public int numOfKilledViewManagers;
	
	public List<ICreateTable> createdTables;
	
//	public String distribution;
//	public List<String> viewTypes;
//	public int numOfViews;
//	public long numOfOperations;
//	public long numOfRecords;
//	public int numOfAggregationKeys;
//	public int numOfBaseTableRegions;
//	public int numOfViewTableRegions;
//	public boolean useDeletes;
//	public boolean useUpdates;
	
	
	
	
	
	
	public Experiment() {
		super();
	}

	
	
	



//	public Experiment(int numOfRegionServers, int numOfViewManagers,
//			int numOfKilledViewManagers, String distribution,
//			List<String> viewTypes, int numOfViews, long numOfOperations,
//			long numOfRecords, int numOfAggregationKeys,
//			int numOfBaseTableRegions, int numOfViewTableRegions,
//			boolean useDeletes, boolean useUpdates) {
//		super();
//		this.numOfRegionServers = numOfRegionServers;
//		this.numOfViewManagers = numOfViewManagers;
//		this.numOfKilledViewManagers = numOfKilledViewManagers;
//		this.distribution = distribution;
//		this.viewTypes = viewTypes;
//		this.numOfViews = numOfViews;
//		this.numOfOperations = numOfOperations;
//		this.numOfRecords = numOfRecords;
//		this.numOfAggregationKeys = numOfAggregationKeys;
//		this.numOfBaseTableRegions = numOfBaseTableRegions;
//		this.numOfViewTableRegions = numOfViewTableRegions;
//		this.useDeletes = useDeletes;
//		this.useUpdates = useUpdates;
//	}







//	public Experiment(int numOfRegionServers, int numOfViewManagers, String distribution,
//			List<String> viewTypes, int numOfViews, long numOfOperations,
//			long numOfRecords, int numOfAggregationKeys,
//			int numOfBaseTableRegions, int numOfViewTableRegions,
//			boolean useDeletes, boolean useUpdates, long readDelay) {
//		super();
//		this.numOfRegionServers = numOfRegionServers;
//		this.numOfViewManagers = numOfViewManagers;
//		this.distribution = distribution;
//		this.viewTypes = viewTypes;
//		this.numOfViews = numOfViews;
//		this.numOfOperations = numOfOperations;
//		this.numOfRecords = numOfRecords;
//		this.numOfAggregationKeys = numOfAggregationKeys;
//		this.numOfBaseTableRegions = numOfBaseTableRegions;
//		this.numOfViewTableRegions = numOfViewTableRegions;
//		this.useDeletes = useDeletes;
//		this.useUpdates = useUpdates;
//		this.readDelay = readDelay;
//	}

	public Experiment(int numOfRegionServers, int numOfViewManagers, int numOfClients, int numOfKilledViewManagers, List<ICreateTable> createdTables) {
		super();
		this.numOfRegionServers = numOfRegionServers;
		this.numOfViewManagers = numOfViewManagers;
		this.numOfClients = numOfClients;
		this.numOfKilledViewManagers = numOfKilledViewManagers;
		this.createdTables = createdTables;
	}







	public int getNumOfRegionServers() {
		return numOfRegionServers;
	}
	public void setNumOfRegionServers(int numOfRegionServers) {
		this.numOfRegionServers = numOfRegionServers;
	}
	public int getNumOfViewManagers() {
		return numOfViewManagers;
	}
	public void setNumOfViewManagers(int numOfViewManagers) {
		this.numOfViewManagers = numOfViewManagers;
	}
	
	
	
	
//	public String getDistribution() {
//		return distribution;
//	}
//	public void setDistribution(String distribution) {
//		this.distribution = distribution;
//	}
//	public List<String> getViewTypes() {
//		return viewTypes;
//	}
//	public void setViewTypes(List<String> viewTypes) {
//		this.viewTypes = viewTypes;
//	}
//	public int getNumOfViews() {
//		return numOfViews;
//	}
//	public void setNumOfViews(int numOfViews) {
//		this.numOfViews = numOfViews;
//	}
//	public long getNumOfOperations() {
//		return numOfOperations;
//	}
//	public void setNumOfOperations(long numOfOperations) {
//		this.numOfOperations = numOfOperations;
//	}
//	public long getNumOfRecords() {
//		return numOfRecords;
//	}
//	public void setNumOfRecords(long numOfRecords) {
//		this.numOfRecords = numOfRecords;
//	}
//	public int getNumOfAggregationKeys() {
//		return numOfAggregationKeys;
//	}
//	public void setNumOfAggregationKeys(int numOfAggregationKeys) {
//		this.numOfAggregationKeys = numOfAggregationKeys;
//	}
//	public int getNumOfBaseTableRegions() {
//		return numOfBaseTableRegions;
//	}
//	public void setNumOfBaseTableRegions(int numOfBaseTableRegions) {
//		this.numOfBaseTableRegions = numOfBaseTableRegions;
//	}
//	public int getNumOfViewTableRegions() {
//		return numOfViewTableRegions;
//	}
//	public void setNumOfViewTableRegions(int numOfViewTableRegions) {
//		this.numOfViewTableRegions = numOfViewTableRegions;
//	}
//	public boolean isUseDeletes() {
//		return useDeletes;
//	}
//	public void setUseDeletes(boolean useDeletes) {
//		this.useDeletes = useDeletes;
//	}
//	public boolean isUseUpdates() {
//		return useUpdates;
//	}
//	public void setUseUpdates(boolean useUpdates) {
//		this.useUpdates = useUpdates;
//	}

	public int getNumOfClients() {
		return numOfClients;
	}







	public void setNumOfClients(int numOfClients) {
		this.numOfClients = numOfClients;
	}







	public int getNumOfKilledViewManagers() {
		return numOfKilledViewManagers;
	}

	public void setNumOfKilledViewManagers(int numOfKilledViewManagers) {
		this.numOfKilledViewManagers = numOfKilledViewManagers;
	}







	public List<ICreateTable> getCreatedTables() {
		return createdTables;
	}







	public void setCreatedTables(List<ICreateTable> createdTables) {
		this.createdTables = createdTables;
	}


	public List<CreateBaseTable> getCreateBaseTablesAsBaseTables(){
		
		List<CreateBaseTable> result = new ArrayList<CreateBaseTable>();
		

			
		for (ICreateTable iCreateTable : createdTables) {
			
			if(iCreateTable instanceof CreateBaseTable){
				
				result.add((CreateBaseTable)iCreateTable);
			}
			
		}
		
		return result;
		
	}
	
	public List<ICreateTable> getCreateBaseTables(){
	
	List<ICreateTable> result = new ArrayList<ICreateTable>();
	
		
		for (ICreateTable iCreateTable : createdTables) {
			
			if(iCreateTable instanceof CreateBaseTable){
				
				result.add(iCreateTable);
			}
			
		}
	return result;
	
}
	
	
	public List<ICreateTable> getCreateViewTables(){
		
		List<ICreateTable> result = new ArrayList<ICreateTable>();
		

			
			for (ICreateTable iCreateTable : createdTables) {
				
				if(iCreateTable instanceof CreateAggregationView || iCreateTable instanceof CreateSelectionView ||  iCreateTable instanceof CreateJoinView  ||  iCreateTable instanceof CreateReverseJoinView ||  iCreateTable instanceof CreateIndexView ||  iCreateTable instanceof CreateDeltaView){
					
					result.add(iCreateTable);
				}
				
			}
		
		return result;
		
	}	




	@Override
	public String toString() {
		return "Experiment [numOfRegionServers=" + numOfRegionServers
				+ ", numOfViewManagers=" + numOfViewManagers
				+ ", numOfKilledViewManagers=" + numOfKilledViewManagers
				+ ", createdTables=" + createdTables + "]";
	}


//
//	@Override
//	public String toString() {
//		return "Experiment [numOfRegionServers=" + numOfRegionServers
//				+ ", numOfViewManagers=" + numOfViewManagers
//				+ ", numOfKilledViewManagers=" + numOfKilledViewManagers
//				+ ", distribution=" + distribution + ", viewTypes=" + viewTypes
//				+ ", numOfViews=" + numOfViews + ", numOfOperations="
//				+ numOfOperations + ", numOfRecords=" + numOfRecords
//				+ ", numOfAggregationKeys=" + numOfAggregationKeys
//				+ ", numOfBaseTableRegions=" + numOfBaseTableRegions
//				+ ", numOfViewTableRegions=" + numOfViewTableRegions
//				+ ", useDeletes=" + useDeletes + ", useUpdates=" + useUpdates
//				+ "]";
//	}




	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
