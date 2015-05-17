package de.webdataplatform.settings;


public class CreateBaseTable implements ICreateTable{

	
	private String name;
	
	private String distribution;
	
	private long numOfOperations;
	
	private int numOfRegions;
	
	private boolean useUpdates;
	
	private boolean useDeletes;

	public CreateBaseTable() {

	}
	public CreateBaseTable(String name) {
		super();
		this.name = name;
	}


	public CreateBaseTable(String name, String distribution,
			long numOfOperations, int numOfRegions, boolean useUpdates,
			boolean useDeletes) {
		super();
		this.name = name;
		this.distribution = distribution;
		this.numOfOperations = numOfOperations;
		this.numOfRegions = numOfRegions;
		this.useUpdates = useUpdates;
		this.useDeletes = useDeletes;
	}

	public CreateBaseTable copy(){
		
		CreateBaseTable createBaseTable = new CreateBaseTable();
		createBaseTable.setName(this.name);
		createBaseTable.setDistribution(distribution);
		createBaseTable.setNumOfOperations(this.numOfOperations);
		createBaseTable.setNumOfRegions(this.numOfRegions);
		createBaseTable.setUseUpdates(this.useUpdates);
		createBaseTable.setUseDeletes(this.useDeletes);
		return createBaseTable;
		
		
	}
	
	
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDistribution() {
		return distribution;
	}

	public void setDistribution(String distribution) {
		this.distribution = distribution;
	}

	public long getNumOfOperations() {
		return numOfOperations;
	}

	public void setNumOfOperations(long numOfOperations) {
		this.numOfOperations = numOfOperations;
	}

	public boolean isUseUpdates() {
		return useUpdates;
	}

	public void setUseUpdates(boolean useUpdates) {
		this.useUpdates = useUpdates;
	}

	public boolean isUseDeletes() {
		return useDeletes;
	}

	public void setUseDeletes(boolean useDeletes) {
		this.useDeletes = useDeletes;
	}


	
	

	public void setNumOfRegions(int numOfRegions) {
		this.numOfRegions = numOfRegions;
	}

	@Override
	public int getNumOfRegions() {
		return numOfRegions;
	}

	@Override
	public String getControlTables() {
		return null;
	}

	@Override
	public String getType() {
		return "basetable";
	}


	@Override
	public String toString() {
		return "CreateBaseTable [name=" + name + ", distribution="
				+ distribution + ", numOfOperations=" + numOfOperations
				+ ", numOfRegions=" + numOfRegions + ", useUpdates="
				+ useUpdates + ", useDeletes=" + useDeletes + "]";
	}


	@Override
	public String getBasetable() {
		// TODO Auto-generated method stub
		return null;
	}



	
	

}
