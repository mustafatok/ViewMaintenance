package de.webdataplatform.settings;

public class KeyDefinition {

	
	

	private String prefix;
	
	private long startRange;
	
	private long endRange;

	
	public long getNumOfPrimaryKeys(){
		
		return endRange - startRange;
	}
	

	public KeyDefinition(String prefix, long startRange, long endRange) {
		super();
		this.prefix = prefix;
		this.startRange = startRange;
		this.endRange = endRange;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public long getStartRange() {
		return startRange;
	}

	public void setStartRange(long startRange) {
		this.startRange = startRange;
	}

	public long getEndRange() {
		return endRange;
	}

	public void setEndRange(long endRange) {
		this.endRange = endRange;
	}

	@Override
	public String toString() {
		return "KeyDefinition [prefix=" + prefix + ", startRange=" + startRange
				+ ", endRange=" + endRange + "]";
	}
	
	
	
	
	
}
