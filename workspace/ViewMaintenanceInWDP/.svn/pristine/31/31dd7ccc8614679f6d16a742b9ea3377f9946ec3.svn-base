package de.webdataplatform.view;

import java.io.Serializable;

public class ViewRecord  implements Serializable, IViewRecord, IViewRecordAggregation, IViewRecordJoin{
	

	/**
	 * 
	 */
	private static final long serialVersionUID = -3355393708281463973L;
	
	private String y;
	
	private int s;
	
	private Signature signature=new Signature();
	

	public ViewRecord(int s) {
		super();
		this.s = s;
	}

	
	public ViewRecord(String y, int s) {
		super();
		this.y = y;
		this.s = s;
	}		

	public Signature getSignature() {
		return signature;
	}

	public void setSignature(Signature signature) {
		this.signature = signature;
	}


	public int getAggregatedValue() {

		return s;
	}


	public void setAggregatedValue(int s) {
		this.s=s;
		
	}


	
	
	@Override
	public String toString() {
		return "ViewTableColumn [s=" + s + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + s;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ViewRecord other = (ViewRecord) obj;
		if (s != other.s)
			return false;
		return true;
	}



	@Override
	public String getForeignKey() {

		return y;
	}



	@Override
	public void setForeignKey(String foreignKey) {
		this.y = foreignKey;
		
	}



	@Override
	public int getValue() {

		return s;
	}



	@Override
	public void setValue(int value) {
		this.s = value;
	}







	
	


	
}
