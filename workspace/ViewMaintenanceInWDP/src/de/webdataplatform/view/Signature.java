package de.webdataplatform.view;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Signature implements Serializable{


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Signature() {
		super();
	}

	private List<Long> eids= new ArrayList<Long>();

	public void addEid(Long eid){
		
		eids.add(eid);
	}

	public List<Long> getEids() {
		return eids;
	}

	public void setEids(List<Long> eids) {
		this.eids = eids;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((eids == null) ? 0 : eids.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {

		
		if (obj == null)
			return false;
	
		Signature sig = (Signature)obj;
		
		if(this.eids.size() != sig.eids.size()){

			return false;
		}
		
		for(int i=0; i < this.eids.size(); i++){
			if(!this.eids.get(i).equals(sig.eids.get(i))){

				return false;
			}
		}
	

		return true;
	}

	@Override
	public String toString() {
		return "Signature [eids=" + eids + "]";
	}
	
	
	


}
