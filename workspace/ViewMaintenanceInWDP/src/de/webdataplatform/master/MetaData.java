package de.webdataplatform.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaData {



	private List<String> viewManagers = new ArrayList<String>();
	
	private List<String> viewManagersShutdown = new ArrayList<String>();
	
	private List<String> regionServers = new ArrayList<String>();
	
	private Map<String, List<String>> assignedViewManagers=new HashMap<String, List<String>>();
	
	private Map<String, Integer> statusReports=new HashMap<String, Integer>();
	
	
	private Map<String, String> vmRemoved = new HashMap<String, String>();
	
	private List<String> rsRemoved = new ArrayList<String>();
	
	
	
	public MetaData() {

	}
	
	public void synchronize(){
		
//		TODO
		
		
	}
	
	
	

	public List<String> getViewManagers() {
		return viewManagers;
	}

	public void setViewManagers(List<String> viewManagers) {
		this.viewManagers = viewManagers;
	}

	public List<String> getViewManagersShutdown() {
		return viewManagersShutdown;
	}

	public void setViewManagersShutdown(List<String> viewManagersShutdown) {
		this.viewManagersShutdown = viewManagersShutdown;
	}

	public List<String> getRegionServers() {
		return regionServers;
	}

	public void setRegionServers(List<String> regionServers) {
		this.regionServers = regionServers;
	}

	public Map<String, List<String>> getAssignedViewManagers() {
		return assignedViewManagers;
	}

	public void setAssignedViewManagers(
			Map<String, List<String>> assignedViewManagers) {
		this.assignedViewManagers = assignedViewManagers;
	}

	public Map<String, Integer> getStatusReports() {
		return statusReports;
	}

	public void setStatusReports(Map<String, Integer> statusReports) {
		this.statusReports = statusReports;
	}

	public Map<String, String> getVmRemoved() {
		return vmRemoved;
	}

	public void setVmRemoved(Map<String, String> vmRemoved) {
		this.vmRemoved = vmRemoved;
	}

	public List<String> getRsRemoved() {
		return rsRemoved;
	}

	public void setRsRemoved(List<String> rsRemoved) {
		this.rsRemoved = rsRemoved;
	}

	
	
	
	
}
