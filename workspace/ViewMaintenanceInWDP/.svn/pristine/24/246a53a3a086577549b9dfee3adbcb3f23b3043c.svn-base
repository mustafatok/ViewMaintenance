package de.webdataplatform.viewmanager.processing;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AnalyzeLoad {

	
	private List<Integer> queueSizes;
	

	
	public AnalyzeLoad() {
		queueSizes = new ArrayList<Integer>();

	}	
	
	private long lastMeasure= new Date().getTime();
	
	public void process(int queueSize){
		
		queueSizes.add(queueSize);
		
//		if((currentTime - lastMeasure) > Monitoring.DISPLAY_INTERVAL_REGIONSERVER){
//			lastMeasure = currentTime;
//
//			
//		}
		
		
//		long currentTime = new Date().getTime();
//		if((currentTime - lastMeasure) > Monitoring.DISPLAY_INTERVAL_VIEWMANAGER){
//			lastMeasure = currentTime;
//			Log.info(this.getClass()," load currently at "+queueSize);
//		}

	}
	
	
	

}
