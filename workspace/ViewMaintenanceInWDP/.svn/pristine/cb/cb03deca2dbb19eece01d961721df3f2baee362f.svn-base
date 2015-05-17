package de.webdataplatform.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import de.webdataplatform.settings.SystemConfig;

public class ResultAggregator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
//		String directoryName = "./testrun,10,10,1392718229198/viewmanagerStatistics";
		String directoryName = "results_sum";
		
		File dir = new File(directoryName);
		
		dir = new File(directoryName);
		for (File testdir : dir.listFiles()) {
		
				long averageThrouput=0;
				long maxThrouput=0;
				long maxTime=0;
				long avgTime=0;
				int count=0;
				Long totalUpdates=0l;
				
				File testdirVM = new File(testdir.getAbsolutePath()+"/viewmanagerStatistics");
				
//				System.out.print(testdir.getName()+" ");
				System.out.print(testdirVM.getAbsolutePath().split(",")[4].substring(0, 2)+" ");
				Long timeSpanCum = 0l;
				for (File file : testdirVM.listFiles()) {
					
//					System.out.println(file.getName());
		
					Long updatesReceived = 0l;
					Long updatesPreProcessed = 0l;
					Long viewRecordUpdates= 0l;
					Long commitLogUpdates= 0l;
					Long incomingUpdates= 0l;
					Long preprocessingUpdates= 0l;
					Long throughput= 0l;
					Long timeSpan= 0l;
					
					String string;
					BufferedReader br=null;
					try {
						br = new BufferedReader(new FileReader(file));
					} catch (FileNotFoundException e1) {
						e1.printStackTrace();
					}
					 
		//				System.out.println(sCurrentLine);
		//			}
		//			
					try {
						while ((string = br.readLine()) != null) {
				
		//							System.out.println(string);
									String[] parts = string.split(";");
		//							System.out.println(Arrays.toString(parts));
									if(parts.length == 8){
									
										if(Long.parseLong(parts[0]) > updatesReceived)updatesReceived = Long.parseLong(parts[0]);
										if(Long.parseLong(parts[1]) > updatesPreProcessed)updatesPreProcessed = Long.parseLong(parts[1]);
										if(Long.parseLong(parts[2]) > viewRecordUpdates)viewRecordUpdates= Long.parseLong(parts[2]);
										if(Long.parseLong(parts[3]) > commitLogUpdates)commitLogUpdates= Long.parseLong(parts[3]);
										if(Long.parseLong(parts[4]) > incomingUpdates)incomingUpdates= Long.parseLong(parts[4]);
										if(Long.parseLong(parts[5]) > preprocessingUpdates)preprocessingUpdates= Long.parseLong(parts[5]);
										if(Long.parseLong(parts[6]) > throughput)throughput= Long.parseLong(parts[6]);
										if(Long.parseLong(parts[7]) > timeSpan)timeSpan= Long.parseLong(parts[7]);
										
										
									}
								
						}
						List<String> statisticValues = new ArrayList<String>();
						statisticValues.add(updatesReceived+"");
						statisticValues.add(updatesPreProcessed+"");
						statisticValues.add(viewRecordUpdates+"");
						statisticValues.add(commitLogUpdates+"");
						statisticValues.add(incomingUpdates+"");
						statisticValues.add(preprocessingUpdates+"");
						statisticValues.add(throughput+"");
						statisticValues.add(timeSpan+"");
						timeSpanCum += timeSpan;
//						System.out.println("timespan:"+timeSpan);
//						System.out.println("max-throughput:"+throughput);
//						if(timeSpan != 0)System.out.println("average-throughput:"+viewRecordUpdates*1000/timeSpan);
						if(timeSpan != 0)averageThrouput += (viewRecordUpdates*1000/timeSpan);
						
						System.out.println("avgTP:"+(viewRecordUpdates*1000/timeSpan));
						
						totalUpdates += viewRecordUpdates;
						maxThrouput += throughput;
//						System.out.println(viewRecordUpdates);
//						System.out.println(timeSpan);
						if(timeSpan > maxTime)maxTime = timeSpan;
						avgTime += timeSpan;
						count++;
						
					} catch (NumberFormatException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
//				System.out.print("timeSpanCum:"+(timeSpanCum/testdirVM.listFiles().length)+", ");
//				System.out.println("---------------------------------------------");
//				System.out.println("max-throughput:"+maxThrouput);
//				System.out.println("average-throughput:"+averageThrouput);
//				System.out.println("maxTime:"+maxTime/1000+"s");
//				long avgTimeOverall=0;
//				if(count!= 0)avgTimeOverall = (avgTime/count)/1000;
//				if(avgTimeOverall!= 0)System.out.print(1000000/avgTimeOverall+" ");
//				if(maxTime != 0)System.out.print((totalUpdates*1000/maxTime)+" ");
				System.out.print(maxTime+" ");
				System.out.print(maxThrouput+" ");
				System.out.println(averageThrouput);
				System.out.println("---------------------------------------------");
		//		System.out.println(result);
				
		//		boolean statisticsLogStarted=false;
		//		
		//		Long updatesRetrieved = 0l;
		//		Long updatesAssigned = 0l;
		//		Long updatesSent= 0l;
		//		Long incomingUpdates= 0l;
		//		Long throughput= 0l;
		//		Long timeSpan= 0l;
		//		
		//		for (String string : result) {
		//			
		////			System.out.println(string);
		//			
		////			if(statisticsLogStarted){
		//				
		//				try{
		//					
		//					
		//					String[] parts = string.split(Constants.STATISTICS_LINE_SEPARATOR);
		////					System.out.println("pl: "+parts.length);
		//					if(parts.length == 7){
		//					
		//						if(Long.parseLong(parts[0]) > updatesRetrieved)updatesRetrieved = Long.parseLong(parts[0]);
		//						if(Long.parseLong(parts[1]) > updatesAssigned)updatesAssigned = Long.parseLong(parts[1]);
		//						if(Long.parseLong(parts[2]) > updatesSent)updatesSent= Long.parseLong(parts[2]);
		//						if(Long.parseLong(parts[3]) > incomingUpdates)incomingUpdates= Long.parseLong(parts[3]);
		//						if(Long.parseLong(parts[4]) > incomingUpdates)incomingUpdates= Long.parseLong(parts[4]);
		//						if(Long.parseLong(parts[5]) > throughput)throughput= Long.parseLong(parts[5]);
		////						if(Long.parseLong(parts[6]) > timeSpan)timeSpan= Long.parseLong(parts[6]);
		////						System.out.println("ur: "+updatesRetrieved);
		//					}
		//				}catch(Exception e){
		//					e.printStackTrace();
		//				}
		//				
		//			}
		////			if(string.contains("statisticLogContent"))statisticsLogStarted=true;
		//		
		//		
		//		List<String> statisticValues = new ArrayList<String>();
		//		statisticValues.add(updatesRetrieved+"");
		//		statisticValues.add(updatesAssigned+"");
		//		statisticValues.add(updatesSent+"");
		//		statisticValues.add(incomingUpdates+"");
		//		statisticValues.add(incomingUpdates+"");
		//		statisticValues.add(throughput+"");
		////		statisticValues.add(timeSpan+"");
		//		System.out.println(statisticValues);
		//		StatisticLog.info(statisticValues);
				
				
			}
	}	

}
