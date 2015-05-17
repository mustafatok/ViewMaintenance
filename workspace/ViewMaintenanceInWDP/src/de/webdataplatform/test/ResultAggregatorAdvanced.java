package de.webdataplatform.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResultAggregatorAdvanced {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
//		String directoryName = "./testrun,10,10,1392718229198/viewmanagerStatistics";
		String directoryName = "testresults";
		int selector = 4;
		File dir = new File(directoryName);
		result = new ArrayList<List<Integer>>();
		
//		int[] seriesSelector = new int[]{6,7,8,9,10};
		int[] seriesSelector = new int[]{28};
		
		System.out.println(Arrays.toString(seriesSelector));
		System.out.println(contains(seriesSelector, 2));
		
		dir = new File(directoryName);
		int i = 1;
		for (File series : dir.listFiles()) {
			if(contains(seriesSelector, i)){
		
				System.out.println("----------------------------------------------------");
				System.out.println(series.getName());
				resultLine = null;
				for (File experiment : series.listFiles()) {
					
					System.out.println("["+experiment.getName().split(",")[3]+","+experiment.getName().split(",")[4]+","+experiment.getName().split(",")[5]+"]");
	
					number = Integer.parseInt(experiment.getName().split(",")[selector]);
					
					readFiles(experiment);
				}
				result.addAll(resultLine);
			}
			i++;
		}
		
		System.out.println(result);
		writeResult();
		
	}
	
	static boolean contains(int[] seriesSelector, int key){
		
		for (int i = 0; i < seriesSelector.length; i++) {
			if(seriesSelector[i]== key)return true;
		}
		return false;
	}
	
	
	static void writeResult(){
		
		int x= 1;
		boolean goOn = true;
		while(goOn){
		
			goOn = false;
			System.out.print(x+"  ");
			for (int i = 0; i < (3 - String.valueOf(x).length()); i++) {
				System.out.print(" ");
			}
			for (List<Integer> line : result) {
				
				if(x <= line.size()){
					System.out.print(line.get(x-1)+"");
					for (int i = 0; i < (10 - line.get(x-1).toString().length()); i++) {
						System.out.print(" ");
					}
					goOn = true;
				}
				
			}
			System.out.println();
			x++;
			
			
		}
		
		
	}
	
	
	static void writeToResultLine(int number, int value){
		
		if(resultLine == null){
			
			resultLine = new ArrayList<List<Integer>>();
		}
		writeLine(number, value, 0);

		
	}

	private static void writeLine(int number, int value, int i) {

		if(resultLine.size() < (i+1)){
			
			resultLine.add(new ArrayList<Integer>());
		}
		
		List<Integer> line = resultLine.get(i);
		
		System.out.println("linesize:"+line.size()+", number:"+number);
		
		if(line.size() > (number-1)){
			writeLine(number, value, i+1);
		}

		while(line.size() < (number-1)){
			
			line.add(0);
			
		}
		if(line.size() == (number-1)){
			line.add(value);
		}
		System.out.println(line);
	}
	
	static List<List<Integer>> resultLine;
	static List<List<Integer>> result;
	static int number;
	static int averageTP;
	
	private static void readFiles(File testdir) {
		
		for (File testFile : testdir.listFiles()) {

			if(testFile.isFile() && testFile.getName().equals("experiment.log"))readFile(testFile);

						
		}
		
		File testdirVM = new File(testdir.getAbsolutePath()+"/viewmanagerStatistics");
		
		Long timeSpanCum = 0l;
		averageTP = 0;
		for (File statisticsFile : testdirVM.listFiles()) {
			
			readStatisticsFile(statisticsFile);
			
		}
		writeToResultLine(number, averageTP);	

	}


	private static void readFile(File testFile) {
	
				
				boolean allNull=false;

					BufferedReader br=null;
					

							
					try {
						br = new BufferedReader(new FileReader(testFile));
					} catch (FileNotFoundException e1) {
						e1.printStackTrace();
					}

					String line;
					try {
						while((line = br.readLine()) != null){

							processLine(line);
							
						}
					} catch (IOException e) {
			
						e.printStackTrace();
					}				
										
				
	}


	private static void processLine(String line) {
		
		if(line.contains("base table filled") || line.contains("view table filled") ){
			
		    Pattern pattern = Pattern.compile("[0-9]+");
		    Matcher matcher = pattern.matcher(line);

		    while (matcher.find()) {
//		      System.out.print("Start index: " + matcher.start());
//		      System.out.print(" End index: " + matcher.end() + " ");
		      System.out.println(matcher.group());
		      
		      writeToResultLine(number, Integer.parseInt(matcher.group()));		         
		      
		    }
		}
	}

	
	private static void readStatisticsFile(File statisticsFile ){
		
//		System.out.println(file.getName());
		
		long averageThrouput=0;
		long maxThrouput=0;
		long maxTime=0;
		long avgTime=0;
//		int count=0;
		Long totalUpdates=0l;
		Long timeSpanCum = 0l;

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
			br = new BufferedReader(new FileReader(statisticsFile));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		 
	
		try {
			while ((string = br.readLine()) != null) {
	

						String[] parts = string.split(";");

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
//			System.out.println("timespan:"+timeSpan);
//			System.out.println("max-throughput:"+throughput);
//			if(timeSpan != 0)System.out.println("average-throughput:"+viewRecordUpdates*1000/timeSpan);
			if(timeSpan != 0){
				averageThrouput += (viewRecordUpdates*1000/timeSpan);
				System.out.println("avgTP:"+(viewRecordUpdates*1000/timeSpan));
				averageTP += (viewRecordUpdates*1000/timeSpan);
			}
			
			
			
			totalUpdates += viewRecordUpdates;
			maxThrouput += throughput;
//			System.out.println(viewRecordUpdates);
//			System.out.println(timeSpan);
			if(timeSpan > maxTime)maxTime = timeSpan;
			avgTime += timeSpan;
//			count++;
			System.out.print(maxTime+" ");
			System.out.print(maxThrouput+" ");
			System.out.println(averageThrouput);
			System.out.println("---------------------------------------------");
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
		

}
