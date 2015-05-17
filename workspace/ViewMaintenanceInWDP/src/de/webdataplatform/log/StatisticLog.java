package de.webdataplatform.log;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import de.webdataplatform.settings.SystemConfig;

public class StatisticLog {

	/**
	 * @param args
	 */
	
	public static String name;
	
	public static String targetDirectory;
	
	private static PrintWriter writer;
	
	public static PrintWriter getPrintWriter(){
		
		
		if(writer == null){
			
			
			try {
				
				if(targetDirectory == null)writer = new PrintWriter(name+"-statistic.log", "UTF-8");
				else writer = new PrintWriter(targetDirectory+name+"-statistic.log", "UTF-8");
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			
			writer.println("statisticLogContent");
			writer.flush();
			
		}
		return writer;
	}
			
			
			
			
	
	
	public static void info(List<String> statisticValues) {
	
		
		String logMessage="";
		if(SystemConfig.LOGGING_STATISTICS){
			
			for (String value : statisticValues) {
				
				logMessage+=value+SystemConfig.LOGGING_STATISTICSLINESEPARATOR;
				
			}
			getPrintWriter().println(logMessage);
			getPrintWriter().flush();
		}

	}
	
	public static void direct(String statisticValues) {
	
			System.out.println(statisticValues);
			getPrintWriter().println(statisticValues);
			getPrintWriter().flush();

	}
	
	public static void close() {
		
		
		writer.close();
		writer = null;

	}
	
	
	
	
}
