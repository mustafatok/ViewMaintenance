package de.webdataplatform.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import de.webdataplatform.settings.SystemConfig;

public class Log {

	/**
	 * @param args
	 */
	
	public String name;

	
	public Log(String name) {
		super();
		this.name = name;
	}


	private PrintWriter writer;
	
	public PrintWriter getPrintWriter(){
		
		
		if(writer == null){
			
//			String path = "."; 
//			  
//			String files;
			File folder = new File("logs");
			if(!folder.exists())folder.mkdir();
//			File[] listOfFiles = folder.listFiles(); 
//			 
//			for (int i = 0; i < listOfFiles.length; i++) 
//			{
//			 
//			   if (listOfFiles[i].isFile()) 
//			   {
//			   files = listOfFiles[i].getName();
//			   System.out.println(files);
//			   }
//			}
			
			try {
				writer = new PrintWriter("logs/"+name, "UTF-8");
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

	
		}
		return writer;
	}
			
			
			
			
	
	
	public void info(Class class_, String logMessage) {
	
		if(SystemConfig.LOGGING_CONSOLE == null)SystemConfig.LOGGING_CONSOLE = true;
		if(SystemConfig.LOGGING_CONSOLE == null)SystemConfig.LOGGING_FILE = false;
		
		
		if(SystemConfig.LOGGING_CONSOLE)System.out.println(class_.getName()+": "+logMessage);
		
		if(SystemConfig.LOGGING_FILE){
			getPrintWriter().println(class_.getName()+": "+logMessage);
			getPrintWriter().flush();
		}

	}
	
	public void wal(Class class_, String logMessage) {
		if(SystemConfig.REGIONSERVER_LOGWAL == true)info(class_, logMessage);
		
	}
	
	public void updates(Class class_, String logMessage) {
		if(SystemConfig.VIEWMANAGER_LOGUPDATES == true)info(class_, logMessage);
		
	}
	public void performance(Class class_, String logMessage) {
		if(SystemConfig.VIEWMANAGER_LOGPERFORMANCE == true)info(class_, logMessage);
		
	}
	
	public void error(Class class_, Exception e) {
		
		if(SystemConfig.LOGGING_CONSOLE){
			e.printStackTrace();
		}
		
		if(SystemConfig.LOGGING_FILE){
			
			getPrintWriter().println(class_.getName()+"///////////////////////////////////////////////////////");
			getPrintWriter().println(e);
			for(StackTraceElement s : e.getStackTrace()){
				getPrintWriter().println("\tat " + s.getClassName() + "." + s.getMethodName()
			        + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
			}
			getPrintWriter().println(class_.getName()+"///////////////////////////////////////////////////////");
			
			getPrintWriter().flush();
		}

	}
	
	public void infoToFile(Class class_, String logMessage) {
		

			getPrintWriter().println(class_.getName()+": "+logMessage);
			getPrintWriter().flush();


	}
	
	public void message(Class class_, String logMessage) {
		
//		if(Constants.LOG_TO_CONSOLE){
//			System.out.println(class_.getName()+": "+logMessage);
//		}
//		if(Constants.LOG_TO_FILE){
//			getPrintWriter().println(class_.getName()+": "+logMessage);
//			getPrintWriter().flush();
//		}
	}
	
	public void update(Class class_, String logMessage) {
		
//		if(Constants.LOG_TO_CONSOLE)System.out.println(class_.getName()+": "+logMessage);
//		if(Constants.LOG_TO_FILE){
//			getPrintWriter().println(class_.getName()+": "+logMessage);
//			getPrintWriter().flush();
//		}
	}
	
	
	public void close() {
		
		
		if(writer != null)writer.close();
		writer = null;

	}

}
