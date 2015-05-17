package de.webdataplatform.viewmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.webdataplatform.settings.SystemConfig;

public class MessageResolver {


	private String previousMessage;
	

	
	public MessageResolver(){
		
		previousMessage = "";
	}
	
	
	
	/**
	 * @param args
	 */
	public  List<String> resolve(String message) {


		  Pattern pattern = Pattern.compile(SystemConfig.MESSAGES_STARTSEQUENCE+".*?"+SystemConfig.MESSAGES_ENDSEQUENCE);
		    // in case you would like to ignore case sensitivity,
		    // you could use this statement:
		    // Pattern pattern = Pattern.compile("\\s+", Pattern.CASE_INSENSITIVE);
		    Matcher matcher = pattern.matcher(message);
		    // check all occurance
		    int matcherEnd= 0;
		    int matcherStart= 0;
		    String matcherGroup="";
		    List<String> result = new ArrayList<String>();
		    boolean first =true;
		    int matches = 0;
		    while (matcher.find()) {
		      matches++;
		      matcherStart = matcher.start();
		      matcherEnd = matcher.end();
		      matcherGroup = matcher.group();
//		      System.out.print("Start index: " + matcherStart);
//		      System.out.print(" End index: " + matcherEnd + " ");

		      if(first && matcherStart != 0) result.add(message.substring(0, matcherStart));
//		      System.out.println("matcherGroup: "+matcherGroup);
		      first=false;
		      result.add(matcherGroup);
		    }
		    
		    if(matches == 0 && message.contains(">") && message.contains("<")){
//		    	System.out.println(message.indexOf(">"));
//		    	System.out.println(message.indexOf("<"));
		    	result.add(message.substring(0, message.indexOf(">")+1));
		    	matcherEnd =  message.indexOf("<");
		    }
//		    System.out.println(matcherEnd);
//		    System.out.println(message.length());
		    
		    if(matcherEnd != message.length())result.add(message.substring(matcherEnd));
		    
		    
//		    System.out.println("result: "+result);
		    
		    return retrieveMessages(result);
	}
	
	public List<String> retrieveMessages(List<String> tokens){
		
		
		List<String> messages= new ArrayList<String>();
		
	    for(String update : tokens){
	  	  
	    	
	  	  if(update.contains(SystemConfig.MESSAGES_STARTSEQUENCE) && update.contains(SystemConfig.MESSAGES_ENDSEQUENCE)){
	  		  messages.add(update.replace(SystemConfig.MESSAGES_STARTSEQUENCE, "").replace(SystemConfig.MESSAGES_ENDSEQUENCE, ""));
//	  		  Log.info(this.getClass(),"received update: "+update+" to queue");
	  	  }else{
	  		  
	  		  if(update.contains(SystemConfig.MESSAGES_STARTSEQUENCE)){
	  			  previousMessage=update;
	  		  }
	  		  if(update.contains(SystemConfig.MESSAGES_ENDSEQUENCE)){
	  			  messages.add((previousMessage+update).replace(SystemConfig.MESSAGES_STARTSEQUENCE, "").replace(SystemConfig.MESSAGES_ENDSEQUENCE, ""));
//	  			  Log.info(this.getClass(),"received update: "+previousMessage+update+" to queue");
	  		  }
	  		  if(!update.contains(SystemConfig.MESSAGES_STARTSEQUENCE) && !update.contains(SystemConfig.MESSAGES_ENDSEQUENCE)){
	  			  previousMessage+=update;
	  		  }
	  	  }
	    }
	    return messages;
	    
	}
	

}
