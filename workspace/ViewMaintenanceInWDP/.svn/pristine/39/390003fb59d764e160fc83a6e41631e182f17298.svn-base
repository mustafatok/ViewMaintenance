package de.webdataplatform.master;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.webdataplatform.log.Log;
import de.webdataplatform.message.Message;
import de.webdataplatform.message.MessageClient;
import de.webdataplatform.message.SystemID;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.system.Command;
import de.webdataplatform.system.Component;
import de.webdataplatform.system.Event;

public class ComponentController implements Runnable{


	private Queue<String> componentMessages;
	
	private MetaData metaData;
	
//	private Map<ICommandCaller, Command> executedCommands=new HashMap<ICommandCaller, Command>();
	
	private Queue<CommandCall> commandsToExecute = new ConcurrentLinkedQueue<CommandCall>();
	
	private List<String> finishedRegionServers=new ArrayList<String>();
	
	private Log log;
	
	
	public ComponentController(Log log, MetaData metaData, Queue<String> componentMessages) {
		super();
		this.componentMessages = componentMessages;
		this.metaData = metaData;
		this.log = log;
		
		PrintWriter writer=null;
		
		try {
			writer = new PrintWriter("update-process-finished", "UTF-8");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		writer.println("false");
		writer.flush();
		writer.close();
	}



	
//	MESSAGING
	
	
	public void sendMessage(String ip, int port, Message message){
		
		
		MessageClient.send(log, ip, port, message);
		
		
	}
	
	public void sendMessageToSystemID(String systemID, Message message){
		
		
		SystemID masterID = new SystemID(systemID);
		
		sendMessage(masterID.getIp(), masterID.getMessagePort(), message);	
		
	}
	

//	COMPONENT CALLS
	
	
	public void assignViewManager(String viewManager, String regionServer){
		
		log.message(this.getClass(),"command assign vm:"+viewManager+" to rs:"+regionServer);
		
		Message message = new Message(Component.master, "master1", Command.ASSIGN_VIEWMANAGER, regionServer);
		
		sendMessageToSystemID(viewManager,  message);
	}
	
	
	public void withdrawViewManager(String viewManager){
		
		log.message(this.getClass(),"command withdraw vm:"+viewManager);
		
		Message message = new Message(Component.master, "master1", Command.WITHDRAW_VIEWMANAGER, viewManager);
		
		sendMessageToSystemID(viewManager,  message);
	}
	
	
	public void withdrawCrashedViewManager(String viewManager, String regionServer){
		
		log.message(this.getClass(),"command withdraw crashed vm:"+viewManager);
		
		Message message = new Message(Component.master, "master1", Command.WITHDRAW_CRASHED_VIEWMANAGER, viewManager);
		
		sendMessageToSystemID(regionServer,  message);
	}
	
	public void replayWriteAheadLog(String regionServer, String seqNo){
		
		log.message(this.getClass(),"command replay wal:"+seqNo);
		
		Message message = new Message(Component.master, "master1", Command.REPLAY_WRITEAHEADLOG, seqNo+"");
		
		sendMessageToSystemID(regionServer,  message);
	}	
	
	public void reassignViewManager(String viewManager, String newRegionServer){
		
		log.message(this.getClass(),"command reassign vm:"+viewManager+" to rs:"+newRegionServer);
		
		Message message = new Message(Component.master, "master1", Command.REASSIGN_VIEWMANAGER, viewManager);
		
		sendMessageToSystemID(viewManager,  message);
		
	}
	
	public void shutdownViewManager(String viewManager){
		
		log.message(this.getClass(),"command shutdown vm:"+viewManager);
		
		Message message = new Message(Component.master, "master1", Command.SHUTDOWN_VIEWMANAGER, viewManager);
		
		sendMessageToSystemID(viewManager,  message);	
	}
	
	
//	COMPONENT CALLBACKS
	
	public void viewManagerAssigned(String viewManager, String regionServer){
		
		log.info(this.getClass(),"view manager has been assigned: "+viewManager);
		
		List<String> viewManagers = metaData.getAssignedViewManagers().get(regionServer);
		
		if(viewManagers == null)viewManagers = new ArrayList<String>();
		
		viewManagers.add(viewManager);
		
		metaData.getAssignedViewManagers().put(regionServer, viewManagers);
		
		log.info(this.getClass(),"assigned managers: "+metaData.getAssignedViewManagers());

	}
	
	public String viewManagerWithdrawn(String viewManager){
		
		log.info(this.getClass(),"view manager has been withdrawn: "+viewManager);
		
		log.info(this.getClass(),"assigned managers: "+metaData.getAssignedViewManagers());

		String regionServer = lookupRSOfViewManager(viewManager);
		
		metaData.getAssignedViewManagers().get(regionServer).remove(viewManager);

		return regionServer;


	}
	
	
	public void crashedViewManagerWithdrawn(String viewManager){
		
		log.info(this.getClass(),"crashed view manager has been withdrawn: "+viewManager);
		
		String regionServer = lookupRSOfViewManager(viewManager);
		
		metaData.getAssignedViewManagers().get(regionServer).remove(viewManager);


		log.info(this.getClass(),"assigned managers: "+metaData.getAssignedViewManagers());

	}
	
	public void viewManagerReassigned(String viewManager, String regionServer){
		
		log.info(this.getClass(),"view manager has been reassigned: "+viewManager);
		
		viewManagerWithdrawn(viewManager);

		viewManagerAssigned(viewManager, regionServer);
		
	}	
	
	public void viewManagerShutdown(String viewManager){
		
		log.info(this.getClass(),"view manager shuts down: "+viewManager);

		String regionServer = viewManagerWithdrawn(viewManager);
		
		metaData.getVmRemoved().put(viewManager, regionServer);
				
		log.info(this.getClass(),"assigned managers: "+metaData.getAssignedViewManagers());
		
	}
	
	
	
	public String lookupRSOfViewManager(String viewManager){
		
		for(String regionServer : metaData.getAssignedViewManagers().keySet()){
			
			List<String> viewManagers = metaData.getAssignedViewManagers().get(regionServer);
			if(viewManagers.contains(viewManager)){
				return regionServer;
				
			}
		}
		return null;
	}
	
	public synchronized void queueCommand(ICommandCaller caller, Command command){
		
		commandsToExecute.add(new CommandCall(caller, command));
	}

	public void sendCommand(ICommandCaller caller, Command command){
	
		
		        command.setExecutionTimestamp(new Date().getTime());
		
				switch(command.getType()){
	
						case Command.ASSIGN_VIEWMANAGER : 
							log.info(this.getClass(),"assign view manager:"+command.getViewManager()+" to:"+command.getRegionServer());
							assignViewManager(command.getViewManager(), command.getRegionServer());
						break;	
						case Command.WITHDRAW_VIEWMANAGER : 
							log.info(this.getClass(),"withdraw view manager "+command.getViewManager());
							withdrawViewManager(command.getViewManager());
						break;
						case Command.WITHDRAW_CRASHED_VIEWMANAGER : 
							log.info(this.getClass(),"withdraw crashed view manager "+command.getViewManager());
							withdrawCrashedViewManager(command.getViewManager(), command.getRegionServer());
						break;
						case Command.REPLAY_WRITEAHEADLOG : 
							log.info(this.getClass(),"replay write ahead log "+command.getContent());
							replayWriteAheadLog(command.getRegionServer(), command.getContent());
						break;
						case Command.REASSIGN_VIEWMANAGER : 
							log.info(this.getClass(),"reassign view manager "+command.getViewManager()+", to:"+command.getRegionServer());
							reassignViewManager(command.getViewManager(), command.getRegionServer());
						break;
						case Command.SHUTDOWN_VIEWMANAGER : 
							log.message(this.getClass(),"view manager shutdown "+command.getViewManager());
							shutdownViewManager(command.getViewManager());
						break;		
						default:
							log.info(this.getClass(),"command:  "+command.getType()+" not supported by component controller!!!");
							commandInProgress = null;
						break;		
				}

			
	}
	
	
	
	public String receiveCommand(String name, String eventType){
		

		if(commandInProgress != null){	
			
			Command command = commandInProgress.getCommand();
			
		
			if(name.equals(command.getViewManager())){
			
				if((command.getType().equals(Command.ASSIGN_VIEWMANAGER) && eventType.equals(Event.VIEWMANAGER_ASSIGNED)) ||
				(command.getType().equals(Command.REASSIGN_VIEWMANAGER) && eventType.equals(Event.VIEWMANAGER_REASSIGNED)) ||
				(command.getType().equals(Command.WITHDRAW_VIEWMANAGER) && eventType.equals(Event.VIEWMANAGER_WITHDRAWN)) ||
				(command.getType().equals(Command.SHUTDOWN_VIEWMANAGER) && eventType.equals(Event.VIEWMANAGER_SHUTDOWN))){
					
					log.info(this.getClass(),"command found: "+command+", calling back");
					commandInProgress.getCommandCaller().callbackExecuteCommand(command);
					commandInProgress=null;
					
				}
			}

			if(name.equals(command.getRegionServer())){

				if((command.getType().equals(Command.WITHDRAW_CRASHED_VIEWMANAGER) && eventType.equals(Event.CRASHED_VIEWMANAGER_WITHDRAWN))  ||
						(command.getType().equals(Command.REPLAY_WRITEAHEADLOG) && eventType.equals(Event.WRITEAHEADLOG_REPLAYED))){
					
					log.info(this.getClass(),"command found: "+command+", calling back");
					commandInProgress.getCommandCaller().callbackExecuteCommand(command);
					commandInProgress=null;
					
				}
			}
					
		}			
					
		return null;
	}

	public void checkCommands(){
		
				
			long currentTime = new Date().getTime();
			
			if(commandInProgress != null){
				if((currentTime - commandInProgress.getCommand().getExecutionTimestamp()) > SystemConfig.MESSAGES_RETRYINTERVAL){
					
					log.info(this.getClass(),"command retries: "+commandInProgress.getCommand().getRetries());
					if(commandInProgress.getCommand().getRetries() < SystemConfig.MESSAGES_NUMOFRETRIES){
						
						log.info(this.getClass(),"command:"+commandInProgress.getCommand()+" can not be executed, component not responding, sending again");
						commandInProgress.getCommand().setRetries(commandInProgress.getCommand().getRetries()+1);
						sendCommand(commandInProgress.getCommandCaller(), commandInProgress.getCommand());
						
					}else{
						log.info(this.getClass(),"command"+commandInProgress.getCommand()+" can not be executed, component cannot be reached, giving up");
						commandInProgress.getCommandCaller().callbackExecuteCommand(commandInProgress.getCommand());
						commandInProgress = null;
					}
				}
			}

		
	}

	
	CommandCall commandInProgress=null;
	
	
	@Override
	public void run() {
		
		while(true){
			
			try{
			
			String messageString = componentMessages.poll();
			
			if(messageString != null){
				
				receiveMessage(messageString);
				
			}

			checkCommands();
			
			if(commandInProgress == null){
				
				commandInProgress = commandsToExecute.poll();
				
				if(commandInProgress != null){
					
					log.info(this.getClass(),"executing command call"+commandInProgress+" ");
					sendCommand(commandInProgress.getCommandCaller(), commandInProgress.getCommand());	
					
				}

			}
			
			
			try {
				Thread.sleep(SystemConfig.MESSAGES_POLLINGINTERVAL);
			} catch (InterruptedException e) {
	
				e.printStackTrace();
			}
			
			
			
		}catch(Exception e){
			log.error(this.getClass(), e);
		}
	}

	}




	private void receiveMessage(String messageString) {
		
		Message message = new Message(messageString);
		
		log.info(this.getClass(),"received message: "+message);

		
		String component = message.getComponent();
		String operation = message.getOperation();
		

		
		switch(component){
		

			case Component.regionServer : 
					switch(operation){
						case Event.STATUS_REPORT_REGIONSERVER : 
							log.message(this.getClass(),"status report from "+message.getName()+":"+message.getContent());
							metaData.getStatusReports().put(message.getName(), Integer.parseInt(message.getContent()));
						break;	
						case Event.CRASHED_VIEWMANAGER_WITHDRAWN: 
							log.message(this.getClass(),"crashed view manager:"+message.getContent()+" withdrawn from:"+message.getName());
							crashedViewManagerWithdrawn(message.getContent());
							receiveCommand(message.getName(), Event.CRASHED_VIEWMANAGER_WITHDRAWN);
						break;
						case Event.WRITEAHEADLOG_REPLAYED: 
							log.message(this.getClass(),"write ahead log replayed:"+message.getContent()+" from:"+message.getName());
							receiveCommand(message.getName(), Event.WRITEAHEADLOG_REPLAYED);
						break;
						case Event.PROCESS_FINISHED: 
							log.info(this.getClass(),"updateProcess finished for region server:"+message.getName());
							
							for(String regionServer : metaData.getRegionServers()){
								
								if(regionServer.equals(message.getName())){
									finishedRegionServers.add(regionServer);
								}
							}
							log.info(this.getClass(),"finished region servers:"+finishedRegionServers);
							if(finishedRegionServers.size() != 0 && finishedRegionServers.containsAll(metaData.getRegionServers())){
								log.info(this.getClass(),"Entire updateProcess completed:"+message.getName());

								
								PrintWriter writer=null;
				
								try {
									writer = new PrintWriter("update-process-finished", "UTF-8");
									
								} catch (FileNotFoundException e) {
									e.printStackTrace();
								} catch (UnsupportedEncodingException e) {
									e.printStackTrace();
								}

								writer.println("true");
								writer.flush();
								writer.close();
									
								
							}

						break;
					}
				break;				
			case Component.viewManager : 
					switch(operation){
					case Event.VIEWMANAGER_ASSIGNED : 
						log.message(this.getClass(),"view manager assigned "+message.getName()+", to:"+message.getContent());
						viewManagerAssigned(message.getName(), message.getContent());
						receiveCommand(message.getName(), Event.VIEWMANAGER_ASSIGNED);
					break;	
					case Event.VIEWMANAGER_REASSIGNED : 
						log.message(this.getClass(),"view manager reassigned "+message.getName()+", to:"+message.getContent());
						viewManagerReassigned(message.getName(), message.getContent());
						receiveCommand(message.getName(), Event.VIEWMANAGER_REASSIGNED);
					break;
					case Event.VIEWMANAGER_WITHDRAWN : 
						log.message(this.getClass(),"view manager withdrawn "+message.getName()+", to:"+message.getContent());
						viewManagerWithdrawn(message.getName());
						receiveCommand(message.getName(), Event.VIEWMANAGER_WITHDRAWN);
					break;
					case Event.VIEWMANAGER_SHUTDOWN : 
						log.message(this.getClass(),"view manager shutdown "+message.getName());
						viewManagerShutdown(message.getName());
						receiveCommand(message.getName(), Event.VIEWMANAGER_SHUTDOWN);
					break;							
					}
				break;
		}
	}

}
