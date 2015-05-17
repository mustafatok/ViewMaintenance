package de.webdataplatform.test;

import java.lang.reflect.Field;

import org.apache.commons.configuration.DatabaseConfiguration;

import de.webdataplatform.log.Log;
import de.webdataplatform.log.StatisticLog;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.EvaluationSet;
import de.webdataplatform.settings.Experiment;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.settings.VariableParam;

public class TestSetup {


	
	
	

//	private static String user = "jan";
//	private static String password = "MTVdvrDV$15";
//	private static String zookeeper = "ubuntu51";

	private static Log log;
	
	
	public static void main(String[] args) {

//		retrieveIpAddresses();

//		
		StatisticLog.name = "result";
		log = new Log("evaluation.log");

		SystemConfig.load(log);
		NetworkConfig.load(log);
		DatabaseConfig.load(log);
		EvaluationConfig.load(log);
		

		SVMSystem svmTest = new SVMSystem();
		
		if(args[0].equals("stop")){
//			svmTest.stopVMSystem(true, experiment);
		}		if(args[0].equals("start")){
			
			
			
//			EVALUATION
	    
			log.info(TestSetup.class, "Evaluation Sets: "+EvaluationConfig.EVALUATIONSETS);
				for (EvaluationSet evaluationSet : EvaluationConfig.EVALUATIONSETS) {
					
					log.info(TestSetup.class, "Running Evaluation Set: "+evaluationSet);
					
					int startIteration;
					int endIteration;
					int stepWidth;
					boolean isVariableParameter = evaluationSet.getEvaluationParams().getVariableParams() != null && evaluationSet.getEvaluationParams().getVariableParams().size() != 0;
					
					if(isVariableParameter){
						
						log.info(TestSetup.class, "dynamic evaluation ");
						startIteration = evaluationSet.getEvaluationParams().getVariableParams().get(0).getStartValue();
						endIteration = evaluationSet.getEvaluationParams().getVariableParams().get(0).getEndValue()+1;
						stepWidth = evaluationSet.getEvaluationParams().getVariableParams().get(0).getStepWidth();
						
					} else{
						
						log.info(TestSetup.class, "static evaluation ");
						startIteration = 0;
						endIteration = evaluationSet.getEvaluationParams().getIterations();
						stepWidth = 1;
						
					}
					
					
					log.info(TestSetup.class, "start iteration: "+startIteration);
					log.info(TestSetup.class, "end iteration: "+endIteration);
					log.info(TestSetup.class, "dynamic parameter: "+isVariableParameter);
					
					
//					if(evaluationSet.getEvaluationParams().isRecreateDatabase()){
						
					if(evaluationSet.getExperiment().getCreateViewTables().size() != 0){
					
						log.info(TestSetup.class, "Evaluation with base table recreation ");
						for (int i = startIteration; i < endIteration; i+=stepWidth) {
							
							log.info(TestSetup.class, "Running "+i+"th iteration of experiment var:"+evaluationSet.getExperiment());
							svmTest.completeTestRun(evaluationSet.getExperiment());

							if(isVariableParameter){
								
								for (VariableParam variableParam : evaluationSet.getEvaluationParams().getVariableParams()) {
									
									updateVariableParameter(variableParam, evaluationSet.getExperiment());  
								}
								
							}
						}
					}else{
						log.info(TestSetup.class, "Evaluation only with base tables");
						for (int i = startIteration; i < endIteration; i+=stepWidth) {
							
							log.info(TestSetup.class, "Running "+i+"th iteration of experiment var:"+evaluationSet.getExperiment());
							svmTest.onlyBaseTables(evaluationSet.getExperiment());

							if(isVariableParameter){
								
								for (VariableParam variableParam : evaluationSet.getEvaluationParams().getVariableParams()) {
									
									updateVariableParameter(variableParam, evaluationSet.getExperiment());  
								}
								
							}
						}
						
					}
//					}	
//					}else{
//						
//						log.info(TestSetup.class, "Evaluation without base table recreation ");
//						svmTest.completeTestRun(true, false, true, evaluationSet.getExperiment());
//						
//						for (int i = startIteration+1; i < endIteration; i++) {
//							
//							if(isVariableParameter)updateVariableParameter(evaluationSet, i);  
//							log.info(TestSetup.class, "Running "+i+"th iteration of experiment var:"+evaluationSet.getExperiment());
//							svmTest.completeTestRun(true, true, true, evaluationSet.getExperiment());
//							
//						}
//						
//						
//					}

				
							

					
				}

			}
			
			

	}


	private static void updateVariableParameter(VariableParam variableParam, Experiment experiment) {
		try {
		
			variableParam.setCurrentValue(variableParam.getCurrentValue()+variableParam.getStepWidth());
			
			Class<?> c = experiment.getClass();

		    Field var = c.getDeclaredField(variableParam.getVariableParameter());
		    
		    var.set(experiment, variableParam.getCurrentValue());

		  
			
		
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	

/*
	private static List<Node>  getClientNodes() {
		
			List<Node> viewManagerNodes = new ArrayList<Node>();


//			viewManagerNodes.add(new Node("ubuntu51",  new SSHConnection("ubuntu51", 22, user, password)));

			
//			viewManagerNodes.add(new Node("172.17.0.22",  new SSHConnection("131.159.52.62", 49009, user, password)));
			viewManagerNodes.add(new Node("172.17.0.22",  new SSHConnection("172.17.0.22", 22, user, password)));
			viewManagerNodes.add(new Node("172.17.0.42",  new SSHConnection("172.17.0.42", 22, user, password)));
			viewManagerNodes.add(new Node("172.17.0.63",  new SSHConnection("172.17.0.63", 22, user, password)));
			viewManagerNodes.add(new Node("172.17.0.27",  new SSHConnection("172.17.0.27", 22, user, password)));		
			viewManagerNodes.add(new Node("172.17.0.29",  new SSHConnection("172.17.0.29", 22, user, password)));
			viewManagerNodes.add(new Node("172.17.0.54",  new SSHConnection("172.17.0.54", 22, user, password)));
			viewManagerNodes.add(new Node("172.17.0.64",  new SSHConnection("172.17.0.64", 22, user, password)));
			viewManagerNodes.add(new Node("172.17.0.44",  new SSHConnection("172.17.0.44", 22, user, password)));
			
			return viewManagerNodes;
			
	}


	private static Node getMasterNode() {
		
		Node masterNode;
		
		masterNode = new Node("172.17.0.22",  new SSHConnection("172.17.0.22", 22, user, password));
//		masterNode = new Node("172.17.0.22",  new SSHConnection("131.159.52.62", 49009, user, password));
//		masterNode = new Node("ubuntu51",  new SSHConnection("ubuntu51", 22, user, password));
		

		return masterNode;

	}

	
	private static Node getHbaseMasterNode() {
		
		Node masterNode;
		
		masterNode = new Node("172.17.0.63",  new SSHConnection("172.17.0.63", 22, user, password));
//		masterNode = new Node("172.17.0.22",  new SSHConnection("131.159.52.62", 49009, user, password));
//		masterNode = new Node("ubuntu51",  new SSHConnection("ubuntu51", 22, user, password));
		

		return masterNode;

	}
	
	private static void retrieveIpAddresses() {
		

//		for(Node node : getViewManagerNodes()){
		for (int i = 7; i < 69; i++) {

				List<String> commands = new ArrayList<String>();
				commands.add("/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'");
				
				Node node = new Node("131.159.52.62", new SSHConnection("131.159.52.62", 49000+i, user, password));
				List<String> results = SSHService.sendCommand(node.getSshConnection(),commands);

				for (String string : results) {
					
					
					if(string.split(".").length==12){
						System.out.println(node.getSshConnection().getPort()+": "+string.trim());
					}
						
//					System.out.println(string.split(".").length);
//					System.out.println(string);
				}
				


		}
		System.out.println();
	}
	
	private static List<Node> getViewManagerNodes() {
		
		List<Node> viewManagerNodes = new ArrayList<Node>();


		viewManagerNodes.add(new Node("172.17.0.40",  new SSHConnection("172.17.0.40", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.53",  new SSHConnection("172.17.0.53", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.36",  new SSHConnection("172.17.0.36", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.8",  new SSHConnection("172.17.0.8", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.47",  new SSHConnection("172.17.0.47", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.51",  new SSHConnection("172.17.0.51", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.35",  new SSHConnection("172.17.0.35", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.11",  new SSHConnection("172.17.0.11", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.55",  new SSHConnection("172.17.0.55", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.12",  new SSHConnection("172.17.0.12", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.57",  new SSHConnection("172.17.0.57", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.34",  new SSHConnection("172.17.0.34", 22, user, password)));				
		viewManagerNodes.add(new Node("172.17.0.45",  new SSHConnection("172.17.0.45", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.7",  new SSHConnection("172.17.0.7", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.50",  new SSHConnection("172.17.0.50", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.66",  new SSHConnection("172.17.0.66", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.25",  new SSHConnection("172.17.0.25", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.18",  new SSHConnection("172.17.0.18", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.14",  new SSHConnection("172.17.0.14", 22, user, password)));
		viewManagerNodes.add(new Node("172.17.0.37",  new SSHConnection("172.17.0.37", 22, user, password)));
//		viewManagerNodes.add(new Node("172.17.0.41",  new SSHConnection("172.17.0.41", 22, user, password)));

		

		
		 
		
		return viewManagerNodes;
		
	}

	
	
	private static List<Node> getRegionServerNodes() {
		
		List<Node> regionServerNodes = new ArrayList<Node>();
        


		
		regionServerNodes.add(new Node("172.17.0.64",  new SSHConnection("172.17.0.27", 22, user, password)));
		regionServerNodes.add(new Node("172.17.0.63",  new SSHConnection("172.17.0.29", 22, user, password)));
		regionServerNodes.add(new Node("172.17.0.62",  new SSHConnection("172.17.0.54", 22, user, password)));
		regionServerNodes.add(new Node("172.17.0.61",  new SSHConnection("172.17.0.64", 22, user, password)));
		regionServerNodes.add(new Node("172.17.0.60",  new SSHConnection("172.17.0.44", 22, user, password)));
		regionServerNodes.add(new Node("172.17.0.48",  new SSHConnection("172.17.0.48", 22, user, password)));
		regionServerNodes.add(new Node("172.17.0.19",  new SSHConnection("172.17.0.19", 22, user, password)));
		regionServerNodes.add(new Node("172.17.0.6",  new SSHConnection("172.17.0.6", 22, user, password)));
//		regionServerNodes.add(new Node("172.17.0.59",  new SSHConnection("172.17.0.59", 22, user, password)));
//		regionServerNodes.add(new Node("172.17.0.39",  new SSHConnection("172.17.0.39", 22, user, password)));
//		regionServerNodes.add(new Node("172.17.0.32",  new SSHConnection("172.17.0.32", 22, user, password)));

		
		
		return regionServerNodes;
		
	}*/
	
	
}
