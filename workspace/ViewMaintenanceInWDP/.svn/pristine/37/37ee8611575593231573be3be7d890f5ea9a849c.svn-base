package de.webdataplatform.test;

import java.io.File;

import de.webdataplatform.log.Log;
import de.webdataplatform.settings.DatabaseConfig;
import de.webdataplatform.settings.EvaluationConfig;
import de.webdataplatform.settings.Experiment;
import de.webdataplatform.settings.NetworkConfig;
import de.webdataplatform.settings.SystemConfig;
import de.webdataplatform.ssh.SSHConnection;
import de.webdataplatform.ssh.SSHService;

public class TestSVM {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		
		
		
		
		Log log= new Log("experiment.log");		
		SVMHBase svmHBase = new SVMHBase(log);
		
		SystemConfig.load(log);
		NetworkConfig.load(log);
		DatabaseConfig.load(log);
		EvaluationConfig.load(log);
//		log.info(this.getClass(), "setting hbase config");
		
		String directoryName = "testresults";
		
		File dir = new File(directoryName);
		dir.mkdir();
		Experiment experiment = EvaluationConfig.EVALUATIONSETS.get(0).getExperiment();
		
		svmHBase.killAll(2);
		
		svmHBase.configureHadoop(experiment.getNumOfRegionServers(), directoryName);
		svmHBase.configureHbase(experiment.getNumOfRegionServers(), directoryName);
		
		svmHBase.cleanZookeeper();
		
		svmHBase.cleanHadoop(experiment.getNumOfRegionServers());
		svmHBase.formatHadoop();
		
		svmHBase.startHadoop();

		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		svmHBase.startHBase();
		
		

		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		svmHBase.stopAll();
		
		SSHService.closeSessions();
		
		
//		if(startDatabase){
//		log.info(TestSVM.class, "starting database");
//		svmHBase.startHBase();
//		}
//		else{
//		log.info(this.getClass(), "database already started");
//		}
//		try {
//			Thread.sleep(35000);
//		} catch (InterruptedException e) {
//
//			e.printStackTrace();
//		}

	}

}
