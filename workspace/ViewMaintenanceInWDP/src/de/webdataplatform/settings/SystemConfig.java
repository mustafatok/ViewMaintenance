package de.webdataplatform.settings;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import de.webdataplatform.log.Log;

public class SystemConfig {

	
		public static String HDFS_USERNAME;
		public static String HDFS_PASSWORD;
	
		public static String SSH_USERNAME;
		public static String SSH_PASSWORD;
		public static Boolean SSH_PASSWORDLESSLOGIN;
		public static String SSH_PRIVATEKEY;
		public static String SSH_KNOWNHOSTS;
		
		public static String DIRECTORY_HDFS;
		public static String DIRECTORY_HDFSSTORAGE;
		public static String DIRECTORY_HBASE;
		public static String DIRECTORY_VMSYSTEM;
		public static String DIRECTORY_ZOOKEEPERSTORAGE;
		
		
		public static String MASTER_ZOOKEEPERPATH;
		public static String MASTER_LOADBALANCINGINTERVAL;
		
		public static String VIEWMANAGER_ZOOKEEPERPATH;
		public static String VIEWMANAGER_HDFSPATH;
		public static Long VIEWMANAGER_STATUSINTERVAL;
		public static Long VIEWMANAGER_CALCSTATISTICINTERVAL;
		public static Long VIEWMANAGER_DISPLAYINTERVAL;
		public static Long VIEWMANAGER_CRUDOPERATIONSALARMTHRESHOLD;
		public static Long VIEWMANAGER_UPDATEPOLLINGINTERVAL;
		public static Boolean VIEWMANAGER_LOGUPDATES;
		public static Boolean VIEWMANAGER_LOGPERFORMANCE;
		
		
		public static String REGIONSERVER_ZOOKEEPERPATH;
		public static Long REGIONSERVER_STATUSINTERVAL;
		public static Long REGIONSERVER_CALCSTATISTICSINTERVAL;
		public static Long REGIONSERVER_DISPLAYINTERVAL;
		public static Long REGIONSERVER_WALPOLLINGINTERVAL;
		public static Integer REGIONSERVER_MAXREPLICASHASHRING;
		public static Boolean REGIONSERVER_LOGWAL;
		
		public static Integer CLIENT_LOGINTERVAL;
		
		public static String MESSAGES_STARTSEQUENCE;
		public static String MESSAGES_ENDSEQUENCE;
		public static String MESSAGES_SPLITSEQUENCE;
		public static String MESSAGES_SPLITCONTENTSEQUENCE;
		public static String MESSAGES_SPLITIDSEQUENCE;
		public static String MESSAGES_MARKERPREFIX;
		public static Long MESSAGES_POLLINGINTERVAL;
		public static Integer MESSAGES_LENGTH;
		public static Long MESSAGES_RETRYINTERVAL;
		public static Integer MESSAGES_NUMOFRETRIES;

    
		public static Boolean LOGGING_CONSOLE;
		public static Boolean LOGGING_FILE;
		public static Boolean LOGGING_STATISTICS;
		public static String LOGGING_STATISTICSLINESEPARATOR;
		public static Boolean LOGGING_LOGUPDATES;

		public static Boolean FAULTTOLERANCE_SIGNATURES;
		public static Boolean FAULTTOLERANCE_COMMITLOG;
		public static Boolean FAULTTOLERANCE_TESTANDSET;
		


	
	
	
	public static void load(Log log) {
	
		
		
		
		try
		{
			
			XMLConfiguration config = new XMLConfiguration();
			config.setDelimiterParsingDisabled(true);
			config.load("VMSystemConfig.xml");

			SystemConfig.LOGGING_CONSOLE = Boolean.parseBoolean(config.getString("systemconfig.logging.console"));
		    SystemConfig.LOGGING_FILE = Boolean.parseBoolean(config.getString("systemconfig.logging.file"));
		    SystemConfig.LOGGING_STATISTICS = Boolean.parseBoolean(config.getString("systemconfig.logging.statistics"));
		    SystemConfig.LOGGING_STATISTICSLINESEPARATOR = config.getString("systemconfig.logging.statisticsLineSeparator");
		    SystemConfig.LOGGING_LOGUPDATES = Boolean.parseBoolean(config.getString("systemconfig.logging.logUpdates"));
			
			log.info(SystemConfig.class, "Loading system config:");
		    
			
		    SystemConfig.HDFS_USERNAME = config.getString("systemconfig.hdfs.username");
		    SystemConfig.HDFS_PASSWORD = config.getString("systemconfig.hdfs.password");
		    
		    SystemConfig.SSH_USERNAME = config.getString("systemconfig.ssh.user");
		    SystemConfig.SSH_PASSWORD = config.getString("systemconfig.ssh.password");
		    SystemConfig.SSH_PASSWORDLESSLOGIN = Boolean.parseBoolean(config.getString("systemconfig.ssh.passwordlessLogin"));
		    SystemConfig.SSH_PRIVATEKEY = config.getString("systemconfig.ssh.privateKeyFile");
		    SystemConfig.SSH_KNOWNHOSTS = config.getString("systemconfig.ssh.knownHostsFile");
		    
		    SystemConfig.DIRECTORY_HDFS = config.getString("systemconfig.directories.hadoop");
		    SystemConfig.DIRECTORY_HDFSSTORAGE = config.getString("systemconfig.directories.hadoopStorage");
		    SystemConfig.DIRECTORY_HBASE = config.getString("systemconfig.directories.hbase");
		    SystemConfig.DIRECTORY_VMSYSTEM = config.getString("systemconfig.directories.vmsystem");
		    SystemConfig.DIRECTORY_ZOOKEEPERSTORAGE = config.getString("systemconfig.directories.zookeeperStorage");
		    
		    SystemConfig.MASTER_ZOOKEEPERPATH = config.getString("systemconfig.master.zookeeperPath");
		    SystemConfig.MASTER_LOADBALANCINGINTERVAL = config.getString("systemconfig.master.loadbalancingInterval");
		    
		    SystemConfig.VIEWMANAGER_ZOOKEEPERPATH = config.getString("systemconfig.viewManager.zookeeperPath");
		    SystemConfig.VIEWMANAGER_HDFSPATH = config.getString("systemconfig.viewManager.hdfsPath");
		    SystemConfig.VIEWMANAGER_STATUSINTERVAL = Long.parseLong(config.getString("systemconfig.viewManager.statusInterval"));
		    SystemConfig.VIEWMANAGER_CALCSTATISTICINTERVAL = Long.parseLong(config.getString("systemconfig.viewManager.calcStatisticsInterval"));
		    SystemConfig.VIEWMANAGER_DISPLAYINTERVAL = Long.parseLong(config.getString("systemconfig.viewManager.displayInterval"));
		    SystemConfig.VIEWMANAGER_CRUDOPERATIONSALARMTHRESHOLD = Long.parseLong(config.getString("systemconfig.viewManager.crudOperationsAlarmThreshold"));
		    SystemConfig.VIEWMANAGER_UPDATEPOLLINGINTERVAL = Long.parseLong(config.getString("systemconfig.viewManager.updatePollingInterval"));
		    SystemConfig.VIEWMANAGER_LOGUPDATES = Boolean.parseBoolean(config.getString("systemconfig.viewManager.logUpdates"));
		    SystemConfig.VIEWMANAGER_LOGPERFORMANCE = Boolean.parseBoolean(config.getString("systemconfig.viewManager.logPerformance"));

		    
		    SystemConfig.CLIENT_LOGINTERVAL = Integer.parseInt(config.getString("systemconfig.client.logInterval"));
		    
		    SystemConfig.REGIONSERVER_ZOOKEEPERPATH = config.getString("systemconfig.regionServer.zookeeperPath");
		    SystemConfig.REGIONSERVER_STATUSINTERVAL = Long.parseLong(config.getString("systemconfig.regionServer.statusInterval"));
		    SystemConfig.REGIONSERVER_CALCSTATISTICSINTERVAL = Long.parseLong(config.getString("systemconfig.regionServer.calcStatisticsInterval"));
		    SystemConfig.REGIONSERVER_DISPLAYINTERVAL = Long.parseLong(config.getString("systemconfig.regionServer.displayInterval"));
		    SystemConfig.REGIONSERVER_WALPOLLINGINTERVAL = Long.parseLong(config.getString("systemconfig.regionServer.walPollingInterval"));
		    SystemConfig.REGIONSERVER_MAXREPLICASHASHRING = Integer.parseInt(config.getString("systemconfig.regionServer.maxReplicasHashring"));
		    SystemConfig.REGIONSERVER_LOGWAL = Boolean.parseBoolean(config.getString("systemconfig.regionServer.logWal"));
		    
		    SystemConfig.MESSAGES_STARTSEQUENCE = config.getString("systemconfig.messages.startSequence");
		    SystemConfig.MESSAGES_ENDSEQUENCE = config.getString("systemconfig.messages.endSequence");
		    SystemConfig.MESSAGES_SPLITSEQUENCE = config.getString("systemconfig.messages.splitSequence");
		    SystemConfig.MESSAGES_SPLITCONTENTSEQUENCE = config.getString("systemconfig.messages.splitContentSequence");
		    SystemConfig.MESSAGES_SPLITIDSEQUENCE = config.getString("systemconfig.messages.splitIDSequence");
		    SystemConfig.MESSAGES_MARKERPREFIX = config.getString("systemconfig.messages.markerPrefix");
		    SystemConfig.MESSAGES_POLLINGINTERVAL = Long.parseLong(config.getString("systemconfig.messages.pollingInterval"));
		    SystemConfig.MESSAGES_LENGTH = Integer.parseInt(config.getString("systemconfig.messages.length"));
		    SystemConfig.MESSAGES_RETRYINTERVAL = Long.parseLong(config.getString("systemconfig.messages.retryInterval"));
		    SystemConfig.MESSAGES_NUMOFRETRIES = Integer.parseInt(config.getString("systemconfig.messages.numOfRetries"));

		    
		    SystemConfig.FAULTTOLERANCE_SIGNATURES = Boolean.parseBoolean(config.getString("systemconfig.faulttolerance.signatures"));
		    SystemConfig.FAULTTOLERANCE_COMMITLOG = Boolean.parseBoolean(config.getString("systemconfig.faulttolerance.commitlog"));
		    SystemConfig.FAULTTOLERANCE_TESTANDSET = Boolean.parseBoolean(config.getString("systemconfig.faulttolerance.testandset"));		    
		    

		    
	
		    
		    log.info(SystemConfig.class, "SystemConfig.HDFS_USERNAME:"+SystemConfig.HDFS_USERNAME);
		    log.info(SystemConfig.class, "SystemConfig.HDFS_PASSWORD:"+SystemConfig.HDFS_PASSWORD);
		    
		    log.info(SystemConfig.class, "SystemConfig.SSH_USERNAME:"+SystemConfig.SSH_USERNAME);
		    log.info(SystemConfig.class, "SystemConfig.SSH_PASSWORD:"+SystemConfig.SSH_PASSWORD);
		    log.info(SystemConfig.class, "SystemConfig.SSH_PASSWORDLESSLOGIN:"+SystemConfig.SSH_PASSWORDLESSLOGIN);
		    log.info(SystemConfig.class, "SystemConfig.SSH_PRIVATEKEY:"+SystemConfig.SSH_PRIVATEKEY);
		    log.info(SystemConfig.class, "SystemConfig.SSH_KNOWNHOSTS:"+SystemConfig.SSH_KNOWNHOSTS);
		    
		    log.info(SystemConfig.class, "SystemConfig.DIRECTORY_HDFS:"+SystemConfig.DIRECTORY_HDFS);
		    log.info(SystemConfig.class, "SystemConfig.DIRECTORY_HDFSSTORAGE:"+SystemConfig.DIRECTORY_HDFSSTORAGE);
		    log.info(SystemConfig.class, "SystemConfig.DIRECTORY_HBASE:"+SystemConfig.DIRECTORY_HBASE);
		    log.info(SystemConfig.class, "SystemConfig.DIRECTORY_VMSYSTEM:"+SystemConfig.DIRECTORY_VMSYSTEM);
		    log.info(SystemConfig.class, "SystemConfig.DIRECTORY_ZOOKEEPERSTORAGE:"+SystemConfig.DIRECTORY_ZOOKEEPERSTORAGE);
		    
		    log.info(SystemConfig.class, "SystemConfig.MASTER_ZOOKEEPERPATH:"+SystemConfig.MASTER_ZOOKEEPERPATH);
		    log.info(SystemConfig.class, "SystemConfig.MASTER_LOADBALANCINGINTERVAL:"+SystemConfig.MASTER_LOADBALANCINGINTERVAL);
		    
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_ZOOKEEPERPATH:"+SystemConfig.VIEWMANAGER_ZOOKEEPERPATH);
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_HDFSPATH:"+SystemConfig.VIEWMANAGER_HDFSPATH);
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_STATUSINTERVAL:"+SystemConfig.VIEWMANAGER_STATUSINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_CALCSTATUSINTERVAL:"+SystemConfig.VIEWMANAGER_CALCSTATISTICINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_DISPLAYINTERVAL:"+SystemConfig.VIEWMANAGER_DISPLAYINTERVAL);	
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_CRUDOPERATIONSALARMTHRESHOLD:"+SystemConfig.VIEWMANAGER_CRUDOPERATIONSALARMTHRESHOLD);	
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_UPDATEPOLLINGINTERVAL:"+SystemConfig.VIEWMANAGER_UPDATEPOLLINGINTERVAL);	
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_LOGUPDATES:"+SystemConfig.VIEWMANAGER_LOGUPDATES);		
		    log.info(SystemConfig.class, "SystemConfig.VIEWMANAGER_LOGPERFORMANCE:"+SystemConfig.VIEWMANAGER_LOGPERFORMANCE);
		    
		    log.info(SystemConfig.class, "SystemConfig.REGIONSERVER_ZOOKEEPERPATH:"+SystemConfig.REGIONSERVER_ZOOKEEPERPATH);
		    log.info(SystemConfig.class, "SystemConfig.REGIONSERVER_STATUSINTERVAL:"+SystemConfig.REGIONSERVER_STATUSINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.REGIONSERVER_CALCSTATUSINTERVAL:"+SystemConfig.REGIONSERVER_CALCSTATISTICSINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.REGIONSERVER_DISPLAYINTERVAL:"+SystemConfig.REGIONSERVER_DISPLAYINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.REGIONSERVER_WALPOLLINGINTERVAL:"+SystemConfig.REGIONSERVER_WALPOLLINGINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.REGIONSERVER_MAXREPLICASHASHRING:"+SystemConfig.REGIONSERVER_MAXREPLICASHASHRING);
		    log.info(SystemConfig.class, "SystemConfig.REGIONSERVER_LOGWAL:"+SystemConfig.REGIONSERVER_LOGWAL);	

		    log.info(SystemConfig.class, "SystemConfig.CLIENT_LOGINTERVAL:"+SystemConfig.CLIENT_LOGINTERVAL);	
		    
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_STARTSEQUENCE:"+SystemConfig.MESSAGES_STARTSEQUENCE);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_ENDSEQUENCE:"+SystemConfig.MESSAGES_ENDSEQUENCE);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_SPLITSEQUENCE:"+SystemConfig.MESSAGES_SPLITSEQUENCE);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_SPLITCONTENTSEQUENCE:"+SystemConfig.MESSAGES_SPLITCONTENTSEQUENCE);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_SPLITIDSEQUENCE:"+SystemConfig.MESSAGES_SPLITIDSEQUENCE);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_MARKERPREFIX:"+SystemConfig.MESSAGES_MARKERPREFIX);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_POLLINGINTERVAL:"+SystemConfig.MESSAGES_POLLINGINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_LENGTH:"+SystemConfig.MESSAGES_LENGTH);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_RETRYINTERVAL:"+SystemConfig.MESSAGES_RETRYINTERVAL);
		    log.info(SystemConfig.class, "SystemConfig.MESSAGES_NUMOFRETRIES:"+SystemConfig.MESSAGES_NUMOFRETRIES);
		
		    
		    log.info(SystemConfig.class, "SystemConfig.LOGGING_CONSOLE:"+SystemConfig.LOGGING_CONSOLE);
		    log.info(SystemConfig.class, "SystemConfig.LOGGING_FILE:"+SystemConfig.LOGGING_FILE);
		    log.info(SystemConfig.class, "SystemConfig.LOGGING_STATISTICS:"+SystemConfig.LOGGING_STATISTICS);	
		    log.info(SystemConfig.class, "SystemConfig.LOGGING_LOGUPDATES:"+SystemConfig.LOGGING_LOGUPDATES);	
		    
		    log.info(SystemConfig.class, "SystemConfig.FAULTTOLERANCE_SIGNATURES:"+SystemConfig.FAULTTOLERANCE_SIGNATURES);
		    log.info(SystemConfig.class, "SystemConfig.FAULTTOLERANCE_COMMITLOG:"+SystemConfig.FAULTTOLERANCE_COMMITLOG);
		    log.info(SystemConfig.class, "SystemConfig.FAULTTOLERANCE_TESTANDSET:"+SystemConfig.FAULTTOLERANCE_TESTANDSET);	

		    log.info(SystemConfig.class, "--------------------------------------------------------------");	
		    

		    
		}
		catch(ConfigurationException cex)
		{
			log.error(SystemConfig.class, cex);
			cex.printStackTrace();
			System.exit(0);
		} 
		
		
		
		
		
		
		




	}
	
	public static void main(String[] args){
//		 load();
	}
	

}
