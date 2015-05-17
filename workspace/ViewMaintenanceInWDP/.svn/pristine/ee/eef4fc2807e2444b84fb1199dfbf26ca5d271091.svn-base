package de.webdataplatform.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner;

public class SeqNoLogCleaner extends BaseLogCleanerDelegate {
	
	  static final Log LOG = LogFactory.getLog(TimeToLiveLogCleaner.class.getName());
	  // Configured time a log can be kept after it was closed
	  private boolean stopped = false;

	  @Override
	  public boolean isLogDeletable(FileStatus fStat) {

	    return false;
	  }

	  @Override
	  public void setConf(Configuration conf) {
	    super.setConf(conf);
	  }


	  @Override
	  public void stop(String why) {
	    this.stopped = true;
	  }

	  @Override
	  public boolean isStopped() {
	    return this.stopped;
	  }
	}