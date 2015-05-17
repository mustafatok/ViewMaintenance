package de.webdataplatform.test;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.filter.Filter;

public interface RowCountProtocol {
	
	long getRowCount() throws IOException;
	
	long getRowCount(Filter filter) throws IOException;
	
	long getKeyValueCount() throws IOException;


	Map<String, Integer> getRowCountAggregation() throws IOException;
	
	Map<String, Integer> getRowCountAggregation(Filter filter) throws IOException;
	
	Map<String, Integer> getKeyValueCountAggregation() throws IOException;


	Map<String, Integer> getRowSumAggregation() throws IOException;
	
	Map<String, Integer> getRowSumAggregation(Filter filter) throws IOException;
	
	Map<String, Integer> getKeyValueSumAggregation() throws IOException;	
	
}