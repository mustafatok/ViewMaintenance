package de.tok.sql;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import de.tok.coprocessor.generated.BSVCoprocessorProtos;
import de.tok.utils.HBaseHelper;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class PutElement extends LogicalElement {
	private String tableName = null;
	List<String> columnList;
	List<String> values;

	@Override
	public void execute() {
		try {
			HBaseHelper.getHelper(HBaseConfiguration.create()).put(tableName, generatePut());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}

	Put generatePut(){
		Put put = new Put(values.get(0).getBytes());

		for (int i = 1; i < columnList.size(); i++) {
			String fullcol = columnList.get(i);
			String[] splitted = fullcol.split("\\.");
			String colfam = splitted[0];
			String qual = splitted[1];
			put.add(colfam.getBytes(), qual.getBytes(), values.get(i).getBytes());
		}
		return put;
	}
	@Override
	public String toString() {
		StringBuffer cl = new StringBuffer();
		StringBuffer vl = new StringBuffer();
		for(String s : columnList){
			cl.append(s + " ");
		}
		for(String s : values){
			vl.append(s + " ");
		}
		return "PutElement{" +
				"tableName='" + tableName + '\'' +
				", columnList=" + cl.toString() +
				", values=" + vl.toString() +
				'}';
	}
}
