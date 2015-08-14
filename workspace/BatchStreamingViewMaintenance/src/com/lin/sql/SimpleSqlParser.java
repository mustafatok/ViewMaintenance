package com.lin.sql;

import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVColumn;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Condition;


/**
 * A simple sql parser
 * @author xiaojielin
 *
 */
public class SimpleSqlParser {
	
	public SimpleSqlParser() {
	}
	
	/**
	 *  Parse standard sql. A simple example would look like:
	 *
	 * select family:column,family1:column1 
	 * from t1
	 * where family1:column > 12
	 * 
	 * @param string
	 * @return
	 */
	public static SimpleLogicalPlan parse(String string){
		SimpleLogicalPlan logicalPlan = new SimpleLogicalPlan();
		LogicalElement logicalElement = new LogicalElement();
		
		String[] sql = string.split(" ");
		List<String> sqlList = Arrays.asList(sql);
		
		int indexFrom = sqlList.indexOf("from"); // should be 2
		
		if(indexFrom != 2){
			System.out.println("Error: wrong sql format");
			return null;
		}
		
		// 0 - index From --> select columns divided by ,
		// add column to logical element
		String[] columns = sql[1].split(",");
		for(int i = 0; i < columns.length; i++){
			logicalElement.getColumns().add(
					BSVColumn.newBuilder()
						.setFamily(ByteString.copyFrom(columns[i].split(":")[0].getBytes()))
						.setColumn(ByteString.copyFrom(columns[i].split(":")[1].getBytes())).build());
		}
		
		// index 3 --> table name
		logicalElement.setTableName(sql[3]);
		
		int indexWhere = sqlList.indexOf("where");
		
		// index Where - end --> conditions
		logicalElement.getConditions().add(
				Condition.newBuilder()
					.setColumn(
						BSVColumn.newBuilder()
							.setFamily(
								ByteString.copyFrom(sql[indexWhere + 1].split(":")[0].getBytes())
						    )
						    .setColumn(
						        ByteString.copyFrom(sql[indexWhere + 1].split(":")[1].getBytes())
						    ).build()
					)
					.setOperator(
						ByteString.copyFrom(sql[indexWhere + 2].getBytes())
					)
					.setValue(
							ByteString.copyFrom(sql[indexWhere + 3].getBytes())
					).build());
		
		logicalPlan.setHead(logicalElement);
		
		return logicalPlan;
	}

}
