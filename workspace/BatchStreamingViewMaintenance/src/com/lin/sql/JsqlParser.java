package com.lin.sql;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.protobuf.ByteString;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVColumn;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Condition;
import com.lin.test.HBaseHelper;

public class JsqlParser {

	/**
	 * Use Jsql parser to parse
	 * @param input
	 * @return
	 */
	public static SimpleLogicalPlan parse(String input) {
		SimpleLogicalPlan logicalPlan = new SimpleLogicalPlan(); // logical plan to be return
		CCJSqlParserManager pm = new CCJSqlParserManager(); 
		try {
			net.sf.jsqlparser.statement.Statement statement = pm.parse(new StringReader(input)); // parse sql statement
			
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder(); // table name finder
			if (statement instanceof Select) {
				Select selectStatement = (Select) statement;
				
				tablesNamesFinder.getTableList(selectStatement); // just for testing
				System.out.println("Constructing plan");

				if(selectStatement.getSelectBody() instanceof PlainSelect){
					// get table name
					PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
					String tableName = ((Table)plainSelect.getFromItem()).getName();
					
					if(tableName != null){
						// check if it is single table or join
						if(plainSelect.getJoins() == null){
							System.out.println("Handling select with single table");
							LogicalElement element = new LogicalElement();
							handleSingleTable(plainSelect, tableName, element);
							logicalPlan.add(element);
						}else{
							System.out.println("Handling select with Join");
							
							// build plan for the first table of from
							LogicalElement element = new LogicalElement();
							handleJoinTable(plainSelect, tableName, element);
							element.setNonBlock(true);
							
							// build plan for join table
							// Assert only one join
							Join join = (Join)plainSelect.getJoins().get(0);
							LogicalElement elementJoin = new LogicalElement();
							handleJoinTable(plainSelect, ((Table)join.getRightItem()).getWholeTableName(), elementJoin);
							elementJoin.setNonBlock(true);
							
							// For each of the plan, the join key field should be filled
							// Assert the join key of the left table is on the left and 
							// the right join key of the right table is on the right
							String leftJoinKey = ((Column) ((EqualsTo) ((Join) plainSelect.getJoins().get(0)).getOnExpression()).getLeftExpression()).getWholeColumnName();
							System.out.println("left join key: " + leftJoinKey);
							element.setJoinKey(leftJoinKey);
							String rightJoinKey = ((Column) ((EqualsTo) ((Join) plainSelect.getJoins().get(0)).getOnExpression()).getRightExpression()).getWholeColumnName();
							System.out.println("right join key: " + rightJoinKey);
							elementJoin.setJoinKey(rightJoinKey);
							
							// Since the two plan above cannot know other plan
							// So we need to transfer the join table name to each of them
							String joinTableName = "join" + tableName + "With" + ((Table)join.getRightItem()).getWholeTableName();
							element.setJoinTable(joinTableName);
							elementJoin.setJoinTable(joinTableName);
							
							// now we create an empty join table
							// if already exist, delete it first
							// The column family of join table will be fixed as colfam1
							Configuration conf = HBaseConfiguration.create();
							HBaseHelper helper;
							try {
								helper = HBaseHelper.getHelper(conf);
								helper.dropTable(joinTableName);
								helper.createTable(joinTableName, "colfam");
							} catch(IOException e){
								e.printStackTrace();
							}
							
							// add the two join plan element to the logical plan
							logicalPlan.add(element);
							logicalPlan.add(elementJoin);
							
							// Create a third plan for join operation
							// since reverse join table will be generated in the last two plan
							// the third plan will just scan the results from it.
							// Assert the third plan have the name of "joinTableAWithTableB"
							LogicalElement elementResult = new LogicalElement();
							elementResult.setWaitForBlock(2);
							elementResult.setJoin(join);
							elementResult.setTableName(joinTableName);
							logicalPlan.add(elementResult);
						}
					} // if(tableName != null)
				} // if(selectStatement.getSelectBody() instanceof PlainSelect)
			}// if (statement instanceof Select)
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return logicalPlan;
	}

	public static void handleJoinTable(PlainSelect plainSelect,
			String tableName, LogicalElement element) {
		element.setTableName(tableName); // set table name
		
		// set materialize
		element.setMaterialize(true);

		// get select items (columns to be select)
		List<SelectExpressionItem> columnList = plainSelect.getSelectItems();
		
		// construct column message(protobuf) and add to logical element
		for(int j = 0; j < columnList.size(); j++){
			// find aggregation
			if(columnList.get(j).getExpression() instanceof Function){
				System.out.println("detected aggreagation function");
				Function aggFunction = (Function) columnList.get(j).getExpression();
				
				// construct aggregation as following format:
				// sum:colFam1
				String aggString = aggFunction.getName() + ":";
				int n = 0;
				boolean isBelong = true;
				for(Object expression:aggFunction.getParameters().getExpressions()){
					if(n != 0){ // if more than one parameter seperate them by comma
						aggString += ",";
					}
					Column column = (Column)expression;
					String famCol = column.getWholeColumnName().split("\\.")[1] + column.getWholeColumnName().split("\\.")[2];;
					// get the table name
					// if table name equals to the currently handling table add it to this plan 
					// if not, it belongs to another plan
					if(!column.getWholeColumnName().split("\\.")[0].equals(tableName)){
						isBelong = false;
					}
					
					aggString += famCol;
					n++;
				}
				
				if(isBelong){
					element.getAggregations().add(ByteString.copyFrom(aggString.getBytes()));
				}
			}// if(columnList.get(j).getExpression() instanceof Function)
			// find simple columns
			else{
				String famCol = columnList.get(j).toString();
				System.out.println("detected family and column " + famCol);

				BSVColumn column = BSVColumn.newBuilder()
						.setFamily(ByteString.copyFrom(famCol.split("\\.")[1].getBytes()))
						.setColumn(ByteString.copyFrom(famCol.split("\\.")[2].getBytes())).build();
				
				// add column to logical element
				// check first if it belongs to this table plan
				if(famCol.split("\\.")[0].equals(tableName)){
					element.getColumns().add(column);
				}
			}
		}// for(int j = 0; j < columnList.size(); j++)
		
		// Find all the condition statements and add them to plan
		if(plainSelect.getWhere() instanceof GreaterThan){
			String GREATER_THAN = ">";
			
			GreaterThan greaterThan = (GreaterThan) plainSelect.getWhere();
			String targetTable = greaterThan.getLeftExpression().toString().split("\\.")[0];
			String leftExpression = greaterThan.getLeftExpression().toString().split("\\.")[1] + "." + greaterThan.getLeftExpression().toString().split("\\.")[2];
			String rightExpression = greaterThan.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			if(targetTable.equals(tableName)){
				buildCondition(
						leftExpression, 
						rightExpression,
						GREATER_THAN, 
						element);
			}
		}
		else if(plainSelect.getWhere() instanceof MinorThan){
			String LESS_THAN = "<";
			
			MinorThan minorThan = (MinorThan) plainSelect.getWhere();
			String targetTable = minorThan.getLeftExpression().toString().split("\\.")[0];
			String leftExpression = minorThan.getLeftExpression().toString().split("\\.")[1] + "." + minorThan.getLeftExpression().toString().split("\\.")[2];
			String rightExpression = minorThan.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			if(targetTable.equals(tableName)){
				buildCondition(
						leftExpression, 
						rightExpression,
						LESS_THAN,
						element);
			}
		}
		else if(plainSelect.getWhere() instanceof GreaterThanEquals){
			String GREATER_THAN_EQUALS = ">=";
			
			GreaterThanEquals greaterThanEquals = (GreaterThanEquals) plainSelect.getWhere();
			String targetTable = greaterThanEquals.getLeftExpression().toString().split("\\.")[0];
			String leftExpression = greaterThanEquals.getLeftExpression().toString().split("\\.")[1] + "." + greaterThanEquals.getLeftExpression().toString().split("\\.")[2];
			String rightExpression = greaterThanEquals.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			if(targetTable.equals(tableName)){
				buildCondition(
						leftExpression, 
						rightExpression,
						GREATER_THAN_EQUALS,
						element);
			}
		}
		else if(plainSelect.getWhere() instanceof MinorThanEquals){
			String GREATER_THAN_EQUALS = "<=";
			
			MinorThanEquals minorThanEquals = (MinorThanEquals) plainSelect.getWhere();
			String targetTable = minorThanEquals.getLeftExpression().toString().split("\\.")[0];
			String leftExpression = minorThanEquals.getLeftExpression().toString();
			String rightExpression = minorThanEquals.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			if(targetTable.equals(tableName)){
				buildCondition(
						leftExpression, 
						rightExpression,
						GREATER_THAN_EQUALS,
						element);
			}
		}
	}

	public static void handleSingleTable(PlainSelect plainSelect,
			String tableName, LogicalElement element) {
		element.setTableName(tableName); // set table name
		
		// get select items (columns to be select)
		List<SelectExpressionItem> columnList = plainSelect.getSelectItems();
		
		// construct column message(protobuf) and add to logical element
		for(int j = 0; j < columnList.size(); j++){
			// find aggregation
			if(columnList.get(j).getExpression() instanceof Function){
				System.out.println("detected aggreagation function");
				Function aggFunction = (Function) columnList.get(j).getExpression();
				
				// construct aggregation as following format:
				// sum:colFam1
				String aggString = aggFunction.getName() + ":";
				int n = 0;
				for(Object expression:aggFunction.getParameters().getExpressions()){
					if(n != 0){ // if more than one parameter seperate them by comma
						aggString += ",";
					}
					Column column = (Column)expression;
					String famCol = column.getWholeColumnName();
					aggString += famCol;
					n++;
				}
				element.getAggregations().add(ByteString.copyFrom(aggString.getBytes()));
			}
			// find simple columns
			else{
				String famCol = columnList.get(j).toString();
				System.out.println("detected family and column " + famCol);

				BSVColumn column = BSVColumn.newBuilder()
						.setFamily(ByteString.copyFrom(famCol.split("\\.")[0].getBytes()))
						.setColumn(ByteString.copyFrom(famCol.split("\\.")[1].getBytes())).build();
				
				// add column to logical element
				element.getColumns().add(column);
			}
		}
		
		// Find all the condition statements and add them to plan
		if(plainSelect.getWhere() instanceof GreaterThan){
			String GREATER_THAN = ">";
			
			GreaterThan greaterThan = (GreaterThan) plainSelect.getWhere();
			String leftExpression = greaterThan.getLeftExpression().toString();
			String rightExpression = greaterThan.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			buildCondition(
					leftExpression, 
					rightExpression,
					GREATER_THAN, 
					element);
		}
		else if(plainSelect.getWhere() instanceof MinorThan){
			String LESS_THAN = "<";
			
			MinorThan minorThan = (MinorThan) plainSelect.getWhere();
			String leftExpression = minorThan.getLeftExpression().toString();
			String rightExpression = minorThan.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			buildCondition(
					leftExpression, 
					rightExpression,
					LESS_THAN,
					element);
		}
		else if(plainSelect.getWhere() instanceof GreaterThanEquals){
			String GREATER_THAN_EQUALS = ">=";
			
			GreaterThanEquals greaterThanEquals = (GreaterThanEquals) plainSelect.getWhere();
			String leftExpression = greaterThanEquals.getLeftExpression().toString();
			String rightExpression = greaterThanEquals.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			buildCondition(
					leftExpression, 
					rightExpression,
					GREATER_THAN_EQUALS,
					element);
		}
		else if(plainSelect.getWhere() instanceof MinorThanEquals){
			String GREATER_THAN_EQUALS = "<=";
			
			MinorThanEquals minorThanEquals = (MinorThanEquals) plainSelect.getWhere();
			String leftExpression = minorThanEquals.getLeftExpression().toString();
			String rightExpression = minorThanEquals.getRightExpression().toString();
			System.out.println("detected condition: [left] " + leftExpression + " [OP] > " + " [right] " + rightExpression);
			
			buildCondition(
					leftExpression, 
					rightExpression,
					GREATER_THAN_EQUALS,
					element);
		}
		
		// handle group by
		if(plainSelect.getGroupByColumnReferences() != null && !plainSelect.getGroupByColumnReferences().isEmpty()){
			String groupBy = ((Column) plainSelect.getGroupByColumnReferences().get(0)).getWholeColumnName();
			element.setAggregationKey(groupBy);
		}
	}

	public static void buildCondition(String leftExpression,
			String rightExpression, String GREATER_THAN, LogicalElement element) {
		// left operation should be a BSVColumn
		BSVColumn column = BSVColumn.newBuilder()
				.setFamily(ByteString.copyFrom(leftExpression.split("\\.")[0].getBytes()))
				.setColumn(ByteString.copyFrom(leftExpression.split("\\.")[1].getBytes())).build();
		
		// build condition
		Condition condition = Condition.newBuilder()
				.setColumn(column)
				.setOperator(ByteString.copyFrom(GREATER_THAN.getBytes()))
				.setValue(ByteString.copyFrom(rightExpression.getBytes())).build();
		
		// add condition to logical element
		element.getConditions().add(condition);
	}
	
}
