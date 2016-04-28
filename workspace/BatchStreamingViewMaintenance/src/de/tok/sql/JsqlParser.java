package de.tok.sql;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import de.tok.coprocessor.generated.BSVCoprocessorProtos;
import de.tok.utils.HBaseHelper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;

import net.sf.jsqlparser.statement.update.Update;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.HColumnDescriptor;

public class JsqlParser {
	/**
	 * Use Jsql parser to parse
	 * @param input
	 * @return
	 */
	public static SimpleLogicalPlan parse(String input, boolean isReturningResults) {
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
							SelectElement element = new SelectElement();
							element.setSQL(input);
//							element.setViewName(viewName);
							element.setReturningResults(isReturningResults);
							handleSingleTable(plainSelect, tableName, element);
							logicalPlan.add(element);

						}else{
							System.out.println("Handling select with Join");
							
							// build plan for the first table of from
							SelectElement element = new SelectElement();
							handleJoinTable(plainSelect, tableName, element);
							element.setReturningResults(isReturningResults);
//							String SQL = element.constructSQLByField();
							element.setSQL(input);
//							element.setViewName(viewName);
							element.setNonBlock(false);

							// build plan for join table
							// Assert only one join
							Join join = (Join)plainSelect.getJoins().get(0);
							SelectElement elementJoin = new SelectElement();
							elementJoin.setReturningResults(isReturningResults);
							handleJoinTable(plainSelect, ((Table)join.getRightItem()).getWholeTableName(), elementJoin);
							elementJoin.setSQL(input);
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

							// add the two join plan element to the logical plan
							logicalPlan.add(element);
							logicalPlan.add(elementJoin);
							
							// Create a third plan for join operation
							// since reverse join table will be generated in the last two plan
							// the third plan will just scan the results from it.
							// Assert the third plan have the name of "joinTableAWithTableB"
							SelectElement elementResult = new SelectElement();
							elementResult.setWaitForBlock(2);
							elementResult.setSQL(input);
							elementResult.setJoin(join);
							elementResult.setReturningResults(isReturningResults);
							elementResult.setBuildJoinView(true);
							logicalPlan.add(elementResult);
						}
					} // if(tableName != null)
				} // if(selectStatement.getSelectBody() instanceof PlainSelect)
			}// if (statement instanceof Select)
			else if (statement instanceof Insert){
				PutElement putElement = new PutElement();
				Insert insertStatement = (Insert) statement;
				String tableName = insertStatement.getTable().getName();
				List<String> columnList = new ArrayList<>();
				final List<String> values = new ArrayList<>();

				for(Column c: (List<Column>) insertStatement.getColumns()){
					columnList.add(c.getWholeColumnName());
				}

				ItemsListVisitor visitor = new ItemsListVisitor() {

					@Override
					public void visit(SubSelect subSelect) {

					}

					@Override
					public void visit(ExpressionList expressionList) {
						List<Expression> list = expressionList.getExpressions();
						for (Expression el: list) {
							if(el instanceof StringValue){
								values.add(((StringValue) el).getValue());
							}else{
								values.add(el.toString());
							}
						}
					}
				};
				insertStatement.getItemsList().accept(visitor);
				putElement.setTableName(tableName);
				putElement.setColumnList(columnList);
				putElement.setValues(values);
				logicalPlan.add(putElement);
			}else if (statement instanceof Update){
				PutElement putElement = new PutElement();
				Update updateStatement = (Update) statement;
				String tableName = updateStatement.getTable().getName();
				List<String> columnList = new ArrayList<>();
				List<String> values = new ArrayList<>();

				for(Column c: (List<Column>) updateStatement.getColumns()){
					columnList.add(c.getWholeColumnName());
				}
				for(Expression val: (List<Column>) updateStatement.getExpressions()){
					values.add(val.toString());
				}

				putElement.setTableName(tableName);
				putElement.setColumnList(columnList);
				putElement.setValues(values);
				logicalPlan.add(putElement);
			}else if (statement instanceof Delete){
				DeleteElement deleteElement = new DeleteElement();
				Delete deleteStatement = (Delete) statement;
				String tableName = deleteStatement.getTable().getName();
				EqualsTo exp = ((EqualsTo) deleteStatement.getWhere());
				String le = exp.getLeftExpression().toString();
				String re = exp.getRightExpression().toString();
				if(le.equals("row"))
					deleteElement.setRow(re.getBytes());

				deleteElement.setTableName(tableName);
				logicalPlan.add(deleteElement);
			}
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return logicalPlan;
	}


	public static void handleJoinTable(PlainSelect plainSelect, String tableName, SelectElement element) {
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

				BSVCoprocessorProtos.BSVColumn column = BSVCoprocessorProtos.BSVColumn.newBuilder()
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

	public static void handleSingleTable(PlainSelect plainSelect, String tableName, SelectElement element) {
		element.setTableName(tableName); // set table name
		
		// get select items (columns to be select)
		List<SelectItem> columnList = plainSelect.getSelectItems();
//		List<SelectExpressionItem> columnList = plainSelect.getSelectItems();

		// construct column message(protobuf) and add to logical element
		for(int j = 0; j < columnList.size(); j++){
			if(columnList.get(j) instanceof AllColumns){
				HColumnDescriptor[] list = new HColumnDescriptor[0];
				try {
					list = HBaseHelper.getHelper(HBaseConfiguration.create()).getColumnFamilies(tableName);
				} catch (IOException e) {
					e.printStackTrace();
				}
				for (HColumnDescriptor col: list) {
					System.out.println("detected family and column " + col);
					// TODO: Support * operation for SQL.

					// add column to logical element
				}
			}else if(columnList.get(j) instanceof SelectExpressionItem) {
				// find aggregation
				SelectExpressionItem item = (SelectExpressionItem) columnList.get(j);
				if (item.getExpression() instanceof Function) {
					System.out.println("detected aggreagation function");
					Function aggFunction = (Function) item.getExpression();

					// construct aggregation as following format:
					// sum:colFam1
					String aggString = aggFunction.getName() + ":";
					int n = 0;
					for (Object expression : aggFunction.getParameters().getExpressions()) {
						if (n != 0) { // if more than one parameter seperate them by comma
							aggString += ",";
						}
						Column column = (Column) expression;
						String famCol = column.getWholeColumnName();
						aggString += famCol;
						n++;
					}
					element.getAggregations().add(ByteString.copyFrom(aggString.getBytes()));
				}
				// find simple columns
				else {
					String famCol = item.toString();
					System.out.println("detected family and column " + famCol);

					BSVCoprocessorProtos.BSVColumn column = BSVCoprocessorProtos.BSVColumn.newBuilder()
							.setFamily(ByteString.copyFrom(famCol.split("\\.")[0].getBytes()))
							.setColumn(ByteString.copyFrom(famCol.split("\\.")[1].getBytes())).build();

					// add column to logical element
					element.getColumns().add(column);
				}
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
			String rightExpression, String GREATER_THAN, SelectElement element) {
		// left operation should be a BSVColumn
		BSVCoprocessorProtos.BSVColumn column = BSVCoprocessorProtos.BSVColumn.newBuilder()
				.setFamily(ByteString.copyFrom(leftExpression.split("\\.")[0].getBytes()))
				.setColumn(ByteString.copyFrom(leftExpression.split("\\.")[1].getBytes())).build();
		
		// build condition
		BSVCoprocessorProtos.Condition condition = BSVCoprocessorProtos.Condition.newBuilder()
				.setColumn(column)
				.setOperator(ByteString.copyFrom(GREATER_THAN.getBytes()))
				.setValue(ByteString.copyFrom(rightExpression.getBytes())).build();
		
		// add condition to logical element
		element.getConditions().add(condition);
	}

	public static byte typeOfQuery(String query) {
		CCJSqlParserManager pm = new CCJSqlParserManager();
		try {
			net.sf.jsqlparser.statement.Statement statement = pm.parse(new StringReader(query)); // parse sql statement
			if (statement instanceof Select) {
				Select selectStatement = (Select) statement;
				if(selectStatement.getSelectBody() instanceof PlainSelect){
					// get table name
					PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
					String tableName = ((Table)plainSelect.getFromItem()).getName();
					if(tableName != null){
						List<SelectExpressionItem> columnList = plainSelect.getSelectItems();
						for(int j = 0; j < columnList.size(); j++) {
							SelectExpressionItem item = columnList.get(j);
							if (item.getExpression() instanceof Function) {
								System.out.println("Detected aggregation function");
								return AGGREGATION;
							}
						}
						// check if it is single table or join
						if(plainSelect.getJoins() == null){
							System.out.println("Handling select with single table");
							return SELECT;
						}else {
							System.out.println("Handling select with Join");
							return JOIN;
						}
					} // if(tableName != null)
				} // if(selectStatement.getSelectBody() instanceof PlainSelect)
			}// if (statement instanceof Select)

		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return NOT_AVAILABLE;
	}

	public static HashSet<Byte> typeOfAggregation(String query) {

		HashSet<Byte> set = new HashSet<>();

		String lowerQ = query.toLowerCase();
		if(lowerQ.contains("min(")){
			set.add(AGGREGATION_MIN);
		}
		if(lowerQ.contains("max(")){
			set.add(AGGREGATION_MAX);
		}
		if(lowerQ.contains("count(")){
			set.add(AGGREGATION_COUNT);
		}
		if(lowerQ.contains("sum(")){
			set.add(AGGREGATION_SUM);
		}
		if(lowerQ.contains("avg(")){
			set.add(AGGREGATION_AVG);
		}
		if(set.isEmpty())
			return null;
		else
			return set;
	}

	public static String getGroupBy(String query){
		CCJSqlParserManager pm = new CCJSqlParserManager();
		try {
			net.sf.jsqlparser.statement.Statement statement = pm.parse(new StringReader(query)); // parse sql statement
			if (statement instanceof Select) {
				Select selectStatement = (Select) statement;
				if(selectStatement.getSelectBody() instanceof PlainSelect){
					// get table name
					PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
					if(plainSelect.getGroupByColumnReferences() != null && !plainSelect.getGroupByColumnReferences().isEmpty()){
						String groupBy = ((Column) plainSelect.getGroupByColumnReferences().get(0)).getWholeColumnName();

						if(groupBy.equals(new String("")))
							return "";
						else
//							return "aggKey";
							return groupBy.substring(groupBy.indexOf('.') + 1);
					}
				} // if(selectStatement.getSelectBody() instanceof PlainSelect)
			}// if (statement instanceof Select)

		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return "";
	}

	public static final byte AGGREGATION_MIN = 1;
	public static final byte AGGREGATION_MAX = 2;
	public static final byte AGGREGATION_SUM = 3;
	public static final byte AGGREGATION_COUNT = 4;
	public static final byte AGGREGATION_AVG = 5;

	public static final byte AGGREGATION = 6;
	public static final byte SELECT = 7;
	public static final byte JOIN = 8;

	public static final byte NOT_AVAILABLE = 0;


}
