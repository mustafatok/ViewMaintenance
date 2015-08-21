package com.lin.sql;

import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import com.google.protobuf.ByteString;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.BSVColumn;
import com.lin.coprocessor.generated.BSVCoprocessorProtos.Condition;

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
				List tableList = tablesNamesFinder.getTableList(selectStatement); // get table name list
				
				System.out.println("Constructing plan");
				for (Iterator iter = tableList.iterator(); iter.hasNext();) { // construct plan for each table
					String tableName = (String) iter.next();

					if(tableName != null){
						LogicalElement element = new LogicalElement();
						element.setTableName(tableName); // set table name
						
						// get select items (columns to be select)
						if(selectStatement.getSelectBody() instanceof PlainSelect){
							PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
							List<SelectExpressionItem> columnList = plainSelect.getSelectItems();
							
							// construct column message(protobuf) and add to logical element
							for(int j = 0; j < columnList.size(); j++){
								String famCol = columnList.get(j).toString();
								System.out.println("detected family and column " + famCol);

								BSVColumn column = BSVColumn.newBuilder()
										.setFamily(ByteString.copyFrom(famCol.split("\\.")[0].getBytes()))
										.setColumn(ByteString.copyFrom(famCol.split("\\.")[1].getBytes())).build();
								
								// add column to logical element
								element.getColumns().add(column); 
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
						}
						
						logicalPlan.add(element);
					}
				}
			}
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return logicalPlan;
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
