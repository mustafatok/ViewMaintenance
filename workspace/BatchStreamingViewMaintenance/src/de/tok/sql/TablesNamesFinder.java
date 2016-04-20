package de.tok.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class TablesNamesFinder implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor {
	private List tables;

	public List getTableList(Select select) {
		tables = new ArrayList();
		select.getSelectBody().accept(this);
		return tables;
	}

	public void visit(PlainSelect plainSelect) {
		System.out.println("visit plain select");
		plainSelect.getFromItem().accept(this);
		
		if (plainSelect.getJoins() != null) {
			for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
				Join join = (Join) joinsIt.next();
				join.getRightItem().accept(this);
			}
		}
		if (plainSelect.getWhere() != null)
			plainSelect.getWhere().accept(this);

	}

	public void visit(Union union) {
		System.out.println("visit union");
		for (Iterator iter = union.getPlainSelects().iterator(); iter.hasNext();) {
			PlainSelect plainSelect = (PlainSelect) iter.next();
			visit(plainSelect);
		}
	}

	public void visit(Table tableName) {
		System.out.println("visit table name");
		String tableWholeName = tableName.getWholeTableName();
		tables.add(tableWholeName);
	}

	public void visit(SubSelect subSelect) {
		System.out.println("visit sub select");
		subSelect.getSelectBody().accept(this);
	}

	public void visit(Addition addition) {
		System.out.println("visit addition");
		visitBinaryExpression(addition);
	}

	public void visit(AndExpression andExpression) {
		System.out.println("visit and expression");
		visitBinaryExpression(andExpression);
	}

	public void visit(Between between) {
		System.out.println("visit between");
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	public void visit(Column tableColumn) {
		System.out.println("visit table column " + tableColumn);
	}

	public void visit(Division division) {
		System.out.println("visit division");
		visitBinaryExpression(division);
	}

	public void visit(DoubleValue doubleValue) {
		System.out.println("visit double value");
	}

	public void visit(EqualsTo equalsTo) {
		System.out.println("visit equals to");
		visitBinaryExpression(equalsTo);
	}

	public void visit(Function function) {
		System.out.println("visit function");
	}

	public void visit(GreaterThan greaterThan) {
		System.out.println("visit greater than");
		visitBinaryExpression(greaterThan);
	}

	public void visit(GreaterThanEquals greaterThanEquals) {
		System.out.println("visit greater than equals");
		visitBinaryExpression(greaterThanEquals);
	}

	public void visit(InExpression inExpression) {
		System.out.println("visit in expression");
		inExpression.getLeftExpression().accept(this);
		inExpression.getItemsList().accept(this);
	}

	public void visit(InverseExpression inverseExpression) {
		System.out.println("visit inverse expression");
		inverseExpression.getExpression().accept(this);
	}

	public void visit(IsNullExpression isNullExpression) {
		System.out.println("visit is null expression");
	}

	public void visit(JdbcParameter jdbcParameter) {
		System.out.println("visit jdbc parameter");
	}

	public void visit(LikeExpression likeExpression) {
		System.out.println("visit like expression");
		visitBinaryExpression(likeExpression);
	}

	public void visit(ExistsExpression existsExpression) {
		System.out.println("visit exists expression");
		existsExpression.getRightExpression().accept(this);
	}

	public void visit(LongValue longValue) {
		System.out.println("visit long value");
	}

	public void visit(MinorThan minorThan) {
		System.out.println("visit minor than");
		visitBinaryExpression(minorThan);
	}

	public void visit(MinorThanEquals minorThanEquals) {
		System.out.println("visit minor than equals");
		visitBinaryExpression(minorThanEquals);
	}

	public void visit(Multiplication multiplication) {
		System.out.println("visit multiplication");
		visitBinaryExpression(multiplication);
	}

	public void visit(NotEqualsTo notEqualsTo) {
		System.out.println("visit not equals to ");
		visitBinaryExpression(notEqualsTo);
	}

	public void visit(NullValue nullValue) {
		System.out.println("visit null value");
	}

	public void visit(OrExpression orExpression) {
		System.out.println("visit or expression");
		visitBinaryExpression(orExpression);
	}

	public void visit(Parenthesis parenthesis) {
		System.out.println("visit parenthesis");
		parenthesis.getExpression().accept(this);
	}

	public void visit(StringValue stringValue) {
		System.out.println("visit string value " + stringValue);
	}

	public void visit(Subtraction subtraction) {
		System.out.println("visit string subtraction");
		visitBinaryExpression(subtraction);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		System.out.println("visit binary expression");
		binaryExpression.getLeftExpression().accept(this);
		binaryExpression.getRightExpression().accept(this);
	}

	public void visit(ExpressionList expressionList) {
		System.out.println("visit expresion list");
		for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
			Expression expression = (Expression) iter.next();
			expression.accept(this);
		}

	}

	public void visit(DateValue dateValue) {
		System.out.println("visit date value");
	}
	
	public void visit(TimestampValue timestampValue) {
		System.out.println("visit timestamp value");
	}
	
	public void visit(TimeValue timeValue) {
		System.out.println("visit time value");
	}

	public void visit(CaseExpression caseExpression) {
		System.out.println("visit case expression");
	}

	public void visit(WhenClause whenClause) {
		System.out.println("visit when clause");
	}

	public void visit(AllComparisonExpression allComparisonExpression) {
		System.out.println("visit all comparison expression");
		allComparisonExpression.GetSubSelect().getSelectBody().accept(this);
	}

	public void visit(AnyComparisonExpression anyComparisonExpression) {
		System.out.println("visit any comparison expression");
		anyComparisonExpression.GetSubSelect().getSelectBody().accept(this);
	}

	public void visit(SubJoin subjoin) {
		System.out.println("visit sub join");
		subjoin.getLeft().accept(this);
		subjoin.getJoin().getRightItem().accept(this);
	}

	public void visit(Concat concat) {
		System.out.println("visit concat");
		visitBinaryExpression(concat);
	}

	public void visit(Matches matches) {
		System.out.println("visit matches");
		visitBinaryExpression(matches);
	}

	public void visit(BitwiseAnd bitwiseAnd) {
		System.out.println("visit bitwise and");
		visitBinaryExpression(bitwiseAnd);
	}

	public void visit(BitwiseOr bitwiseOr) {
		System.out.println("visit bitwise or");
		visitBinaryExpression(bitwiseOr);
	}

	public void visit(BitwiseXor bitwiseXor) {
		System.out.println("visit bitwise Xor");
		visitBinaryExpression(bitwiseXor);
	}

}

