package com.lin.test;

import org.junit.Test;

import com.lin.sql.LogicalPlan;
import com.lin.sql.SqlParser;
import com.lin.sql.SqlTree;
import com.lin.sql.impl.SimpleSqlParser;

public class SqlParserIntegrationTest {

	@Test
	public void test() {
		// create query parser
		SqlParser parser = new SimpleSqlParser("select * from t1 ");
		// generate sql tree
		SqlTree sqlTree = parser.generateSqlTree();
		System.out.println("generated sql tree\n"+sqlTree.toString());
		// generate logical plan
		LogicalPlan logicalPlan = sqlTree.generateLogicalPlan();
		
	}

}
