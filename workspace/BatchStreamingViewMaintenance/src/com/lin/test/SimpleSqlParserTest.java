package com.lin.test;

import org.junit.Test;

import com.lin.sql.impl.SimpleSqlParser;

public class SimpleSqlParserTest {

	@Test
	public void test1() {
		SimpleSqlParser parser = new SimpleSqlParser("select * from t1 where score>90 and age<20 and year>2015 or class=18 ");
		System.out.println(parser.generateSqlTree().toString());
	}
	
	@Test
	public void test2() {
		SimpleSqlParser parser = new SimpleSqlParser("select * from t1,t2 where t1.articleNumber=t2.articleNumber ");
		System.out.println(parser.generateSqlTree().toString());
	}

	@Test
	public void test3() {
		SimpleSqlParser parser = new SimpleSqlParser("select score,rate,class,age from t1 ");
		System.out.println(parser.generateSqlTree().toString());
	}
}
