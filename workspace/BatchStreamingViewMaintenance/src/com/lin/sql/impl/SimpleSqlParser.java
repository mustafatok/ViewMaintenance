package com.lin.sql.impl;

import java.util.ArrayList;
import java.util.List;

import com.lin.sql.SqlParser;
import com.lin.sql.SqlTree;
import com.lin.sql.Tree;
import com.lin.sql.Tree.Node;

/**
 * A simple sql parser
 * @author xiaojielin
 *
 */
public class SimpleSqlParser implements SqlParser {
	
	public String sqlString = null;
	SqlInterpreter interpreter = null;
	Node<String> currentParent = null;
	
	public SimpleSqlParser() {
		super();
	}

	public SimpleSqlParser(String sqlString) {
		super();
		this.sqlString = sqlString;
		interpreter = new SqlInterpreter(sqlString.toCharArray());
	}
	
	public String getSqlString() {
		return sqlString;
	}

	public void setSqlString(String sqlString) {
		this.sqlString = sqlString;
	}

	@Override
	public SqlTree generateSqlTree() {
		return query();
	}

	/**
	 * One query sentence
	 * @param sqlTree
	 * @return 
	 */
	private SqlTree query() {
		SimpleSqlTree sqlTree = null;
		
		StringBuilder word = new StringBuilder();
		Node<String> currentConditionNode = null;
		
		while(interpreter.hasNext()){
			char currentChar = interpreter.read();
			System.out.println("current char is " + currentChar);
			// 1.not a space
			if(currentChar != ' '){
				// 1.1 star
				if(currentChar == '*'){
					Node<String> oneNode = new Node<String>();
					oneNode.setData("*");
					currentParent.getChildren().add(oneNode);
				}
				// 1.2 equal
				else if(currentChar == '='){
					// the word before 
					Node<String> oneNode = new Node<String>();
					oneNode.setData(word.toString());
					currentParent.getChildren().add(oneNode);
					// reset word
					word = new StringBuilder();
					
					// >
					Node<String> secondNode = new Node<String>();
					secondNode.setData("=");
					currentParent.getChildren().add(secondNode);
				}
				// 1.3  greater than
				else if(currentChar == '>'){
					// the word before 
					Node<String> oneNode = new Node<String>();
					oneNode.setData(word.toString());
					currentParent.getChildren().add(oneNode);
					// reset word
					word = new StringBuilder();
					
					// >
					Node<String> secondNode = new Node<String>();
					secondNode.setData(">");
					currentParent.getChildren().add(secondNode);
				}
				// 1.4 less than
				else if(currentChar == '<'){
					// the word before 
					Node<String> oneNode = new Node<String>();
					oneNode.setData(word.toString());
					currentParent.getChildren().add(oneNode);
					// reset word
					word = new StringBuilder();
					
					// >
					Node<String> secondNode = new Node<String>();
					secondNode.setData("<");
					currentParent.getChildren().add(secondNode);
				}
				// 1.5  comma
				else if(currentChar == ','){
					// add the last word to parent
					Node<String> oneNode = new Node<String>();
					oneNode.setData(word.toString());
					currentParent.getChildren().add(oneNode);
					// reset word
					word = new StringBuilder();
				}
				// 1.6 point
				else if(currentChar == '.'){
					
				}
				// 1.7 naming character
				else{
					word.append(currentChar);
				}
			}
			// 2. a space
			else{
				// 2.1 select
				if(word.toString().equalsIgnoreCase("select")){
					if(sqlTree == null){
						// root
						sqlTree = new SimpleSqlTree("select");
						currentParent = sqlTree.getRoot();
					}else{
						// not root: this is sub-query
					}
				}
				// 2.2 from 
				else if(word.toString().equalsIgnoreCase("from")){
					Node<String> oneNode = new Node<String>();
					oneNode.setData("from");
					currentParent.getChildren().add(oneNode);
					
					// set current node to be this node
					currentParent = oneNode;
					// initialize children
					currentParent.setChildren(new ArrayList<Node<String>>());
				}
				// 2.3 where
				else if(word.toString().equalsIgnoreCase("where")){
					Node<String> oneNode = new Node<String>();
					oneNode.setData("where");
					currentParent.getChildren().add(oneNode);
					// initialize children
					oneNode.setChildren(new ArrayList<Node<String>>());
					
					// save current condition node
					currentConditionNode = oneNode;
					
					// create the first and
					Node<String> andNode = new Node<String>();
					andNode.setData("and");
					// initialize children
					andNode.setChildren(new ArrayList<Node<String>>());
					// append 'and' node to 'where' node
					oneNode.getChildren().add(andNode);
					// set current node to be this and node
					currentParent = andNode;
				}
				// 2.4 and
				else if(word.toString().equalsIgnoreCase("and")){
					Node<String> oneNode = new Node<String>();
					oneNode.setData("and");
					currentParent.getChildren().add(oneNode);
					// initialize children
					oneNode.setChildren(new ArrayList<Node<String>>());
					
					// set current node to be this node
					currentParent = oneNode;
				}
				// 2.5 or
				else if(word.toString().equalsIgnoreCase("or")){
					Node<String> oneNode = new Node<String>();
					oneNode.setData("or");
					currentConditionNode.getChildren().add(oneNode);
					
					// create 'and' node for next condition sentence
					Node<String> andNode = new Node<String>();
					andNode.setData("and");
					// initialize children
					andNode.setChildren(new ArrayList<Node<String>>());
					// append 'and' node to condition node
					currentConditionNode.getChildren().add(andNode);
					
					// set the current to be this 'and' node
					currentParent = andNode;
				}
				// 2.6 add one word
				else if(!word.toString().trim().equals("")){
					Node<String> oneNode = new Node<String>();
					oneNode.setData(word.toString());
					
					// add to children
					currentParent.getChildren().add(oneNode);
				}
				
				// reset the current word
				word = new StringBuilder();
			}
		}
		return sqlTree;
	}
	
	/**
	 * 
	 * @author xiaojielin
	 *
	 */
	public class SqlInterpreter{
		public char[] sqlString = null;
		public int readIndex = 0;
		
		public SqlInterpreter(char[] sqlString){
			this.sqlString = sqlString;
		}
		
		/**
		 * Read one char
		 * @return
		 */
		public char read(){
			return sqlString[readIndex++];
		}
		
		/**
		 * Has next char
		 * @return
		 */
		public boolean hasNext(){
			return readIndex<sqlString.length;
		}
	}
	

}
