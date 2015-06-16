package com.lin.sql.impl;

import java.util.ArrayList;
import java.util.List;

import com.lin.sql.LogicalPlan;
import com.lin.sql.SqlTree;
import com.lin.sql.Tree;

public class SimpleSqlTree extends Tree<String> implements SqlTree {
	
	public SimpleSqlTree(){
		super();
	}
	
	public SimpleSqlTree(String rootData) {
		super(rootData);
	}

	@Override
	public LogicalPlan generateLogicalPlan() {
		SimpleLogicalPlan logicalPlan = new SimpleLogicalPlan();
		
		// traverse the whole tree
		logicalPlan.setHead(fillLogicalPlanRecursively(root));
		
		return logicalPlan;
	}

	private LogicalElement fillLogicalPlanRecursively(Node<String> node) {
		// leaf
		if(node.getChildren() == null){
			Parameter parameter = new Parameter();
			parameter.setValue(node.getData());
			return parameter;
		}
		// parent
		else{
			if(node.getData().equalsIgnoreCase("select")){
				SelectOperation operation = new SelectOperation();
				// collect children
				for(Node<String> child:node.getChildren()){
					// from operation
					if(child.getData().equalsIgnoreCase("from")){
						FromOperation fromOperation = new FromOperation();
						operation.setNext(fillLogicalPlanRecursively(child));
					}
					// parameters of select
					else{
						Parameter parameter = new Parameter();
						parameter.setValue(child.getData());
						operation.parameters.add(parameter);
					}
				}
				return operation;
			}else{
				return null;
			}
			
		}
	}
	
	@Override
	public String toString() {
		return toStringRecursively(root) ;
	}
	
	public String toStringRecursively(Node<String> node){
		java.lang.String formatStr = "";
		if(node.getChildren() == null){
			return  "\"" + node.getData() + "\",";
		}
		formatStr += "{\""+node.getData()+"\":";
		formatStr += "[";
		for(Node<String> oneNode : node.getChildren()){
			
			formatStr += toStringRecursively(oneNode);
			
		}
		formatStr += "]},";
		return formatStr;
	}

}
