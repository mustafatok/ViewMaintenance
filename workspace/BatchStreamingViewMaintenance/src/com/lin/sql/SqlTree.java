package com.lin.sql;

/**
 * From sql to logical plan(A DAG, Directed Acyclic Graph)
 * @author xiaojielin
 *
 */
public interface SqlTree {
	public LogicalPlan generateLogicalPlan();
}
