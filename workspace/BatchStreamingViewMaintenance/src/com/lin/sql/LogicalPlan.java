package com.lin.sql;

import java.util.Deque;

/**
 * Logical Plan (A directed acyclic graph)
 * @author xiaojielin
 *
 */
public interface LogicalPlan {
	public PhysicalPlan generatePhysicalPlan();
}
