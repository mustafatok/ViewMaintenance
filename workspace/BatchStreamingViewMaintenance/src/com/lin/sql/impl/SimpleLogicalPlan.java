package com.lin.sql.impl;

import java.util.Deque;

import com.lin.sql.LogicalPlan;
import com.lin.sql.PhysicalPlan;

public class SimpleLogicalPlan implements LogicalPlan {
	
	// execution stack
	public LogicalElement head = null;
	
	public LogicalElement getHead() {
		return head;
	}

	public void setHead(LogicalElement head) {
		this.head = head;
	}

	@Override
	public PhysicalPlan generatePhysicalPlan() {
		
		return null;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		
		LogicalElement element = head;
		
		do{
//			stringBuilder.append(element.)
		}while(element.getNext() != null);
		
		return null;
	}

}
