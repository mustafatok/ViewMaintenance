package com.lin.sql;


public class SimpleLogicalPlan{
	
	// execution stack
	public LogicalElement head = null;
	
	public LogicalElement getHead() {
		return head;
	}

	public void setHead(LogicalElement head) {
		this.head = head;
	}

	@Override
	public String toString() {
		String str = "";
		LogicalElement le = head;
		do{
			str += "[ " + le.toString() + " ] ";
		}while(le.getNext() != null);
		
		return str;
	}
	

}
