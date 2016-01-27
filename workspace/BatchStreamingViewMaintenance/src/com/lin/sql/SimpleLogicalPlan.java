package com.lin.sql;


public class SimpleLogicalPlan{
	
	// execution stack
	private LogicalElement head = null;
	
	public LogicalElement getHead() {
		return head;
	}

	public void setHead(LogicalElement head) {
		this.head = head;
	}
	
	/**
	 * return the last element
	 * @return
	 */
	public LogicalElement getLast(){
		if(head == null){
			return null;
		}else{
			LogicalElement element = head;
			while(element.getNext() != null){
				element = element.getNext();
			}
			return element;
		}
	}

	@Override
	public String toString() {
		String str = "";
		LogicalElement le = head;
		if(le == null){
			return "";
		}
		do{
			str += "[ " + le.toString() + " ] ";
		}while((le = le.getNext()) != null);
		
		return str;
	}

	/**
	 * Add a plan to the end of this plan
	 * @param element
	 */
	public void add(LogicalElement element) {
		// set next element field
		if(head == null){
			this.setHead(element);
		}else{
			this.getLast().setNext(element);
		}
	}	

}
