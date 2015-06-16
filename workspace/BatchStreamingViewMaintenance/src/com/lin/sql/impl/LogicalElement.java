package com.lin.sql.impl;

import java.util.ArrayList;
import java.util.List;

public abstract class LogicalElement{
	private LogicalElement next = null;

	public LogicalElement getNext() {
		return next;
	}

	public void setNext(LogicalElement next) {
		this.next = next;
	}
	public abstract LogicalElement execute(Context context);
	public List<Parameter> parameters = new ArrayList<Parameter>();
}
