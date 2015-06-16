package com.lin.sql.impl;

public class Parameter extends LogicalElement {
	private String value = null;
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public LogicalElement execute(Context context) {
		// TODO Auto-generated method stub
		return null;
	}

}
