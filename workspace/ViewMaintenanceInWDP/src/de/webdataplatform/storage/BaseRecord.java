package de.webdataplatform.storage;

import java.io.Serializable;

import de.webdataplatform.view.Signature;

public class BaseRecord implements Serializable, IBaseRecordAggregation, IBaseRecordProjection, IBaseRecordJoin{
	

	
	private static final long serialVersionUID = -7160701713001238022L;

	private String x;
	
	private int y;
	
	private boolean joinSide;
	
	
	

	public BaseRecord(String x, boolean joinSide) {
		super();
		this.x = x;
		this.joinSide = joinSide;
	}
	

	public BaseRecord(int y, boolean joinSide) {
		super();
		this.y = y;
		this.joinSide = joinSide;
	}



	public BaseRecord(String x, int y) {
		super();
		this.x = x;
		this.y = y;
	}

	public String getX() {
		return x;
	}

	public void setX(String x) {
		this.x = x;
	}

	public int getY() {
		return y;
	}

	public void setY(int y) {
		this.y = y;
	}


	@Override
	public String toString() {
		return "TableColumn [x=" + x + ", y=" + y + "]";
	}

	
//	AGGREGATION IMPLEMENTATION
	
	
	@Override
	public String getAggregationIdentifier() {

		return x;
	}

	@Override
	public int getAggregationValue() {

		return y;
		
	}
	
	
//	PROJECTION IMPLEMENTATION
	

	@Override
	public boolean isMatching(int projectionCondition, int projectionOperator) {

		if(projectionOperator == IBaseRecordProjection.EQUAL)return (y == projectionCondition);
		if(projectionOperator == IBaseRecordProjection.GREATER_THAN)return (y > projectionCondition);
		if(projectionOperator == IBaseRecordProjection.LOWER_THAN)return (y < projectionCondition);
		
		
		return false;
	}

	
	
//		VIEW RECORD IMPLEMENTATION
	
	
	private Signature signature=new Signature();
	
	
	
	@Override
	public Signature getSignature() {
		return signature;
	}
	
	@Override
	public void setSignature(Signature signature) {
		this.signature = signature;
	}


	@Override
	public boolean isLeftSide() {
	
		return joinSide;
	}


	@Override
	public boolean isRightSide() {

		return !joinSide;
	}


	
	
	
	
	
}
