package de.webdataplatform.settings;

import java.util.List;

public class EvaluationParams {

	private int iterations;
	private List<VariableParam> variableParams;
//	private boolean recreateDatabase;
	
	
	
	public EvaluationParams(int iterations) {
		super();
		this.iterations = iterations;
	}


	public EvaluationParams(int iterations, List<VariableParam> variableParams) {
		super();
		this.iterations = iterations;
		this.variableParams = variableParams;
	}




	public int getIterations() {
		return iterations;
	}
	public void setIterations(int iterations) {
		this.iterations = iterations;
	}



	public List<VariableParam> getVariableParams() {
		return variableParams;
	}


	public void setVariableParams(List<VariableParam> variableParams) {
		this.variableParams = variableParams;
	}




	@Override
	public String toString() {
		return "EvaluationParams [iterations=" + iterations
				+ ", variableParams=" + variableParams + "]";
	}


	
	
	
}
