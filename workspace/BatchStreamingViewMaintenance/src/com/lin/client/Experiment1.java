package com.lin.client;

import java.io.PrintWriter;

public class Experiment1 extends Thread {
	String[] evaluateCases;
	int caseNo;
	PrintWriter writer;
	int rows;
	int m;
	public Experiment1(String[] evaluateCases, int caseNo, PrintWriter writer,int rows, int m) {
		this.evaluateCases=evaluateCases;
		this.caseNo=caseNo;
		this.writer=writer;
		this.rows=rows;
		this.m = m;
	}

	@Override
	public void run() {
		super.run();
	}
	
}
