package com.lin.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.lin.sql.JsqlParser;
import com.lin.sql.LogicalElement;
import com.lin.sql.SimpleLogicalPlan;
import com.lin.test.HBaseHelper;

public class CmdInterface {
	public static void main(String[] args) {
		// Read from input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;

		boolean esc = false;
		while (!esc) {
			try {
				System.out.print("Enter input: ");
				input = br.readLine();
				String[] argIn = input.split(" ");

				if (argIn[0].equals("quit")) {
					esc = true;
				} else if (argIn[0].equals("load")) {
					load(argIn);
				} else if(argIn[0].equals("test")){
					test(argIn);
				}
				else{
					handleSQL(input);
				}
			}catch (IOException e) {
				e.printStackTrace();
			}catch (Exception generalEx) {
				generalEx.printStackTrace();
				System.out.println("Error in command");
			}
		}
	}

	private static void test(String[] args) {
		// create Options object
		Options options = new Options();
		
		// test systematically
		options.addOption(OptionBuilder.withLongOpt("sys")
				.withDescription("test systematically").create());
		
		// test a single case
		options.addOption(OptionBuilder.withLongOpt("case").hasArg()
				.withArgName("CASE")
				.withDescription("test systematically").create());
		
		String[] testCase={
				/*
				 * Selection test cases
				 */
				// test simple selection
				"select colfam1.qual1 from testtable1", 
				// test simple selection with condition
				"select colfam1.qual1 from testtable2 where colfam1.qual1>20",
				
				/*
				 * Aggregation test cases
				 */
				// test aggregation without aggregation key
				"select max(colfam1.qual1), min(colfam1.qual1), sum(colfam1.qual1), avg(colfam1.qual1), count(colfam1.qual1) from testtable2",
				// test aggregation with aggregation key
				"select max(colfam.value), min(colfam.value), sum(colfam.value), avg(colfam.value), count(colfam.value) from testtable5 group by colfam.aggKey",
				
				/*
				 * Join test cases
				 */
				// test  simple join
				"select testtable3.colfam.qualifier_testtable3, testtable4.colfam.qualifier_testtable4 from testtable3 join testtable4 on colfam.joinkey=colfam.joinkey",
				// Join with conditions
				"select testtable3.colfam.qualifier_testtable3, testtable4.colfam.qualifier_testtable4 from testtable3 join testtable4 on colfam.joinkey=colfam.joinkey where testtable3.colfam.qualifier_testtable3>10",
		};

		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			
			if(cmd.hasOption("sys")){
				for(int i = 0; i < testCase.length; i++){
					System.out.println(testCase[i]);
					handleSQL(testCase[i]);
				}
			}else if(cmd.hasOption("case")){
				String caseName = cmd.getOptionValue("case");
				System.out.println("Testing case " + caseName);
				handleSQL(testCase[Integer.parseInt(caseName)]);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Handle sql
	 * @param input
	 */
	public static void handleSQL(String input) {
		SimpleLogicalPlan simpleLogicalPlan = JsqlParser.parse(input, true);
		System.out.println(simpleLogicalPlan);
		simpleLogicalPlan.getHead().execute();
		
	}

	/**
	 * Load command
	 * 
	 * @param args
	 */
	public static void load(String[] args) {
		// create Options object
		Options options = new Options();

		options.addOption(OptionBuilder.withLongOpt("name")
				.withDescription("the name of table").hasArg()
				.withArgName("NAME").create());

		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("name")) {
				String tableName = cmd.getOptionValue("name");

				System.out.println("loading " + tableName);
				
				// testtable 1
				if(tableName.equals("testtable1")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam1");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i <= 100; i++){
							System.out.println("put row " + i);
							helper.put(tableName, "row"+i, "colfam1", "qual1", 1, "val"+i);
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable 2
				if(tableName.equals("testtable2")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam1");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i <= 100; i++){
							System.out.println("put row " + i);
							helper.put(tableName, "row"+i, "colfam1", "qual1", 1, "" + i);
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable3
				if(tableName.equals("testtable3")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i <= 100; i++){
							System.out.println("put row " + i);
							helper.put(tableName, "row"+i, "colfam", "joinkey", 1, "x" + i);
							helper.put(tableName, "row"+i, "colfam", "qualifier_testtable3", 1, "" + i);
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable4
				if(tableName.equals("testtable4")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 100; i >= 0; i--){
							System.out.println("put row " + (101 - i));
							helper.put(tableName, "row"+(101 - i), "colfam", "joinkey", 1, "x" + i);
							helper.put(tableName, "row"+(101 - i), "colfam", "qualifier_testtable4", 1, "" + (101 - i));
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable5
				if(tableName.equals("testtable5")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 100; i >= 0; i--){
							System.out.println("put row " + (101 - i));
							helper.put(tableName, "row"+(101 - i), "colfam", "aggKey", 1, "x" + i % 20);
							helper.put(tableName, "row"+(101 - i), "colfam", "value", 1, "" + (101 - i));
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
			} else {
				System.out.println("Must specify the table name");
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
