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
				} else{
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

	/**
	 * Handle sql
	 * @param input
	 */
	public static void handleSQL(String input) {
		SimpleLogicalPlan simpleLogicalPlan = JsqlParser.parse(input);
		
		System.out.println(simpleLogicalPlan);
		
		LogicalElement logicalElement = simpleLogicalPlan.getHead();
		do{
			logicalElement.execute();
		}while((logicalElement = logicalElement.getNext()) != null);
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
			} else {
				System.out.println("Must specify the table name");
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
